import argparse
import sys
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from tx_training.jobs.g2i.base_g2i import BaseG2I
from tx_training.helper.logger import LoggerSimple
from pyspark.sql.window import Window

logger = LoggerSimple.get_logger(__name__)


class JoinCustomersLoans(BaseG2I):
    def read_data(self) -> Dict[str, DataFrame]:
        return super().read_input()

    def transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        for df_name, df in dfs.items():
            logger.info(f"df_name {df_name}: {df}")

        customer_df = dfs.get("customers")
        transaction_df = dfs.get("transactions")
        accounts_df = dfs.get("accounts")
        branches_df = dfs.get("branches")
        services_df = dfs.get("services")
        transaction_details_df = dfs.get("transaction_details")
        loans_df = dfs.get("loans")
        loan_payments_df = dfs.get("loan_payments")

        previous_day = F.date_sub(F.lit(self.data_date), 1)
        first_day_prev_month = F.trunc(
            F.date_sub(F.to_date(F.lit(self.data_date), "yyyy-MM-dd"), 1), "month")
        last_day_prev_month = F.last_day(F.date_sub(F.lit(self.data_date), 1))

        filtered_customers_df = (
            customer_df.filter(
                F.to_date(F.col("ds_partition_date"),
                          "yyyy-MM-dd") == previous_day
            ).select(
                F.col("CustomerID"),
                F.col("Name"),
                F.col("Email"),
                F.col("AccountType"),
                F.col("BranchID"),
            )
        )

        filtered_transactions_df = (
            transaction_df.filter(F.col("TransactionDate").between(
                first_day_prev_month, last_day_prev_month)).select(
                    F.col("TransactionID"),
                    F.col("CustomerID"),
                    F.col("TransactionDate"),
                    F.col("Amount"),
                    F.col("TransactionType"),
            )
        )

        filtered_accounts_df = (
            accounts_df.where((F.col("Balance") > 5000) & (
                F.col("OpenDate") <= F.lit(self.data_date)))
        )

        filtered_branches_df = (
            branches_df.where((F.col("Location").like("%N%")) &
                              (F.to_date(F.col("ds_partition_date"), "yyyy-MM-dd") == previous_day))
        )

        filtered_services_df = (
            services_df.where(F.col("ServiceType").isin(["Banking", "Loan"]))
        )

        filtered_transaction_details_df = (
            transaction_details_df.where(F.col("Amount") > 100)
        )

        filtered_loans_df = (
            loans_df.where((F.col("LoanAmount") > 5000) &
                           (F.col("LoanStartDate").between(first_day_prev_month, previous_day)))
        )

        filtered_loan_payments_df = (
            loan_payments_df.filter(F.col("PaymentAmount") > 0)
        )

        logger.info("Joining Customers and Transactions")
        customers_transactions = (
            filtered_customers_df.alias("fc")
            .join(
                filtered_transactions_df.alias("ft"),
                (F.col("fc.CustomerID") == F.col("ft.CustomerID")),
                "inner"
            ).drop(
                F.col("ft.CustomerID")
            )
        )
        customers_transactions.show(5, truncate=False)

        customer_transaction_accounts = (
            customers_transactions.alias("ct")
            .join(filtered_transactions_df.alias("ft"),
                  (F.col("ct.CustomerID") == F.col("ft.CustomerID")),
                  "inner")
            .drop(F.col("ft.CustomerID"))
        )

        customer_transaction_accounts.show(5, truncate=False)

        customer_transaction_accounts_branches = (
            customer_transaction_accounts.alias("cta")
            .join(branches_df.alias("b"),
                  (F.col("cta.BranchID") == F.col("b.BranchID")), "inner")
            .drop(F.col("b.BranchID"))
        )

        customers_transactions.show(5, truncate=False)

        transaction_details_services = (
            filtered_transaction_details_df.alias("ftd")
            .join(
                filtered_services_df.alias("fs"),
                (F.col("ftd.TransactionID") == F.col("fs.TransactionID")),
                "inner"
            )
        )

        customer_loans = (
            filtered_loans_df.alias("fl")
            .join(filtered_loan_payments_df.alias("flp"),
                  (F.col("fl.LoanID") == F.col("flp.LoanID")),
                  "left")
        ).groupBy(
            F.col("fl.CustomerID")
        ).agg(
            F.sum(F.col("fl.LoanAmount")).alias("TotalLoanAmount"),
            F.avg(F.col("fl.fl.InterestRate")).alias("AverageInterestRate"),
            F.sum(
                F.sum("fl.LoanAmount").alias("TotalLoanAmount"),
                F.avg("fl.InterestRate").alias("AverageInterestRate"),
                F.sum(
                    F.when(
                        (F.col("flp.PaymentDate").between(
                            first_day_of_month, run_data_date)),
                        F.col("flp.PaymentAmount")
                    ).otherwise(0)
                ).alias("TotalLoanPayments"),
                (
                    F.sum("fl.LoanAmount") - F.sum(
                        F.when(
                            (F.col("flp.PaymentDate").between(
                                first_day_of_month, run_data_date)),
                            F.col("flp.PaymentAmount")
                        ).otherwise(0)
                    )
                ).alias("RemainingLoanAmount"),
                F.count("fl.LoanID").alias("TotalLoans")
            )
        )

        # customer_loans = customer_loans.withColumn(
        #     "RemainingLoanAmount",
        #     F.col("TotalLoanAmount") -
        #     F.coalesce(F.col("TotalPayments"), F.lit(0))
        # )

        # customer_loans.show(5, truncate=False)

        # logger.info("Creating final output")
        # final_df = (
        #     cte_customers
        #     .join(
        #         customer_loans,
        #         cte_customers["C_CustomerID"] == customer_loans["L_CustomerID"],
        #         "left"
        #     )
        #     .select(
        #         cte_customers["C_CustomerID"].alias("CustomerID"),
        #         cte_customers["Name"],
        #         cte_customers["Email"],
        #         cte_customers["C_AccountType"].alias("AccountType"),
        #         cte_customers["C_BranchID"].alias("BranchID"),
        #         F.coalesce(customer_loans["TotalLoanAmount"], F.lit(
        #             0)).alias("TotalLoanAmount"),
        #         F.coalesce(customer_loans["AverageInterestRate"], F.lit(
        #             0)).alias("AverageInterestRate"),
        #         F.coalesce(customer_loans["PaymentsDuringPeriod"], F.lit(
        #             0)).alias("PaymentsDuringPeriod"),
        #         F.coalesce(customer_loans["RemainingLoanAmount"], F.lit(
        #             0)).alias("RemainingLoanAmount"),
        #         F.coalesce(customer_loans["TotalLoans"],
        #                    F.lit(0)).alias("TotalLoans")
        #     )
        # )

        # logger.info("Final result:")
        # final_df.show(20, truncate=False)

        # return final_df

    def execute(self):
        input_dfs = self.read_data()
        result_df = self.transformations(input_dfs)
        return result_df


def get_dbutils():
    try:
        from pyspark.dbutils import DBUtils
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except ImportError:
        logger.info("DBUtils not found. Running in local mode.")
        return None


def main():
    dbutils = get_dbutils()
    if dbutils:
        logger.info("Running in Databricks mode")
        job_name = dbutils.jobs.taskValues.get(
            taskKey="create_params", key="job_name")
        data_date = dbutils.jobs.taskValues.get(
            taskKey="create_params", key="data_date")
        skip_condition = dbutils.jobs.taskValues.get(
            taskKey="create_params", key="skip_condition")
        metadata_filepath = f"/Workspace/Shared/tx_project_metadata/{job_name}.json"
    else:
        logger.info("Running in local mode")
        metadata_filepath = "tx_training/jobs/g2i/monthly.json"
        data_date = "2024-01-05"

    logger.info(f"Metadata file: {metadata_filepath}, Data Date: {data_date}")
    customer_loans = JoinCustomersLoans(metadata_filepath, data_date)
    result = customer_loans.execute()


if __name__ == "__main__":
    main()
