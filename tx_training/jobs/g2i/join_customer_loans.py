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
                F.col("ft.CustomerID"),
                F.col("ft.Amount")
            )
        )

        customer_transaction_accounts = (
            customers_transactions.alias("ct")
            .join(filtered_accounts_df.alias("fa"),
                  (F.col("ct.CustomerID") == F.col("fa.CustomerID")),
                  "inner")
            .drop(F.col("fa.CustomerID")).drop(F.col("fa.AccountType"))
        )

        print("asdfasdfasfasdfsadf")
        customers_transactions.show(20)
        filtered_transactions_df.show(20)

        print("customer_transaction_accounts.show(5, truncate=False)")
        customer_transaction_accounts.show(5, truncate=False)

        customer_transaction_accounts_branches = (
            customer_transaction_accounts.alias("cta")
            .join(branches_df.alias("b"),
                  (F.col("cta.BranchID") == F.col("b.BranchID")), "inner")
            .drop(F.col("b.BranchID"))
        )

        print("customer_transaction_accounts_branches.show(20)")
        customer_transaction_accounts_branches.show(20)

        transaction_details_services = (
            filtered_transaction_details_df.alias("ftd")
            .join(
                filtered_services_df.alias("fs"),
                (F.col("ftd.ServiceID") == F.col("fs.ServiceID")),
                "inner"
            )
        )
        print("hererere")
        transaction_details_services.show(20)

        customer_loans = (
            filtered_loans_df.alias("fl")
            .join(filtered_loan_payments_df.alias("flp"),
                  (F.col("fl.LoanID") == F.col("flp.LoanID")),
                  "left")
        ).groupBy(
            F.col("fl.CustomerID")
        ).agg(
            F.sum(F.col("fl.LoanAmount")).alias("TotalLoanAmount"),
            F.avg(F.col("fl.InterestRate")).alias("AverageInterestRate"),
            F.sum(
                F.when(
                    (F.col("flp.PaymentDate").between(
                        first_day_prev_month, last_day_prev_month)),
                    F.col("flp.PaymentAmount")
                ).otherwise(0)
            ).alias("TotalLoanPayments"),
            (
                F.sum("fl.LoanAmount") -
                F.sum(
                    F.when(
                        (F.col("flp.PaymentDate").between(
                            first_day_prev_month, last_day_prev_month)),
                        F.col("flp.PaymentAmount")
                    ).otherwise(0)
                )
            ).alias("RemainingLoanAmount"),
            F.count("fl.LoanID").alias("TotalLoans")
        )
        print("customer_loans.show(20)")
        customer_loans.show(20)

        final_data = customer_transaction_accounts_branches.alias("cb").join(
            customer_loans.alias("cl"),
            (F.col("cl.CustomerID") == F.col("cb.CustomerID")),
            "left"
        ).drop(F.col("cb.CustomerID"))

        final_data = (
            final_data.alias("fd")
            .join(transaction_details_services.alias("tds"),
                  (F.col("fd.TransactionID") == F.col("tds.TransactionID")),
                  "inner").drop(F.col("tds.TransactionID"))
        )

        final_data = final_data.groupBy(
            "CustomerID", "Name", "Email", "AccountType", "BranchName", "Location",
            "Balance", "ServiceName", "TotalLoanAmount", "AverageInterestRate", "TotalLoanPayments",
            "RemainingLoanAmount"
        ).agg(
            F.sum("Amount").alias("TotalTransactionAmount"),
            F.countDistinct("TransactionID").alias("TotalTransactions")
        )

        final_data = final_data.withColumn(
            "CustomerCategory",
            F.when(F.col("AccountType") == "Premium", "High Value Customer")
            .when((F.col("AccountType") == "Standard") & ((F.col("TotalTransactionAmount") > 1000) | (F.col("TotalLoanAmount") > 50000)),
                  "Potential High Value Customer")
            .otherwise("Regular Customer")
        )

        print("final_data.show(5, truncate=True)")
        final_data.show(5, truncate=True)

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
