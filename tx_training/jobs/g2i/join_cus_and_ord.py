import argparse
import sys
from tx_training.jobs.g2i.base_g2i import BaseG2I
from pyspark.sql import DataFrame
from typing import Dict
# from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


class JoinCustomersIntoOrders(BaseG2I):
    def read_input(self):
        return super().read_input()

    def transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        customers_df = dfs.get("customer_df")
        orders_df = dfs.get("orders_df")
        if customers_df is None or orders_df is None:
            raise ValueError("Required input DataFrames are missing.")

        result_df = (
            customers_df.join(orders_df, on="customer_id", how="inner")
        )
        return result_df

    def write_data(self, df):
        super().write_data(df)

    def execute(self):
        input_data = self.read_input()
        transformed_data = self.transformations(input_data)
        self.write_data(transformed_data)


def run_execute(config_path=None, data_date=None):
    print("config_path: ", config_path)
    print("data_date: ", data_date)
    g2i = JoinCustomersIntoOrders(config_path, data_date)
    result = g2i.execute()


# def get_dbutils() -> DBUtils:
#     spark = SparkSession.builder.getOrCreate()
#     return DBUtils(spark)


def main():
    # dbutils = get_dbutils()
    # job_name = dbutils.jobs.taskValues.get(
    #     taskKey="create_params", key="job_name")
    # data_date = dbutils.jobs.taskValues.get(
    #     taskKey="create_params", key="data_date")
    # skip_condition = dbutils.jobs.taskValues.get(
    #     taskKey="create_params", key="skip_condition"
    # )
    # print("===========", job_name, data_date, skip_condition)
    # metadata_filepath = f"/Workspace/Shared/tx_project_metadata/{job_name}.json"
    metadata_filepath = "tx_training/jobs/g2i/join_customer_order.json"
    data_date = "2024-01-01"
    run_execute(config_path=metadata_filepath, data_date=data_date)


if __name__ == "__main__":
    main()
