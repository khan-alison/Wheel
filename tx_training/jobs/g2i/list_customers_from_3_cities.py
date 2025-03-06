import argparse
import sys
from typing import Dict
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from tx_training.jobs.g2i.base_g2i import BaseG2I


class ListCustomersFrom3Cities(BaseG2I):
    """
    This job:
      1) Reads both 'customers_df' and 'orders_df' from the config.
      2) Assumes orders_df has a 'ShippingCity' or 'OrderCity' column that can differ for each order.
      3) Groups orders by CustomerID and counts distinct cities used.
      4) Filters for customers with >= 3 distinct cities.
      5) Joins the customers table to list CustomerName.
    """

    def read_input(self) -> Dict[str, DataFrame]:
        """
        Expects the config to have at least:
          - customers_df
          - orders_df (with a column 'ShippingCity' or 'OrderCity')
        """
        return super().read_input()

    def transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        customers_df = dfs.get("customers_df")
        orders_df = dfs.get("orders_df")

        if customers_df is None or orders_df is None:
            raise ValueError(
                "Required input DataFrames are missing: 'customers_df' or 'orders_df'."
            )

        result_df = (
            orders_df
            .groupBy("CustomerID")
            .agg(F.countDistinct(F.col("ShippingCity")).alias("DistinctCitiesCount"))
            .filter(
                F.col("DistinctCitiesCount") >= 3).join(customers_df, on="CustomerID", how="inner")
            .select("CustomerName", "DistinctCitiesCount")
        )

        return result_df

    def write_data(self, df: DataFrame):
        super().write_data(df)

    def execute(self):
        input_dfs = self.read_input()
        result_df = self.transformations(input_dfs)
        self.write_data(result_df)


def run_execute(config_path=None, data_date=None):
    pipeline = ListCustomersFrom3Cities(config_path, data_date)
    pipeline.execute()


def get_dbutils() -> DBUtils:
    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)


def main():
    dbutils = get_dbutils()
    job_name = dbutils.jobs.taskValues.get(
        taskKey="create_params", key="job_name")
    data_date = dbutils.jobs.taskValues.get(
        taskKey="create_params", key="data_date")
    skip_condition = dbutils.jobs.taskValues.get(
        taskKey="create_params", key="skip_condition")
    metadata_filepath = f"/Workspace/Shared/tx_project_metadata/{job_name}.json"

    run_execute(config_path=metadata_filepath, data_date=data_date)


if __name__ == "__main__":
    main()
