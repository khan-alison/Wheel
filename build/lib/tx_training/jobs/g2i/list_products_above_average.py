import argparse
import sys
from typing import Dict

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from tx_training.jobs.g2i.base_g2i import BaseG2I


class ListProductsAboveAverage(BaseG2I):
    """
    This job:
      1) Reads a 'products_df' from the config (which has ProductID, ProductName, Price, etc.).
      2) Calculates the average of 'Price' across all products.
      3) Filters products whose Price is strictly greater than that average.
      4) Returns ProductName and Price for those products.
    """

    def read_input(self) -> Dict[str, DataFrame]:
        """
        Expects the config to have an entry like:
          {
            "name": "products_df",
            "format": "delta",
            "table": "products"
          }
        """
        return super().read_input()

    def transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        products_df = dfs.get("products_df")
        if products_df is None:
            raise ValueError("Missing 'products_df' in the job config.")
        avg_price = products_df.agg(F.avg("Price").alias(
            "avg_price")).collect()[0]["avg_price"]
        result_df = products_df.filter(
            F.col("Price") > avg_price).select("ProductName", "Price")

        return result_df

    def write_data(self, df: DataFrame):
        super().write_data(df)

    def execute(self):
        input_dfs = self.read_input()
        result_df = self.transformations(input_dfs)
        self.write_data(result_df)


def run_execute(config_path=None, data_date=None):
    pipeline = ListProductsAboveAverage(config_path, data_date)
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
