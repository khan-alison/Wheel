import argparse
import sys
from typing import Dict

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from tx_training.jobs.g2i.base_g2i import BaseG2I


class ListOrdersInValueRange(BaseG2I):
    """
    This job:
      1) Reads Orders and OrderDetails from the config
      2) Computes the total value of each Order (sum of Quantity*UnitPrice)
      3) Filters to only include orders with total value in [1000, 2000]
      4) Writes the result out
    """

    def read_input(self) -> Dict[str, DataFrame]:
        """
        Expects the config to have at least two inputs:
          - orders_df  (the Orders table)
          - order_details_df  (the OrderDetails table)
        """
        return super().read_input()

    def transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        orders_df = dfs.get("orders_df")
        order_details_df = dfs.get("order_details_df")
        if orders_df is None or order_details_df is None:
            raise ValueError(
                "Required input DataFrames are missing: 'orders_df' or 'order_details_df'."
            )
        order_totals_df = (
            order_details_df
            .groupBy("OrderID")
            .agg(F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("TotalOrderValue"))
        )

        orders_with_totals_df = orders_df.join(
            order_totals_df, on="OrderID", how="inner")
        filtered_orders_df = orders_with_totals_df.filter(
            (F.col("TotalOrderValue") >= 1000) & (
                F.col("TotalOrderValue") <= 2000)
        )

        result_df = filtered_orders_df.select("OrderID", "TotalOrderValue")
        return result_df

    def write_data(self, df: DataFrame, data_date: str):
        super().write_data(df, data_date)

    def execute(self):
        input_dfs = self.read_input()
        result_df = self.transformations(input_dfs)
        self.write_data(result_df, self.data_date)


def run_execute(config_path=None, data_date=None):
    g2i = ListOrdersInValueRange(config_path, data_date)
    g2i.execute()


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
