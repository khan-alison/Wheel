import argparse
import sys
from tx_training.jobs.g2i.base_g2i import BaseG2I
from pyspark.sql import DataFrame
from typing import Dict


class JoinCustomersIntoOrders(BaseG2I):
    def read_input(self):
        return super().read_input()

    def transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        customers_df = dfs.get("customer_df")
        orders_df = dfs.get("orders_df")
        if customers_df is None or orders_df is None:
            raise ValueError("Required input DataFrames are missing.")

        result_df = customers_df.join(orders_df, on="customer_id", how="inner").where(
            F.col(f"start_date >= {self.data_date}")
        )
        return result_df

    def write_data(self, df):
        super().write_data(df)

    def execute(self):
        input_data = self.read_input()
        transformed_data = self.transformations(input_data)
        self.write_data(transformed_data)


def run_execute():
    job_name = dbutils.jobs.taskValues.get(
        taskKey="create_params", key="job_name")
    data_date = dbutils.jobs.taskValues.get(
        taskKey="create_params", key="data_date")
    skip_condition = dbutils.jobs.taskValues.get(
        taskKey="create_params", key="skip_condition"
    )
    print("===========", job_name, data_date, skip_condition)
    metadata_filepath = f"/Workspace/Shared/tx_project_metadata/{job_name}.json"

    print("config_path: ", config_path)
    print("data_date: ", data_date)
    g2i = JoinCustomersIntoOrders(metadata_filepath, data_date)
    result = g2i.execute()


# if __name__ == "__main__":
#     # parser = argparse.ArgumentParser()
#     # parser.add_argument("--config_path", default="",
#     #                     help="Path to JSON config")
#     # args = parser.parse_args()
    
#     run_execute(config_path=metadata_filepath, data_date=data_date)
