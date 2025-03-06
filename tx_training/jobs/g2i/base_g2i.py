from abc import ABC, abstractmethod
from pyspark.storagelevel import StorageLevel
from tx_training.common.spark_session.local_spark_session import (
    LocalSparkSessionManager,
)
from tx_training.common.spark_session.databricks_spark_session import (
    DBxSparkSessionManager,
)
from tx_training.helper.logger import LoggerSimple
import json
from tx_training.common.scd_handler import SCD_Handler
from pyspark.sql import DataFrame, SparkSession
from tx_training.common.reader.csv_reader import CSVReader
from tx_training.common.reader.delta_reader import DeltaReader
from tx_training.common.writer.delta_writer import DeltaWriter
from tx_training.common.writer.csv_writer import CSVWriter

logger = LoggerSimple.get_logger(__name__)


class BaseG2I:
    def __init__(self, config_path, data_date):
        self.data_date = data_date
        self.config_path = config_path

        with open(self.config_path, "r") as f:
            self.config = json.load(f)

        master = self.config.get("master", "local").lower()
        if master == "dbx":
            self.spark = DBxSparkSessionManager.get_session("tx-training")
        else:
            self.spark = LocalSparkSessionManager.get_session("tx-training")
        self.scd_handler = SCD_Handler(self.spark)
        self.readers = {
            "csv": CSVReader,
            "delta": DeltaReader,
        }

        self.writers = {
            "csv": CSVWriter,
            "delta": DeltaWriter,
        }

    @abstractmethod
    def read_input(self):
        dfs = {}
        input_config = self.config.get("input", [])
        for entry in input_config:
            df_name = entry.get("name", entry["df_name"])
            input_format = entry.get("format")
            input_path = entry.get("table")
            if input_format not in self.readers:
                raise ValueError(f"Unsupported format: {input_format}")

            reader_class = self.readers[input_format]
            reader = reader_class(self.spark, input_path, entry)
            df = reader.read()

            dfs[df_name] = df
            logger.info(f"Loaded DataFrame: {df_name}")
        return dfs

    @abstractmethod
    def transformations(self):
        pass

    @abstractmethod
    def write_data(self, df: DataFrame, data_date: str):
        output_config = self.config["outputs"]
        logger.info(output_config)

        output_format = output_config.get("format")
        output_path = output_config.get("scd", {}).get(
            "path", output_config.get("path"))
        options = output_config.get("options", {})
        scd_conf = output_config.get("scd", None)

        if output_format not in self.writers:
            raise ValueError(f"Unsupported output format: {output_format}")

        writer_class = self.writers[output_format]
        writer = writer_class(self.spark, self.scd_handler, scd_conf, options)

        logger.info(f"Writing DataFrame to {output_format} at {output_path}")
        writer.write(df, data_date)
