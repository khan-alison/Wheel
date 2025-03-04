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


logger = LoggerSimple.get_logger(__name__)


class BaseG2I:
    def __init__(self, config_path, data_date):
        self.data_date = data_date
        self.config_path = config_path
        
        print("self.config_path: ", self.config_path)
        with open(self.config_path, "r") as f:
            self.config = json.load(f)

        master = self.config.get("master", "local").lower()
        print(f"master {master}")
        if master == "dbx":
            self.spark = DBxSparkSessionManager.get_session("tx-training")
        else:
            # self.spark = SparkSession.builder.master("local[*]")\
            #     .appName("test")\
            #     .config("spark.driver.bindAddress", "127.0.0.1")\
            #     .config("spark.driver.port", "7077")\
            #     .config("spark.blockManager.port", "6066")\
            #     .config("spark.ui.port", "4040")\
            #     .config("spark.sql.shuffle.partitions", "5")\
            #     .config("spark.network.timeout", "600s")\
            #     .config("spark.executor.heartbeatInterval", "120s")\
            #     .config("spark.databricks.delta.schema.autoMerge.enabled", "true")\
            #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            #     .config("spark.jars", "jars/delta-spark_2.12-3.2.0.jar, jars/delta-storage-3.2.0.jar")\
            #     .getOrCreate()
            self.spark = LocalSparkSessionManager.get_session("tx-training")
        self.scd_handler = SCD_Handler(self.spark)

    @abstractmethod
    def read_input(self):
        dfs = {}
        input_config = self.config.get("input", [])
        for entry in input_config:
            df_name = entry.get("name", entry["df_name"])
            input_path = entry.get("table", None)
            logger.info(df_name)
            input_format = entry.get("format")
            options = entry.get("option", {})
            if input_format == "csv":
                reader = self.spark.read.format("csv")
                for key, value in options.items():
                    reader = reader.option(key, value)
                df = reader.load(input_path)
            elif input_format == "delta":
                if "." in input_path and not input_path.startswith("dbfs:/"):
                    df = self.spark.table(input_path)
                else:
                    df = self.spark.read.format("delta").load(input_path)
            else:
                raise ValueError(f"Unsupported format: {input_format}")

            if entry.get("isCache", False):
                level_str = entry.get("persistentLevel", "MEMORY_ONLY")
                storage_level = getattr(
                    StorageLevel, level_str, StorageLevel.MEMORY_ONLY
                )
                df = df.persist(storage_level)
            dfs[df_name] = df

            logger.info(f"Loaded DataFrame: {df_name}")
            print(dfs)
        return dfs

    @abstractmethod
    def transformations(self):
        pass

    @abstractmethod
    def write_data(self, df: DataFrame):
        output_config = self.config["outputs"]
        logger.info(output_config)
        output_format = output_config.get("format")
        logger.info(output_format)
        scd_conf = output_config.get("scd")
        logger.info(scd_conf)

        self.scd_handler.process(df, scd_conf)
