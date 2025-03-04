from pyspark.sql import SparkSession
from tx_training.common.spark_session.spark_session_manager import (
    SparkSessionManagerBase,
)
from tx_training.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class LocalSparkSessionManager(SparkSessionManagerBase):
    def __init__(self, appName: str):
        super().__init__(appName, "local")

    def _create_spark_session(self) -> SparkSession:
        builder = (
            SparkSession.builder.appName(self.appName)
            .master("local")
            .config("spark.sql.shuffle.partitions", 5)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.jars",
                "jars/delta-spark_2.12-3.2.0.jar, jars/delta-storage-3.2.0.jar",
            )
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        logger.info("Creating local Spark session with local jar files.")
        return builder.getOrCreate()

    @staticmethod
    def get_session(appName: str):
        """Get a SparkSession for the application"""
        instance = SparkSessionManagerBase.get_instance(appName, "local")
        if instance is None:
            instance = LocalSparkSessionManager(appName)
        return instance.spark

    @staticmethod
    def close_session(appName: str):
        SparkSessionManagerBase.close_session(appName, "local")
