from pyspark.sql import SparkSession
from tx_training.common.spark_session.spark_session_manager import (
    SparkSessionManagerBase,
)
from tx_training.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class DBxSparkSessionManager(SparkSessionManagerBase):
    def __init__(self, appName: str):
        self.appName = appName
        self.master = "dbx"
        self.spark = self._create_spark_session()
        SparkSessionManagerBase._instances[(appName, self.master)] = self

    def _create_spark_session(self) -> SparkSession:
        logger.info("Getting existing Databricks SparkSession")
        return SparkSession.builder.config(
            "spark.databricks.delta.schema.autoMerge.enabled", "true"
        ).getOrCreate()

    @staticmethod
    def get_session(appName: str):
        """Get a SparkSession for the application"""
        instance = SparkSessionManagerBase.get_instance(appName, "dbx")
        if instance is None:
            instance = DBxSparkSessionManager(appName)
        return instance.spark
