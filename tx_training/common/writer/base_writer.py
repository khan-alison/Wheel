from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from tx_training.common.scd_handler import SCD_Handler


class BaseWriter(ABC):
    def __init__(self, spark: SparkSession, scd_handler: SCD_Handler = None, scd_conf: dict = None, options: dict = None):
        self.spark = spark
        self.scd_handler = scd_handler
        self.scd_conf = scd_conf
        self.options = options if options else {}

    @abstractmethod
    def write(self):
        raise NotImplementedError
