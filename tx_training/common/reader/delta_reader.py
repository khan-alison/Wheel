from tx_training.common.reader.base_reader import BaseReader
from pyspark.sql import SparkSession, DataFrame


class DeltaReader(BaseReader):
    def __init__(self, spark: SparkSession, config: dict):
        super().__init__(spark, config)

    def read(self) -> DataFrame:
        if "." in self.path and not self.path.startswith("dbfs:/"):
            return self.spark.table(self.path)
        return self.spark.read.format("delta").load(self.path)
