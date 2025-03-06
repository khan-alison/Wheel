from tx_training.common.reader.base_reader import BaseReader
from pyspark.sql import SparkSession, DataFrame


class CSVReader(BaseReader):
    def __init__(self, spark: SparkSession, path: str, config: dict):
        super().__init__(spark, path, config)

    def read(self) -> DataFrame:
        options = self.config.get("option")
        reader = self.spark.read.format("csv")
        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.path)
