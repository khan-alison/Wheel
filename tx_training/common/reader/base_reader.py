from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel


class BaseReader(ABC):
    def __init__(self, spark: SparkSession, path: str, config: dict):
        self.spark = spark
        self.path = path
        self.config = config
        self.is_cache = config.get("isCache", False)
        self.storage_level = getattr(
            StorageLevel, config.get(
                "persistentLevel", "MEMORY_ONLY"), StorageLevel.MEMORY_ONLY
        )

    @abstractmethod
    def read(self) -> DataFrame:
        raise NotImplementedError

    def get_dataframe(self) -> DataFrame:
        df = self.read()
        if self.is_cache:
            df = df.persist(self.storage_level)
        return df
