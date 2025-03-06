from tx_training.common.writer.base_writer import BaseWriter
from pyspark.sql import SparkSession, DataFrame
from tx_training.common.scd_handler import SCD_Handler


class DeltaWriter(BaseWriter):
    def __init__(self, spark: SparkSession, scd_handler: SCD_Handler = None, scd_conf: dict = None, options: dict = None):
        super().__init__(spark, scd_handler, scd_conf, options)

    def write(self, df: DataFrame):
        partition_cols = self.scd_conf.get("partition_by", [])

        if partition_cols:
            for col in partition_cols:
                df = df.withColumn(col, F.lit(self.scd_conf.get("data_date")))

        self.scd_handler.process(df, self.scd_conf)
