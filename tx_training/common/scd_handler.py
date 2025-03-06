from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from tx_training.helper.logger import LoggerSimple
from typing import Dict, Any
import datetime

logger = LoggerSimple.get_logger(__name__)


class SCD_Handler:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process(self, df: DataFrame, scd_conf: dict, data_date: str):
        output_path = scd_conf.get("path")
        scd_type = scd_conf.get("type", "scd2")
        logger.info(scd_type)
        output_format = scd_conf.get("format", "delta")
        primary_key = scd_conf.get("primary_keys")
        save_mode = scd_conf.get(
            "save_mode", "overwrite" if scd_type == "scd1" else "append"
        )
        is_table = "." in output_path and not output_path.startswith("dbfs:/")

        if scd_type == "scd1":
            self._handle_scd1(
                df, output_path, output_format, "append", is_table, scd_conf, data_date
            )
        elif scd_type == "scd2":
            self._handle_scd2(
                df, output_path, output_format, save_mode, is_table, scd_conf, data_date
            )
        elif scd_type == "scd4":
            self._handle_scd4(
                df, output_path, output_format, save_mode, is_table, scd_conf, data_date
            )
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")

    def _handle_scd1(
        self,
        df: DataFrame,
        output_path: str,
        output_format: str,
        save_mode: str,
        is_table: bool,
        scd_conf: Dict[str, Any],
        data_date: str
    ):
        primary_keys = scd_conf.get("primary_keys", ["customer_id"])
        partition_cols = scd_conf.get(
            "partition_by", [])

        logger.info(f"Handling SCD Type 1 for {output_path}")
        logger.info(
            f"Primary keys: {primary_keys}, Partitioning by: {partition_cols}")

        condition = " AND ".join(
            f"target.{col} = source.{col}" for col in primary_keys)

        try:
            if is_table:
                delta_table = DeltaTable.forName(self.spark, output_path)
            else:
                delta_table = DeltaTable.forPath(self.spark, output_path)

            logger.info(
                "Attempting to append data using merge (SCD Type 1)...")

            delta_table.alias("target").merge(
                df.alias("source"), condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        except AnalysisException as e:
            logger.warning(
                f"Append failed or Delta table not found. Error: {str(e)}")
            logger.info("Overwriting with new data instead.")

            writer = df.write.format(output_format).option(
                "overwriteSchema", "true").mode("overwrite")

            if partition_cols:
                writer = writer.partitionBy(partition_cols)

            if is_table:
                writer.saveAsTable(output_path)
            else:
                writer.save(output_path)

        logger.info("SCD Type 1 processing completed.")

    def _handle_scd2(
        self,
        df: DataFrame,
        output_path: str,
        output_format: str,
        save_mode: str,
        is_table: bool,
        scd_conf: Dict[str, Any],
        data_date: str
    ):
        logger.info(f"Handling SCD Type 2 for {output_path}")
        primary_keys = scd_conf["primary_keys"]
        logger.info(f"primary_keys {primary_keys}")
        business_cols = [col for col in df.columns if col not in primary_keys]
        logger.info(f"business_cols {business_cols}")
        hash_expr = F.sha2(F.concat_ws(
            ".", *[F.col(c) for c in business_cols]), 256)
        logger.info(f"hash_expr {hash_expr}")
        df_with_hash = df.withColumn("hash_data", hash_expr)
        logger.info(f"df_with_hash {df_with_hash}")
        df_with_hash.show(20)
        if data_date and "data_date" in partition_cols and "data_date" not in df.columns:
            df_with_hash = df_with_hash.withColumn(
                "data_date", F.lit(data_date))
        try:
            delta_table = (
                DeltaTable.forPath(self.spark, output_path)
                if not is_table
                else DeltaTable.forName(self.spark, output_path)
            )
            existing_df = delta_table.toDF()
            existing_df.show(20)

            join_condition = " AND ".join(
                [f"target.{col} = source.{col}" for col in primary_keys]
            )

            comparison_df = (
                existing_df.alias("target")
                .join(df_with_hash.alias("source"), primary_keys, "outer")
                .select(
                    F.col("target.*"), F.col("source.hash_data").alias("new_hash_data")
                )
            )
            changed_records = comparison_df.filter(
                F.col("hash_data") != F.col("new_hash_data")
            )
            logger.info("change")
            changed_records.show(20)

            if changed_records.count() > 0:
                logger.info(
                    f"Expiring {changed_records.count()} old records and inserting new versions..."
                )

                expired_records = (
                    existing_df.alias("target")
                    .join(changed_records.alias("source"), primary_keys, "inner")
                    .select("target.*")
                    .withColumn("end_date", F.current_timestamp())
                    .withColumn("is_current", F.lit(False))
                )

                delta_table.alias("target").merge(
                    expired_records.alias("source"), join_condition
                ).whenMatchedUpdate(
                    set={"end_date": F.current_timestamp(),
                         "is_current": F.lit(False)}
                ).execute()

                logger.info("after merfe")
                delta_table.toDF().show(20)

                new_versions = (
                    df_with_hash.alias("source")
                    .join(changed_records.alias("target"), primary_keys, "inner")
                    .select("source.*")
                    .withColumn("start_date", F.current_timestamp())
                    .withColumn("end_date", F.lit(None).cast("timestamp"))
                    .withColumn("is_current", F.lit(True))
                )
                writer = new_versions.write.format(
                    output_format).mode("append")

                if partition_cols:
                    writer = writer.partitionBy(partition_cols)

                writer.save(output_path)

                logger.info("SCD Type 2 processing complete.")

        except AnalysisException:
            logger.info("Excepts")
            df_with_scd = (
                df.withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .withColumn("hash_data", hash_expr)
            )

            if data_date and "data_date" in partition_cols and "data_date" not in df.columns:
                df_with_hash = df_with_hash.withColumn(
                    "data_date", F.lit(data_date))

            writer = df_with_scd.write.format(output_format).option(
                "overwriteSchema", "true"
            ).mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            if is_table:
                writer.saveAsTable(output_path)
            else:
                writer.save(output_path)

    def _handle_scd4(
        self,
        df: DataFrame,
        output_path: str,
        output_format: str,
        save_mode: str,
        is_table: bool,
        scd_conf: Dict[str, Any],
        data_date: str
    ):
        logger.info(
            f"Handling SCD Type 4 for {output_path['current']} and history in {output_path['history']}")
        primary_keys = scd_conf["primary_keys"]
        partition_cols = scd_conf.get("partition_by", [])
        business_cols = [col for col in df.columns if col not in primary_keys]
        hash_expr = F.sha2(F.concat_ws(
            ".", *[F.col(c) for c in business_cols]), 256)
        df_with_hash = df.withColumn("hash_data", hash_expr)
        logger.info(df_with_hash)
        current_path = output_path["current"]
        history_path = output_path["history"]

        try:
            if is_table:
                current_table = DeltaTable.forName(self.spark, current_path)
                history_table = DeltaTable.forName(self.spark, history_path)
            else:
                current_table = DeltaTable.forPath(self.spark, current_path)
                history_table = DeltaTable.forPath(self.spark, history_path)

            current_df = current_table.toDF()

            current_df_with_hash = current_df.withColumn(
                "hash_data",
                F.sha2(F.concat_ws(".", *[F.col(c)
                       for c in business_cols]), 256)
            )
            join_df = (
                current_df.alias("old")
                .join(df_with_hash.alias("new"),
                      [F.col(f"old.{pk}") == F.col(f"new.{pk}")
                       for pk in primary_keys],
                      "full")
            )

            delete_records = join_df.filter(
                F.col("old.hash_data").isNotNull() & F.col(
                    "new.hash_data").isNull()
            ).select("old.*")

            update_records = join_df.filter(
                F.col("old.hash_data").isNotNull()
                & F.col("new.hash_data").isNotNull()
                & (F.col("old.hash_data") != F.col("new.hash_data"))
            ).select("old.*")

            old_versions_to_history = (
                delete_records.unionByName(update_records)
                .withColumn("end_date", F.current_timestamp())
            )

            if old_versions_to_history.count() > 0:
                logger.info(
                    f"Appending {old_versions_to_history.count()} old/deleted records to history"
                )
                history_writer = old_versions_to_history.write.format(
                    output_format).mode("append")
                if partition_cols:
                    history_writer = history_writer.partitionBy(partition_cols)

                history_writer.save(history_path)

            new_snapshot = df_with_hash
            snapshot_writer = new_snapshot.write.format(output_format) \
                .option("overwriteSchema", "true") \
                .mode("overwrite")

            if partition_cols:
                snapshot_writer = snapshot_writer.partitionBy(partition_cols)

            if is_table:
                snapshot_writer.saveAsTable(current_path)
            else:
                snapshot_writer.save(current_path)
            logger.info("SCD Type 4 processing complete.")

        except AnalysisException as e:
            logger.warning(f"Tables not found; initializing. Error: {str(e)}")
            df_with_scd = (
                df_with_hash
                .withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
            )

            history_writer = df_with_scd.write.format(output_format) \
                .option("overwriteSchema", "true") \
                .mode("overwrite")

            snapshot_writer = df_with_scd.write.format(output_format) \
                .option("overwriteSchema", "true") \
                .mode("overwrite")

            if partition_cols:
                history_writer = history_writer.partitionBy(partition_cols)
                snapshot_writer = snapshot_writer.partitionBy(partition_cols)

            if is_table:
                snapshot_writer.saveAsTable(current_path)
                history_writer.saveAsTable(history_path)
            else:
                snapshot_writer.save(current_path)
                history_writer.save(history_path)
                logger.info(
                    "SCD Type 4 tables initialized with incoming data.")

    def _scd4_cdf(
            self,
            output_path: str,
            output_format: str,
            save_mode: str,
            is_table: bool,
            scd_conf: Dict[str, Any],
    ):
        """
        NEED TO ENABLE CDF FIRST
        """
        logger.info(
            f"Handling SCD Type 4 with CDF for {output_path['current']} and history in {output_path['history']}")
        primary_keys = scd_conf["primary_keys"]
        current_path = output_path["current"]
        history_path = output_path["history"]
        try:
            if is_table:
                current_table = DeltaTable.forName(self.spark, current_path)
            else:
                current_table = DeltaTable.forPath(self.spark, current_path)

            logger.info("Reading Change Data Feed (CDF)...")
            cdf_df = self.spark.read.format("delta") \
                .option("readChangeData", "true") \
                .table(current_path)

            delete_records = cdf_df\
                .filter(F.col("_change_type") == "delete")
            update_old_records = cdf_df\
                .filter(F.col("_change_type") == "update_preimage")
            update_new_records = cdf_df\
                .filter(F.col("_change_type") == "update_postimage")
            insert_records = cdf_df\
                .filter(F.col("_change_type") == "insert")

            if delete_records.count() > 0 or update_old_records.count() > 0:
                history_updates = delete_records.unionByName(update_old_records) \
                    .withColumn("end_date", F.current_timestamp())

                logger.info(
                    f"Appending {history_updates.count()} records to history table...")
                history_updates.write.format(output_format).mode(
                    "append").save(history_path)

            new_snapshot = insert_records.unionByName(update_new_records)

            if is_table:
                new_snapshot.write.format(output_format) \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite") \
                    .saveAsTable(current_path)
            else:
                new_snapshot.write.format(output_format) \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite") \
                    .save(current_path)

            logger.info("SCD Type 4 (CDF) processing complete.")
        except AnalysisException as e:
            logger.warning(f"Tables not found; initializing. Error: {str(e)}")
            df_with_scd = df.withColumn("start_date", F.current_timestamp()) \
                .withColumn("end_date", F.lit(None).cast("timestamp"))

            # Enable CDF for the first time
            if is_table:
                df_with_scd.write.format(output_format)\
                    .option("delta.enableChangeDataFeed", "true") \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .saveAsTable(current_path)
                df_with_scd.write.format(output_format)\
                    .option("delta.enableChangeDataFeed", "true") \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .saveAsTable(history_path)
            else:
                df_with_scd.write.format(output_format)\
                    .option("delta.enableChangeDataFeed", "true") \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .save(current_path)
                df_with_scd.write.format(output_format)\
                    .option("delta.enableChangeDataFeed", "true") \
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .save(history_path)
                logger.info(
                    "SCD Type 4 tables initialized with incoming data.")
