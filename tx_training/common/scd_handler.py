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

    def process(self, df: DataFrame, scd_conf: dict):
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
                df, output_path, output_format, "append", is_table, scd_conf
            )
        elif scd_type == "scd2":
            self._handle_scd2(
                df, output_path, output_format, save_mode, is_table, scd_conf
            )
        elif scd_type == "scd4":
            self._handle_scd4(
                df, output_path, output_format, save_mode, is_table, scd_conf
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
    ):
        primary_keys = scd_conf.get("primary_keys", ["customer_id"])
        logger.info(f"Primary keys: {primary_keys}")

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

            if is_table:
                df.write.format(output_format).option("overwriteSchema", "true").mode(
                    "overwrite"
                ).saveAsTable(output_path)
            else:
                df.write.format(output_format).option("overwriteSchema", "true").mode(
                    "overwrite"
                ).save(output_path)

            logger.info("Successfully overwritten the Delta table.")

    def _handle_scd2(
        self,
        df: DataFrame,
        output_path: str,
        output_format: str,
        save_mode: str,
        is_table: bool,
        scd_conf: Dict[str, Any],
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
                logger.info("new_versions")
                new_versions.write.format(output_format).mode("append").save(
                    output_path
                )

                logger.info("SCD Type 2 processing complete.")

        except AnalysisException:
            logger.info("Excepts")
            df_with_scd = (
                df.withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .withColumn("hash_data", hash_expr)
            )
            if is_table:
                df_with_scd.write.format(output_format).option(
                    "overwriteSchema", "true"
                ).mode("overwrite").saveAsTable(output_path)
            else:
                df_with_scd.write.format(output_format).option(
                    "overwriteSchema", "true"
                ).mode("overwrite").save(output_path)

    def _handle_scd4(
        self,
        df: DataFrame,
        output_path: str,
        output_format: str,
        save_mode: str,
        is_table: bool,
        scd_conf: Dict[str, Any],
    ):
        logger.info(
            f"Handling SCD Type 4 for {output_path['current']} and history in {output_path['history']}")
        primary_keys = scd_conf["primary_keys"]
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

            # 2. Convert them to DataFrames for comparison
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

            logger.info("join_df")
            join_df.show(20)
            logger.info("delete_records")
            delete_records.show(20)
            logger.info("update_record")
            update_records.show(20)

            old_versions_to_history = (
                delete_records.unionByName(update_records)
                .withColumn("end_date", F.current_timestamp())
            )

            logger.info("asdfasdfs")
            old_versions_to_history.show(20)

            if old_versions_to_history.count() > 0:
                logger.info(
                    f"Appending {old_versions_to_history.count()} old/deleted records to history"
                )
                old_versions_to_history.write.format(output_format).mode("append").save(
                    history_path
                )

            new_snapshot = df_with_hash
            logger.info("new_snapshot")
            new_snapshot.show(20)

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

            logger.info("SCD Type 4 processing complete.")

        except AnalysisException as e:
            logger.warning(f"Tables not found; initializing. Error: {str(e)}")
            df_with_scd = (
                df_with_hash
                .withColumn("start_date", F.current_timestamp())
                .withColumn("end_date", F.lit(None).cast("timestamp"))
            )

            if is_table:
                df_with_scd.write.format(output_format)\
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .saveAsTable(current_path)
                df_with_scd.write.format(output_format)\
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .saveAsTable(history_path)
            else:
                df_with_scd.write.format(output_format)\
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .save(current_path)
                df_with_scd.write.format(output_format)\
                    .option("overwriteSchema", "true") \
                    .mode("overwrite")\
                    .save(history_path)
                logger.info(
                    "SCD Type 4 tables initialized with incoming data.")
