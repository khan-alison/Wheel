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

        condition = " AND ".join(f"target.{col} = source.{col}" for col in primary_keys)

        try:
            if is_table:
                delta_table = DeltaTable.forName(self.spark, output_path)
            else:
                delta_table = DeltaTable.forPath(self.spark, output_path)

            logger.info("Attempting to append data using merge (SCD Type 1)...")

            delta_table.alias("target").merge(
                df.alias("source"), condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        except AnalysisException as e:
            logger.warning(f"Append failed or Delta table not found. Error: {str(e)}")
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
        hash_expr = F.sha2(F.concat_ws(".", *[F.col(c) for c in business_cols]), 256)
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
                    set={"end_date": F.current_timestamp(), "is_current": F.lit(False)}
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
            f"Handling SCD Type 4 for {output_path['current']} and history in {output_path['history']}"
        )
        primary_keys = scd_conf["primary_keys"]
        business_cols = [col for col in df.columns if col not in primary_keys]
        hash_expr = F.sha2(F.concat_ws(".", *[F.col(c) for c in business_cols]), 256)
        df_with_hash = df.withColumn("hash_data", hash_expr)

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
            current_df_with_hash = current_df.withColumn("hash_data", hash_expr)

            joined_df = (
                df_with_hash.alias("new")
                .join(current_df_with_hash.alias("old"), on=primary_keys, how="inner")
                .filter(F.col("new.hash_data") != F.col("old.hash_data"))
            )

            updated_records_new = joined_df.select("new.*")
            print("updated_records_new")
            updated_records_new.show(10)
            updated_records_old = joined_df.select("old.*")
            print("updated_records_old")
            updated_records_old.show(10)
            if updated_records_new.count() > 0:
                logger.info(
                    f"🔄 Processing {updated_records_new.count()} updated records..."
                )
                updated_records_old_with_end = updated_records_old.withColumn(
                    "end_date", F.current_timestamp()
                )
                updated_records_old_with_end.show(20)
                updated_records_old_with_end.write.format(output_format).mode(
                    "append"
                ).save(history_path)
                delete_condition = " AND ".join(
                    [f"target.{pk} = source.{pk}" for pk in primary_keys]
                )
                current_table.alias("target").merge(
                    updated_records_old.alias("source"), delete_condition
                ).whenMatchedDelete().execute()

                updated_records_new.write.format(output_format).mode("append").save(
                    current_path
                )

        except AnalysisException:
            logger.info("Initializing SCD Type 4 tables as they don't exist.")
            df_with_scd = df.withColumn("start_date", F.current_timestamp()).withColumn(
                "end_date", F.lit(None).cast("timestamp")
            )
            if is_table:
                df_with_scd.write.format(output_format).mode("overwrite").saveAsTable(
                    current_path
                )
                df_with_scd.write.format(output_format).mode("overwrite").saveAsTable(
                    history_path
                )
            else:
                df_with_scd.write.format(output_format).mode("overwrite").save(
                    current_path
                )
                df_with_scd.write.format(output_format).mode("overwrite").save(
                    history_path
                )
            logger.info("SCD Type 4 tables initialized with incoming data.")
