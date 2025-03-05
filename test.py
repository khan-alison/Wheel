from delta import DeltaTable
from tx_training.common.spark_session.local_spark_session import (
    LocalSparkSessionManager,
)

spark = LocalSparkSessionManager.get_session("tx-training")

table = DeltaTable.forPath(spark, "data/output/scd4/customers/current")
table.toDF().show()
table = DeltaTable.forPath(spark, "data/output/scd4/customers/history")
table.toDF().show()
