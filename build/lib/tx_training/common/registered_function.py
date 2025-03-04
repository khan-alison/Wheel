from functools import reduce
from tx_training.common.function_registry import FunctionRegistry
from operator import and_
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@FunctionRegistry.register()
def join_df(df1, df2, join_cols: List[str], how: str) -> DataFrame:
    df1_alias = df1.alias("df1")
    df2_alias = df2.alias("df2")
    conditions = [F.col(f"df1.{col}") == F.col(f"df2.{col}") for col in join_cols]
    join_exp = reduce(and_, conditions)

    return df1_alias.join(df2_alias, join_expr, how=how)
