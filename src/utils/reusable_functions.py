from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Handling nulls

def handle_nulls(df: DataFrame, fill_map: dict) -> DataFrame:
    """
    fill_map = {"col1": 0, "col2": "UNKNOWN"}
    """
    return df.fillna(fill_map)


# Remove duplicates

def remove_duplicates(df: DataFrame, subset_cols: list) -> DataFrame:
    return df.dropDuplicates(subset_cols)

# Trim and Standardize Case

def standardize_string(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, upper(trim(col(col_name))))


# Rename Columns

def rename_columns(df: DataFrame, col_map: dict) -> DataFrame:
    for old, new in col_map.items():
        df = df.withColumnRenamed(old, new)
    return df


# Drop Columns

def drop_columns(df: DataFrame, cols: list) -> DataFrame:
    return df.drop(*cols)

# Add derived columns


def add_derived_column(df: DataFrame, new_col: str, expr_str: str) -> DataFrame:
    return df.withColumn(new_col, expr(expr_str))


# Data type and format conversions

def convert_column(df: DataFrame, col_name: str, dtype: str)  transformations

def cast_column(df: DataFrame, col_name: str, data_type) -> DataFrame:
    return df.withColumn(col_name, col(col_name).cast(data_type))


def parse_date(df: DataFrame, col_name: str, fmt="yyyy-MM-dd") -> DataFrame:
    return df.withColumn(col_name, to_date(col(col_name), fmt))


# Filtoring and conditional lgic

def filter_rows(df: DataFrame, condition: str) -> DataFrame:
    return df.filter(condition)

def add_flag_column(df: DataFrame, new_col: str, condition: str) -> DataFrame:
    return df.withColumn(new_col, when(expr(condition), lit(1)).otherwise(lit(0)))

# Join and Enrichment

def join_dataframes(df1: DataFrame, df2: DataFrame, join_key: str, join_type: str = "inner") -> DataFrame:
    return df1.join(df2, join_key, join_type)

def enrich_with_lookup(df: DataFrame, lookup_df: DataFrame, join_key: str) -> DataFrame:
    return df.join(lookup_df, join_key, "left   outer")

def join_df(
    df1: DataFrame,
    df2: DataFrame,
    join_cond,
    join_type="left"
) -> DataFrame:
    return df1.join(df2, join_cond, join_type)

# Sampling
def sample_data(df: DataFrame, sample_frac: float, seed: int = 42) -> DataFrame:
    return df.sample(sample_frac, seed) 


# Aggregations

def aggregate_df(df: DataFrame, group_cols: list, agg_exprs: dict) -> DataFrame:
    """
    agg_exprs = {"sales": "sum", "qty": "avg"}
    """
    agg_list = [getattr(functions, func)(col(c)).alias(f"{func}_{c}")
                for c, func in agg_exprs.items()]
    return df.groupBy(*group_cols).agg(*agg_list)


def group_and_agg(df: DataFrame, group_cols: list, agg_exprs: dict) -> DataFrame:
    """
    agg_exprs = {"col1": "avg", "col2": "sum"}
    """
    return df.groupBy(group_cols).agg   
    **agg_exprs)    

    def aggregate_df(df: DataFrame, group_cols: list, agg_exprs: dict) -> DataFrame:
    """
    agg_exprs = {"sales": "sum", "qty": "avg"}
    """


# TEMPORAL TRANSFORMATIONS
def convert_timezone(df: DataFrame, col_name: str, from_tz="UTC", to_tz="Asia/Kolkata") -> DataFrame:
    return df.withColumn(col_name, from_utc_timestamp(col(col_name), to_tz))

def add_date_parts(df: DataFrame, col_name: str) -> DataFrame:
    return (
        df.withColumn("year", year(col(col_name)))
          .withColumn("month", month(col(col_name)))
          .withColumn("day", dayofmonth(col(col_name)))
    )

# INCREMENTAL / CDC (DELTA MERGE)


def merge_delta(
    spark,
    source_df: DataFrame,
    target_table: str,
    merge_condition: str
):
    source_df.createOrReplaceTempView("source_view")

    merge_sql = f"""
    MERGE INTO {target_table} t
    USING source_view s
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)

# SCD TYPE 2 (COMMON INTERVIEW FAVORITE)

def scd_type_2(
    df: DataFrame,
    business_key: str,
    start_date_col="start_date",
    end_date_col="end_date",
    is_current_col="is_current"
) -> DataFrame:
    return (
        df.withColumn(start_date_col, current_date())
          .withColumn(end_date_col, lit(None).cast(DateType()))
          .withColumn(is_current_col, lit(True))
    )

# SECURITY TRANSFORMATIONS
Mask Column
def mask_column(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, regexp_replace(col(col_name), ".(?=.{4})", "*"))

Hash Column
def hash_column(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, sha2(col(col_name), 256))

# PERFORMANCE OPTIMIZATION
def repartition_df(df: DataFrame, num_partitions: int) -> DataFrame:
    return df.repartition(num_partitions)

def cache_df(df: DataFrame) -> DataFrame:
    return df.cache()

# METADATA / AUDIT COLUMNS
def add_audit_columns(df: DataFrame, source_system: str) -> DataFrame:
    return (
        df.withColumn("created_at", current_timestamp())
          .withColumn("source_system", lit(source_system))
    )


# BRONZE → SILVER → GOLD HELPERS

def write_delta(df: DataFrame, table_name: str, mode="append"):
    df.write.format("delta").mode(mode).saveAsTable(table_name)

'''
HOW TO USE UTILS IN YOUR NOTEBOOK
----------------------------------
--%pip install git+https://github.com/databricks-industry-solutions/etl-utils.git

from etl_utils import *

df = spark.read.csv("/mnt/raw/orders.csv", header=True)

df = handle_nulls(df, {"amount": 0})
df = standardize_string(df, "customer_name")
df = add_derived_column(df, "order_value", "amount * quantity")
df = add_audit_columns(df, "ERP")

write_delta(df, "silver.orders")
'''



