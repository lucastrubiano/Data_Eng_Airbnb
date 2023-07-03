# Contains the classes, functions and variables that are common or reused in ETLs.

# ========================== INFO GENERAL ==========================
# Path: ./scripts/helper.py
# Author: Lucas Trubiano (lucas.trubiano@gmail.com)
# Created: 2023-07-02
# Updated: 2023-07-03
# Version: 1.0
# ==================================================================

from datetime import date
from databricks.sdk.runtime import *
from pyspark.sql.functions import col, lit

TABLES_PATH = f"/dbfs/FileStore/tables"
BI_CORP_PATH = f"{TABLES_PATH}/bi_corp"
LANDING = "landing"
LANDING_PATH = f"{BI_CORP_PATH}/{LANDING}"
STAGING = "staging"
STAGING_SCHEMA = "bi_corp_staging"
STAGING_PATH = f"{BI_CORP_PATH}/{STAGING}"
COMMON = "common"
COMMON_SCHEMA = "bi_corp_common"
COMMON_PATH = f"{BI_CORP_PATH}/{COMMON}"
BUSINESS = "business"
BUSINESS_SCHEMA = "bi_corp_business"
BUSINESS_PATH = f"{BI_CORP_PATH}/{BUSINESS}"
CSV = "csv"
BI_CORP_DBFS_PATH = BI_CORP_PATH.replace("/dbfs", "dbfs:")
CSV_READER_OPTIONS = {
    "inferSchema": False,
    "header": True,
    "delimiter": ","
}


# Return actual date in YYYY-MM-DD format
def get_process_date():
    return str(date.today())


"""
Considerations in spark_read_csv function:
* To use inferSchema is not a recommended practice because it slows down the process. The most efficient is to use a user defined schema or else leave all the fields as string (even if it takes up more disk space).
* For simplicity, I chose not to infer schema and left all fields as strings
"""


# Function to read a csv file and load into a Spark dataframe
def spark_read_csv(spark_session, table_name: str, process_date: str, options: dict):
    return (
        spark_session.read.format("csv")
        .options(**options)
        .load(f'{BI_CORP_DBFS_PATH}/{LANDING}/{table_name}/{process_date.replace("-", "/")}')
        .withColumn("partition_date", lit(process_date))
    )


# Function to write a Spark dataframe into a parquet file
def spark_write_df(
    df,
    schema: str,
    table_name: str,
    layer: str,
    repartition: int = 1,
    write_mode: str = "overwrite",
):
    # mode: overwrite/append
    df.repartition(repartition).write.format("parquet").mode(write_mode).partitionBy(
        "partition_date"
    ).option("path", f"{BI_CORP_DBFS_PATH}/{layer}/{table_name}").saveAsTable(
        f"{schema}.{table_name}"
    )
    print(f"Finished writing table: {schema}.{table_name}")
