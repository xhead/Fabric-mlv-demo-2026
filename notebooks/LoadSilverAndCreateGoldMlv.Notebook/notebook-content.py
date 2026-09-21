# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "MLVDemoLakehouse"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import DataFrame


SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
LAKEHOUSE_FILES = "/lakehouse/default/Files"
SILVER_DATA_ROOT = f"{LAKEHOUSE_FILES}/data/silver"
SALES_BATCH_ROOT = f"{LAKEHOUSE_FILES}/data/silver/batches"
GOLD_SQL_ROOT = f"{LAKEHOUSE_FILES}/sql/gold"
TABLES = ("Date", "Sales", "Store", "Item", "PurchaseOrder")
# Keep True for the first run. Set False for later append-only runs.
LOAD_INITIAL_DATA = True
# Set this to a file name such as "SalesBatch_0001.csv" to append a batch.
SALES_BATCH_FILE = ""


def load_csv_as_silver(table_name: str) -> DataFrame:
    source_path = f"{SILVER_DATA_ROOT}/{table_name}.csv"
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(source_path)
    )
    if not df.columns:
        raise ValueError(f"Input file has no columns: {source_path}")

    spark.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.{table_name}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{SILVER_SCHEMA}.{table_name}")
    )
    return df


def read_gold_sql(view_name: str) -> str:
    sql_path = f"{GOLD_SQL_ROOT}/{view_name}.sql"
    sql_text = notebookutils.fs.head(sql_path)
    if not sql_text.strip():
        raise ValueError(f"Gold SQL file is empty: {sql_path}")
    return sql_text


def append_sales_batch(batch_file: str) -> None:
    batch_path = f"{SALES_BATCH_ROOT}/{batch_file}"
    batch_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(batch_path)
    )
    required_columns = set(loaded["Sales"].columns)
    actual_columns = set(batch_df.columns)
    missing_columns = required_columns - actual_columns
    if missing_columns:
        raise ValueError(
            f"Sales batch {batch_path} is missing columns: {sorted(missing_columns)}"
        )
    (
        batch_df.select(loaded["Sales"].columns)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{SILVER_SCHEMA}.Sales")
    )
    print(f"Appended {batch_df.count()} rows from {batch_path}")


def create_gold_mlv(view_name: str) -> None:
    definition = read_gold_sql(view_name)
    qualified_name = f"{GOLD_SCHEMA}.{view_name}"
    existing_mlvs = {
        row.name
        for row in spark.sql(f"SHOW MATERIALIZED LAKE VIEWS IN {GOLD_SCHEMA}").collect()
    }

    if view_name not in existing_mlvs:
        spark.sql(f"DROP TABLE IF EXISTS {qualified_name}")

    ddl = (
        f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {qualified_name} AS\n"
        f"{definition}"
    )
    spark.sql(ddl)
    display(spark.sql(f"SELECT * FROM {qualified_name} LIMIT 10"))


# CELL ********************

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

loaded = {}
if LOAD_INITIAL_DATA:
    for table in TABLES:
        loaded[table] = load_csv_as_silver(table)
        print(f"Loaded {table}: {loaded[table].count()} rows")
else:
    for table in TABLES:
        qualified_name = f"{SILVER_SCHEMA}.{table}"
        if not spark.catalog.tableExists(qualified_name):
            raise ValueError(
                f"Cannot run append-only mode because {qualified_name} does not exist"
            )
        loaded[table] = spark.table(qualified_name)


# CELL ********************

if SALES_BATCH_FILE:
    if LOAD_INITIAL_DATA:
        raise ValueError(
            "Set LOAD_INITIAL_DATA to False before appending a sales batch "
            "to an existing silver table."
        )
    append_sales_batch(SALES_BATCH_FILE)


# CELL ********************

for table in TABLES:
    create_gold_mlv(table)
    print(f"Created {GOLD_SCHEMA}.{table}")
