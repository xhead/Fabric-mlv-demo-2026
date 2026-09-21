# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d4b47789-20e5-4dd7-9d95-3edebb453ab7",
# META       "default_lakehouse_name": "DemoLakehouse",
# META       "default_lakehouse_workspace_id": "0e192b3f-2b04-42b8-9234-a6c9b74b5c92",
# META       "known_lakehouses": [
# META         {
# META           "id": "d4b47789-20e5-4dd7-9d95-3edebb453ab7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
LAKEHOUSE_FILES = "Files"
SILVER_DATA_ROOT = f"{LAKEHOUSE_FILES}/data/silver"
GOLD_SQL_ROOT = f"{LAKEHOUSE_FILES}/sql/gold"
TABLES = ("Date", "Sales", "Store", "Item", "PurchaseOrder")
SALES_ROW_COUNT = 50000
SALES_START_ID = 500001
SALES_BATCH_ROW_COUNT = 1000
SALES_BATCH_START_ID = 1000001
# Set True to create a CDF update on silver.Item.
UPDATE_ITEM_SAMPLE = True
# Set True on an append-only run to add generated sales rows.
GENERATE_SALES_BATCH = False
# Keep True for the first run. Set False for later append-only runs.
LOAD_INITIAL_DATA = False


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
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(f"{SILVER_SCHEMA}.{table_name}")
    )
    return df


def enable_change_data_feed(table_name: str) -> None:
    spark.sql(
        f"ALTER TABLE {SILVER_SCHEMA}.{table_name} "
        "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def build_sales_dataframe(
    row_count: int = SALES_ROW_COUNT,
    starting_sales_id: int = SALES_START_ID,
) -> DataFrame:
    row_number = F.col("row_number")
    sales_id = row_number + F.lit(starting_sales_id)
    sale_units = (F.pmod(row_number, F.lit(31)) + F.lit(1)).cast("int")
    sale_price = F.round(
        F.lit(2.49) + F.pmod(row_number, F.lit(250)) * F.lit(0.01), 2
    )
    return_amount = F.when(F.pmod(row_number, F.lit(10)) == 0, sale_units * sale_price).otherwise(
        F.lit(0.0)
    )
    return (
        spark.range(row_count)
        .withColumnRenamed("id", "row_number")
        .withColumn("SalesDate", F.date_add(F.lit("2026-01-01"), F.pmod(row_number, F.lit(365)).cast("int")))
        .withColumn("SalesId", sales_id)
        .withColumn("SeasonCode", F.lit("FY26"))
        .withColumn("ItemNumber", F.lit(100001) + F.pmod(row_number, F.lit(999)).cast("int"))
        .withColumn("StoreNumber", F.format_string("S%05d", F.pmod(row_number, F.lit(5000)) + 1))
        .withColumn("StationNumber", F.format_string("ST%03d", F.pmod(row_number, F.lit(100)) + 1))
        .withColumn("RouteNumber", F.format_string("%04d", F.pmod(row_number, F.lit(5000)) + 1))
        .withColumn("SalespersonId", F.format_string("SP%03d", F.pmod(row_number, F.lit(250)) + 1))
        .withColumn("DocumentNumber", F.format_string("INV-%08d", sales_id))
        .withColumn("SalesType", F.lit("SALE"))
        .withColumn("SaleUnitQuantity", sale_units)
        .withColumn("SalePrice", sale_price)
        .withColumn("SaleAmount", F.round(sale_units * sale_price, 2))
        .withColumn("ReturnUnitQuantity", F.when(F.pmod(row_number, F.lit(10)) == 0, sale_units).otherwise(F.lit(0)))
        .withColumn("ReturnPrice", F.when(F.pmod(row_number, F.lit(10)) == 0, sale_price).otherwise(F.lit(0.0)))
        .withColumn("ReturnAmount", F.round(return_amount, 2))
        .withColumn("LastUpdated", F.to_timestamp(F.lit("2026-09-20T12:00:00")))
        .select(
            "SalesDate",
            "SalesId",
            "SeasonCode",
            "ItemNumber",
            "StoreNumber",
            "StationNumber",
            "RouteNumber",
            "SalespersonId",
            "DocumentNumber",
            "SalesType",
            "SaleUnitQuantity",
            "SalePrice",
            "SaleAmount",
            "ReturnUnitQuantity",
            "ReturnPrice",
            "ReturnAmount",
            "LastUpdated",
        )
    )


def generate_sales_as_silver(
    row_count: int = SALES_ROW_COUNT,
    starting_sales_id: int = SALES_START_ID,
) -> DataFrame:
    df = build_sales_dataframe(row_count, starting_sales_id)
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.Sales")
    (
        df.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(f"{SILVER_SCHEMA}.Sales")
    )
    return df


def append_generated_sales_batch(
    row_count: int = SALES_BATCH_ROW_COUNT,
    starting_sales_id: int = SALES_BATCH_START_ID,
) -> None:
    batch_df = build_sales_dataframe(row_count, starting_sales_id)
    (
        batch_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{SILVER_SCHEMA}.Sales")
    )
    print(f"Appended {batch_df.count()} generated sales rows")


def update_item_sample() -> None:
    spark.sql(
        f"""
        UPDATE {SILVER_SCHEMA}.Item
        SET
            Item = 'Genovese Basil 0001 - Updated',
            LastUpdated = CAST('2026-09-20T12:00:00' AS TIMESTAMP)
        WHERE ItemNumber = 100001
        """
    )
    print("Updated silver.Item row for ItemNumber 100001")


def read_gold_sql(view_name: str) -> str:
    sql_path = f"{GOLD_SQL_ROOT}/{view_name}.sql"
    sql_text = notebookutils.fs.head(sql_path)
    if not sql_text.strip():
        raise ValueError(f"Gold SQL file is empty: {sql_path}")
    return sql_text


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



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

loaded = {}
if LOAD_INITIAL_DATA:
    for table in TABLES:
        loaded[table] = (
            generate_sales_as_silver()
            if table == "Sales"
            else load_csv_as_silver(table)
        )
        print(f"Loaded {table}: {loaded[table].count()} rows")
else:
    for table in TABLES:
        qualified_name = f"{SILVER_SCHEMA}.{table}"
        if not spark.catalog.tableExists(qualified_name):
            raise ValueError(
                f"Cannot run append-only mode because {qualified_name} does not exist"
            )
        loaded[table] = spark.table(qualified_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


for table in TABLES:
    enable_change_data_feed(table)

if UPDATE_ITEM_SAMPLE:
    update_item_sample()

if GENERATE_SALES_BATCH:
    if LOAD_INITIAL_DATA:
            raise ValueError(
                "Set LOAD_INITIAL_DATA to False before appending a sales batch "
                "to an existing silver table."
            )
    append_generated_sales_batch()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


for table in TABLES:
    create_gold_mlv(table)
    print(f"Created {GOLD_SCHEMA}.{table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
