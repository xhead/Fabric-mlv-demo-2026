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

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW gold.sales AS
# MAGIC 
# MAGIC SELECT
# MAGIC      s.SalesDate
# MAGIC     ,s.SalesId
# MAGIC     ,s.SeasonCode
# MAGIC     ,s.ItemNumber
# MAGIC     ,s.StoreNumber
# MAGIC     ,s.StationNumber
# MAGIC     ,s.RouteNumber
# MAGIC     ,s.SalespersonId
# MAGIC     ,s.DocumentNumber
# MAGIC     ,s.SalesType
# MAGIC     ,CAST(s.SaleUnitQuantity / COALESCE(i.QuantityPerContainer, 1) AS DECIMAL(14, 4)) AS SaleQuantity
# MAGIC     ,s.SaleUnitQuantity
# MAGIC     ,s.SalePrice
# MAGIC     ,s.SaleAmount
# MAGIC     ,CAST(s.ReturnUnitQuantity / COALESCE(i.QuantityPerContainer, 1) AS DECIMAL(14, 4)) AS ReturnQuantity
# MAGIC     ,s.ReturnUnitQuantity
# MAGIC     ,s.ReturnPrice
# MAGIC     ,s.ReturnAmount
# MAGIC     ,s.LastUpdated
# MAGIC FROM silver.Sales s
# MAGIC LEFT JOIN silver.Item i ON s.ItemNumber = i.ItemNumber
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW MATERIALIZED LAKE VIEWS in gold


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE table gold.item

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY silver.item

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

itemNo = 10000296y/  

spark.sql(f"""
SELECT
    ItemNumber,
    Item,
    LastUpdated
FROM silver.Item
WHERE ItemNumber = {itemNo}
""").show(truncate=False)

spark.sql(f"""
UPDATE silver.Item
SET
    Item = concat(Item, ' ', date_format(current_timestamp(), 'HH:mm:ss')),
    LastUpdated = current_timestamp()
WHERE ItemNumber = {itemNo}
""")

spark.sql(f"""
SELECT
    ItemNumber,
    Item,
    LastUpdated
FROM silver.Item
WHERE ItemNumber = {itemNo}
""").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC REFRESH MATERIALIZED LAKE VIEW gold.sales


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dbo.sys_dq_metrics 
# MAGIC order by RefreshTimestamp desc
# MAGIC limit 100

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
