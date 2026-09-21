# Bonnie Plants MLV demo

This folder is a Git-ready demo for a separate Fabric tenant and lakehouse. It
does not create, update, or delete anything in the Bonnie Plants tenant.

## Contents

- `data/silver/*.csv` - synthetic, representative extracts for the five demo
  inputs: 1,000 Canadian plant varieties, every date in 2026, 5,000 Canadian
  stores, 500,000 sales, and purchase orders.
- `data/silver/batches/SalesBatch_0001.csv` - a 1,000-row incremental sales
  batch for simulating activity after the initial load.
- `sql/gold/*.sql` - gold materialized lake view definitions based on
  `BonnieLakehouse.Files/PipelineTasks/gold`.
- `notebooks/LoadSilverAndCreateGoldMlv.Notebook/notebook-content.py` - Fabric
  PySpark notebook that loads the CSV files into silver Delta tables and
  creates/replaces the gold MLVs.

The CSVs are intentionally small and contain no production export. Replace
them with an approved export before presenting production-shaped results.

## Fabric setup

1. Create or open the demo lakehouse and attach it as the notebook's default
   lakehouse.
2. Copy this folder's `data/silver` directory to the lakehouse Files area as
   `Files/data/silver`.
3. Copy this folder's `sql/gold` directory to the lakehouse Files area as
   `Files/sql/gold`.
4. Import the notebook into the new tenant/repository and run it. The default
   run loads the initial 500,000 sales and does not append a batch.

The notebook creates `silver` and `gold` schemas. It writes the five input
tables as Delta tables and creates these MLVs:

- `gold.Date`
- `gold.Sales`
- `gold.Store`
- `gold.Item`
- `gold.PurchaseOrder`

The demo's `Date` definition reads `silver.Date` rather than the production
`reference.BonnieCalendar`, and the demo's `Sales` definition uses the
station/route values already present in `silver.Sales` rather than requiring
the production `silver.StoreRouteMap` dependency. These are the only
dependency reductions needed to keep the demo to five silver inputs.

## Simulating incremental sales

To append a batch after the initial load:

1. Copy a batch CSV into `Files/data/silver/batches`.
2. Set `LOAD_INITIAL_DATA = False` and set `SALES_BATCH_FILE` to the file name,
   for example `SalesBatch_0001.csv`.
3. Run the notebook. The batch is appended to the existing `silver.Sales`; the gold MLVs
   are then recreated so `gold.Sales` reflects the new rows.

Each batch must use the same columns as the initial `Sales.csv` file. The
included batch contains 1,000 rows with non-overlapping `SalesId` values.

## Replacing the sample files

Keep the header names and compatible data types when replacing the CSVs. The
notebook uses `inferSchema`; for a production-quality demo, pin the schemas
and add validation before loading sensitive or large extracts.
