# Fabric MLV demo

This folder is a Git-ready demo for a separate Fabric tenant and lakehouse.

## Contents

- `data/silver/*.csv` - synthetic, representative extracts for four demo
  inputs: 1,000 Canadian plant varieties, every date in 2026, 5,000 Canadian
  stores, and purchase orders. The notebook generates 50,000 synthetic sales
  rows programmatically.
- `sql/gold/*.sql` - gold materialized lake view definitions
- `LoadSilverAndCreateGoldMlv.Notebook/notebook-content.py` - Fabric
  PySpark notebook that loads the CSV files into silver Delta tables and
  creates/replaces the gold MLVs.
- `Demo.Notebook/notebook-content.py` - interactive Fabric notebook for
  inspecting the gold MLVs, viewing Delta history, updating an Item row to
  create a CDF change, refreshing `gold.sales`, and reviewing data-quality
  metrics.
- `MLV Refresh.DataPipeline/pipeline-content.json` - Fabric Data Pipeline
  named `MLV Refresh` with a `RefreshMaterializedLakeView` activity.

The CSVs are intentionally small and contain no production export. Replace
them with an approved export before presenting production-shaped results.

## Fabric setup

1. Fork this repository into the user's GitHub organization or account. Work
   from the fork so the Fabric workspace has its own repository connection and
   can receive future changes independently.
2. Create or open the target Fabric workspace and bind it to the fork using
   the workspace's Git integration.
3. Use **Update from Git** to sync the fork's contents to the workspace.
   The `LoadSilverAndCreateGoldMlv` notebook, `Demo` notebook, and `MLV
   Refresh` pipeline are Fabric items in this repository; they should appear
   in the workspace after synchronization rather than being imported
   individually.
4. Create or open the demo lakehouse and attach it as the
   `LoadSilverAndCreateGoldMlv` notebook's default lakehouse. Update the
   notebook and pipeline lakehouse, workspace, and connection references for
   the target environment if Fabric does not resolve them automatically.
5. Copy this folder's `data/silver` directory to the lakehouse Files area as
   `Files/data/silver`, and copy `sql/gold` as `Files/sql/gold`. These
   supporting files are lakehouse inputs and are not Fabric items synced by
   the workspace Git integration.
6. Run the `LoadSilverAndCreateGoldMlv` notebook from the synchronized
   workspace. Its default run loads the initial 50,000 sales and does not
   append a batch.

The notebook creates `silver` and `gold` schemas. It writes the four CSV input
tables and generated sales data as Delta tables, enables Delta Change Data Feed
(CDF) on every silver table, and creates these MLVs:

- `gold.Date`
- `gold.Sales`
- `gold.Store`
- `gold.Item`
- `gold.PurchaseOrder`

## Demo notebook and refresh pipeline

After running the load notebook, run the synchronized `Demo` notebook to walk
through the MLV lifecycle. It:

1. Creates or replaces `gold.sales` with the sales and item quantity
   calculation.
2. Lists the materialized lake views and describes `gold.item`.
3. Displays Delta history for `silver.item`.
4. Updates a selected `silver.Item` row so the change appears in CDF.
5. Refreshes `gold.sales`.
6. Queries `dbo.sys_dq_metrics` to inspect recent refresh results.

The Demo notebook contains a sample `itemNo` assignment; replace it with an
`ItemNumber` that exists in the loaded `silver.Item` table before running the
update cell.

The `MLV Refresh` pipeline provides the operational version of the refresh
step. Configure the synchronized `MLV Refresh` pipeline's workspace,
lakehouse, and connection references for the target Fabric environment before
running it. Its `RefreshMaterializedLakeView` activity refreshes the
materialized lake views without rerunning the silver load or rebuilding the
gold definitions.

## Simulating changes

To update a non-sales row and capture the update in CDF:

1. Set `LOAD_INITIAL_DATA = False`.
2. Set `UPDATE_ITEM_SAMPLE` to the number of random rows to update, for example
   `UPDATE_ITEM_SAMPLE = 10`.
3. Run the notebook. The selected `silver.Item` rows are updated and the gold
   MLVs are recreated.

To append a generated sales batch after the initial load:

1. Set `LOAD_INITIAL_DATA = False`.
2. Set `GENERATE_SALES_BATCH = True`.
3. Run the notebook. A deterministic 1,000-row batch is appended to
   `silver.Sales`; the gold MLVs are then recreated so `gold.Sales` reflects
   the new rows.

CDF changes can be read with Delta's `readChangeFeed` option after the
corresponding table's CDF property has been enabled.

## Replacing the sample files

Keep the header names and compatible data types when replacing the CSVs. The
notebook uses `inferSchema`; for a production-quality demo, pin the schemas
and add validation before loading sensitive or large extracts.
