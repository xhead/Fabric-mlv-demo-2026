# Copilot instructions for Fabric-mlv-demo-2026

## What this repo is

A portable, Git-ready demo of Microsoft Fabric **Materialized Lake Views
(MLVs)** for a separate ("Bonnie Plants") tenant/lakehouse. There is no
build/test/lint tooling — this is data + a PySpark notebook that must be
imported and run inside a Fabric workspace. All validation happens by running
the notebook in Fabric and inspecting the resulting Delta tables/MLVs, not
locally.

## Layout and how the pieces connect

- `data/silver/*.csv` — synthetic silver-layer inputs: `Date`, `Item`,
  `PurchaseOrder`, `Store` (Sales is generated in code, not from CSV).
  `data/silver/batches/SalesBatch_0001.csv` is legacy/unused; sales batches are
  now generated programmatically (see below).
- `sql/gold/*.sql` — one `SELECT` per gold MLV (`Date`, `Item`,
  `PurchaseOrder`, `Sales`, `Store`), mirrored from the production
  `BonnieLakehouse.Files/PipelineTasks/gold` definitions but with two
  intentional dependency reductions to keep the demo self-contained:
  - `Date.sql` reads `silver.Date` directly instead of production's
    `reference.BonnieCalendar`.
  - `Sales.sql` uses `StationNumber`/`RouteNumber` already present on
    `silver.Sales` instead of joining `silver.StoreRouteMap`.
- `notebooks/LoadSilverAndCreateGoldMlv.Notebook/notebook-content.py` — the
  only executable code. It must be run inside Fabric (uses the implicit
  `spark` and `notebookutils` globals; not runnable as a standalone script).

The notebook's flow, driven by three flags at the top of the file
(`LOAD_INITIAL_DATA`, `GENERATE_SALES_BATCH`, `UPDATE_ITEM_SAMPLE`):

1. Load the four non-Sales CSVs from `Files/data/silver` into `silver.*` Delta
   tables (`load_csv_as_silver`, `inferSchema=true`, overwrite mode).
2. Programmatically generate `silver.Sales` (50,000 deterministic synthetic
   rows via `build_sales_dataframe`) instead of reading a CSV.
3. Enable Delta Change Data Feed (CDF) on every silver table.
4. Optionally mutate data to demonstrate CDF: `UPDATE_ITEM_SAMPLE = N` updates
   N random `silver.Item` rows; `GENERATE_SALES_BATCH = True` appends 1,000
   more deterministic sales rows (`SALES_BATCH_START_ID = 1000001`).
5. Read each `sql/gold/<View>.sql` file from `Files/sql/gold` and run
   `CREATE OR REPLACE MATERIALIZED LAKE VIEW gold.<View> AS <sql>` for all five
   views (`create_gold_mlv`).

`LOAD_INITIAL_DATA` and `GENERATE_SALES_BATCH` are mutually exclusive: the
notebook raises if both are `True` (initial load must run once with
`LOAD_INITIAL_DATA = True`, then later append/mutate runs use
`LOAD_INITIAL_DATA = False`).

## Conventions to follow when editing

- **Keep `sql/gold/*.sql` and `notebook-content.py`'s `TABLES` tuple in sync**:
  every gold view name must have a matching `.sql` file, and vice versa.
- Gold SQL files are plain `SELECT` statements only (no `CREATE VIEW`
  wrapper) — the notebook supplies the `CREATE OR REPLACE MATERIALIZED LAKE
  VIEW` DDL. Follow the existing style: leading-comma column lists, one
  column per line, short table aliases (`s`, `i`, `d`), `LEFT JOIN` with
  `COALESCE` for optional divisor fields.
- When changing a gold definition to depend on a new silver column/table,
  check whether it's one of the two intentional demo-only simplifications
  documented in the README before "fixing" it back to the production
  definition.
- CSV replacements must keep existing header names and Spark-inferable types
  (loader uses `inferSchema=true` and `mode=FAILFAST`); it will raise if a
  file has no columns.
- Sales row generation is deterministic (formulas keyed off `row_number`) —
  preserve that determinism if modifying `build_sales_dataframe` so demo
  results stay reproducible.

## Fabric setup / manual verification (from README)

There's no automated test suite; verifying a change means walking through
this Fabric workflow:

1. Attach a lakehouse (e.g. `MLVDemoLakehouse`) to the notebook as default.
2. Copy `data/silver` to lakehouse `Files/data/silver` and `sql/gold` to
   `Files/sql/gold`.
3. Import and run the notebook (first run: `LOAD_INITIAL_DATA = True`).
4. To simulate a CDF update: set `LOAD_INITIAL_DATA = False`,
   `UPDATE_ITEM_SAMPLE = <n>`, rerun.
5. To simulate an appended sales batch: set `LOAD_INITIAL_DATA = False`,
   `GENERATE_SALES_BATCH = True`, rerun.
