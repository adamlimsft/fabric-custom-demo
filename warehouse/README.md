# Warehouse in Gold

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

The notebooks build Silver in the lakehouse. This module puts a Fabric Data Warehouse on top of it and builds a star
schema with stored procedures. It's the pattern a SQL Server or SSIS team already knows, running on Fabric, reading
the same Silver tables the framework writes. Use it when the conversation is "should Gold be a warehouse?".

```
 lh_mdf (lakehouse)                         wh_mdf (warehouse, same workspace)
 silver.<entity>   (SCD2)  --3-part name-->  stg.*  --procedures-->  dw.dim_<entity>   SCD2, unknown member -1
 silver.<event>    (SCD1)  --3-part name-->  stg.*  --procedures-->  dw.fact_<event>   MERGE, as-at key, late keys
 silver.<reading>  (15min) --3-part name-->  stg.*  --procedures-->  dw.fact_<x>_daily recompute affected days
                                             etl.load_log, etl.watermark, etl.late_arriving   sec.*  RLS / CLS / masking
```

I keep PII out of the warehouse on purpose. There's no party or customer dimension here: that data stays in the
lakehouse, already masked by the framework.

## What's in each pack folder

| Script | What it does |
|---|---|
| `01_setup.sql` | Schemas `dw`, `stg`, `etl`, `sec`. Run log, watermark and late-arriving tables. Staging tables, the star schema, keys (`NOT ENFORCED`), `dw.dim_date` generated for 2020 to 2028 and the `-1` unknown member. Re-runnable: it drops and recreates everything. |
| `02_procs.sql` | One procedure per table plus `etl.usp_run_all`. |
| `03_security_demo.sql` | Reconciliation against Silver, run log, demo queries, zero-copy clone, time travel, column-level security, dynamic data masking and row-level security. |

What the procedures do:

- **Watermarks.** Each table reads Silver incrementally: `_silver_updated_at` for the register and the events,
  `_ingested_at` for readings. Every run appends to `etl.watermark`, so you can show the trail.
- **SCD2 replayed from Silver.** Silver already keeps every version of the register. The dimension procedure stages all
  versions of any changed row and inserts a new warehouse version only when status, the tier column or the capacity
  column changed. Name, type, region and start date are type 1. The first version of a new member is valid from
  1900-01-01, so older events still find it.
- **As-at key lookup.** An event gets the dimension version valid when it was opened, else the current version, else `-1`.
- **Late-arriving keys.** Rows that land on `-1` are logged in `etl.late_arriving` and re-pointed on a later run once
  the member exists. The readings fact recomputes any day that still has `-1` rows.
- **Run control.** Each procedure runs its writes in one transaction inside `TRY/CATCH`, writes to `etl.load_log`, and
  re-throws on error so the pipeline activity fails properly.

## Run it

1. Run the notebook path at least once (`nb_mdf_90_run_all`), so `lh_mdf` has the Silver tables.
2. In the same workspace, select **New item > Warehouse** and name it `wh_mdf`. The scripts use the name `lh_mdf` in
   3-part names, so keep both names or search and replace.
3. In the warehouse, select **New SQL query**. Open `warehouse/<pack>/01_setup.sql`, paste it all and run it.
   Do the same with `02_procs.sql`.
4. Load: `EXEC etl.usp_run_all;` It returns one row per table from the run log.
5. Open `03_security_demo.sql` and run it one section at a time while you talk. The order is: reconcile, run log,
   demo queries, clone, time travel, column-level security, masking, then row-level security last.

The scripts use `GO` between batches. The Fabric SQL query editor runs each `GO` batch in its own session, which is why
nothing relies on variables or transactions across batches.

## What to demo

- **Reconciliation** (section 1 of script 3). Silver and warehouse counts and totals side by side, `OK` or `CHECK`.
  This is the parallel-run evidence people ask for before a cut-over.
- **Incremental and idempotent.** Run `EXEC etl.usp_run_all;` twice. The second run stages nothing for the dimension
  and the event fact, and changes no rows. Then release the next sample day, run the framework and run the procedure
  again: only changed rows move, and the watermark trail grows.
- **SCD2.** The day 2 and day 3 sample data change status and tier on some rows. The "SCD2 history" query in section 3
  shows the versions with `valid_from` and `valid_to`.
- **Late-arriving keys.** In the sample data, day 2 has six events for member 199999, which the source only publishes
  on day 3. The framework holds them in Silver quarantine until then. When they reach the warehouse they still get a
  real key, even though they were opened before the member existed, because a new member's first version starts at
  1900-01-01. To see the `-1` repair itself, run `01_setup.sql` again, run `EXEC etl.usp_load_<event fact>;` on its own
  (every row lands on `-1`), then `EXEC etl.usp_run_all;` and look at section 2 of script 3.
- **Clone and time travel.** A zero-copy clone of the fact, and the fact as it was after the first load.
- **Security.** Column-level security on the amount column, masking on the name column, and row-level security on the
  region column for the dimension and both facts.

Security notes I always say out loud:

- Workspace Admin, Member and Contributor roles see everything unmasked. Test as a user who only has Read on the
  warehouse.
- Row-level security filters everyone, including you. Script 3 maps you to `*` first. The identity the pipeline uses
  must be mapped to `*` too: `etl.usp_run_all` refuses to load if row-level security is set up and the caller isn't
  mapped, because filtered reads would write wrong history.
- These rules apply to T-SQL. Power BI in Direct Lake mode falls back to DirectQuery when the warehouse has
  row-level security. Plan the semantic model with that in mind.

## Call it from the pipeline

In `pl_mdf_orchestrator`, add a **Stored procedure** activity after the Bronze to Silver step:

1. **Settings > Connection**: the warehouse `wh_mdf` (a Fabric Data Warehouse connection).
2. **Stored procedure name**: `etl.usp_run_all`. No parameters.
3. Dependency: **On success** of the Silver step. If you also run the Gold notebook, the two can run in parallel.
4. Add the connection's identity to `sec.user_region` with `*` if you have set up row-level security.

A failure inside any procedure is logged in `etl.load_log` and re-thrown, so the activity goes red with the original
error message.

## Packs

| Pack | Dimension | Event fact | Daily reading fact | Region column | SCD2 columns |
|---|---|---|---|---|---|
| energy | `dw.dim_asset` | `dw.fact_work_order` | `dw.fact_tx_load_daily` | `region` | status, criticality, rating_mva |
| telco | `dw.dim_network_element` | `dw.fact_fault_ticket` | `dw.fact_cell_kpi_daily` | `region` | status, criticality, capacity_users |
| fsi | `dw.dim_account` | `dw.fact_risk_case` | `dw.fact_txn_monitor_daily` | `state` | status, risk_grade, balance_aud |
| manufacturing | `dw.dim_machine` | `dw.fact_maintenance_order` | `dw.fact_machine_telemetry_daily` | `plant` | status, criticality, rated_units_per_hour |
| healthcare | `dw.dim_ward` | `dw.fact_admission` | `dw.fact_ward_occupancy_daily` | `hospital` | status, acuity_level, bed_count |
| ites | `dw.dim_client_service` | `dw.fact_incident` | `dw.fact_service_perf_daily` | `delivery_centre` | status, sla_tier, monthly_fee_aud |
| government | `dw.dim_public_asset` | `dw.fact_citizen_case` | `dw.fact_sensor_daily` | `district` | status, criticality, replacement_value_kaud |
| retail | `dw.dim_store` | `dw.fact_stock_issue` | `dw.fact_store_trading_daily` | `state` | status, store_tier, weekly_sales_kaud |
| digital | `dw.dim_tenant` | `dw.fact_support_ticket` | `dw.fact_usage_daily` | `cloud_region` | status, account_tier, seats |

All packs also get `dw.dim_date` (`date_key`, `calendar_date`, `fy_year`, `fy_label` with FY27 = July 2026 to June 2027).

## Fabric T-SQL notes

These are the limits the scripts are written around. Check the Microsoft Learn page "T-SQL surface area in Fabric
Data Warehouse" if something fails.

- `MERGE` is generally available. It needs the closing semicolon and has no `OUTPUT` clause here.
- `IDENTITY` exists but is `BIGINT` only, with no seed and no `IDENTITY_INSERT`. The scripts use `INT` keys from
  `MAX + ROW_NUMBER`, so `-1` can be the unknown member.
- No sequences, no triggers, no `SET TRANSACTION ISOLATION LEVEL`. Text columns are `VARCHAR` (no `NVARCHAR` columns),
  timestamps are `DATETIME2(6)`, and keys must be `NOT ENFORCED`.
- Only some `ALTER TABLE` options work: add nullable columns, add or drop `NOT ENFORCED` keys, and masking.
- The 3-part names (`lh_mdf.silver.x`) assume the lakehouse and the warehouse sit in the same workspace.

Verify in your tenant:

- The lakehouse SQL analytics endpoint syncs a few seconds to minutes after a notebook writes. If the reconciliation
  shows `CHECK` straight after a load, wait and run it again.
- `sp_executesql` with the time travel hint, `DATABASE_PRINCIPAL_ID`, and `ALTER COLUMN ... ADD MASKED` on an existing
  table. All three are documented, but they're newer than the rest.
- Lakehouse table and column names come from the pack. A pack column that is a T-SQL keyword is written in square
  brackets (for example `[plan]` in the digital pack).

## Rebuild and checks

- `python tools/build_kit.py` rebuilds everything, including these scripts. `python tools/build_wh.py` rebuilds just the
  warehouse folder.
- `python tools/build_wh.py --check` rebuilds the scripts for all nine packs and checks every column, INSERT/MERGE
  and `dw` table against the pack's Silver schema.
