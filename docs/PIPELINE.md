# Pipeline setup

The notebooks run the whole flow on their own (`nb_mdf_90_run_all`). The pipeline is the enterprise version of the same thing: a worklist read from the control tables, a parallel ForEach, a **database Copy driven by watermark queries**, retries, Silver in dependency order, then Gold.

```
Get worklist (nb_mdf_05) ──► For each object (parallel, batch 4) ────────────────► Bronze to Silver ──► Build Gold? ──► Silver to Gold
                               ├─ If copy_db: Copy SQL query → Files/landing/…csv     (runs when the ForEach      (if run_gold)
                               └─ Landing to Bronze (nb_mdf_10, object_ids=item)        has Completed)
```

Nothing in the pipeline is specific to a table or an industry. Adding a source is new metadata rows, not new activities.

Two pipelines ship in `pipelines/`:

| File | Silver and Gold built by |
|---|---|
| `pl_mdf_orchestrator.json` | notebooks `nb_mdf_20` and `nb_mdf_30` |
| `pl_mdf_orchestrator_mlv.json` | one refresh of the materialized lake views (`nb_mdf_55`) – see [mlv/README.md](../mlv/README.md) |

## Pick a path

| Path | What you get | Extra setup |
|---|---|---|
| **A. With a source database** (the one I demo) | The two "ops" tables of the pack (e.g. assets and work orders) are copied from SQL with incremental watermark queries – the Fabric equivalent of SSIS metadata-driven extracts | A Fabric SQL database, about 10 minutes |
| **B. Files only** | Same pipeline; the ops tables arrive as landed CSV files instead | None – deactivate the Copy activity |

## Path A – source database

1. In the same workspace create a **SQL database**, e.g. `sqldb_source`. It plays the pack's on-prem operational system.
2. Open a new query and run the whole of `sql/source_db/<pack>.sql`. It creates schema `src`, the two source tables (empty), the staged rows for days 1–3, and a procedure that releases one day at a time.
3. Release day 1: `EXEC src.usp_release_day 1;`
4. Tell the notebooks not to land those two tables as files. In `nb_mdf_01_generate_sample_data` set `include_db_extracts = False`, `regenerate = True`, `release_through_day = 1`, and run it. (If you already ran it with the defaults, run `nb_mdf_00` with `reset_all = True` and `nb_mdf_01` with `reset_landing = True` first.)
5. Build the pipeline (below), with your SQL database as the Copy source.
6. Run the pipeline with `run_date = 2026-11-02`.
7. Day 2: `EXEC src.usp_release_day 2;`, then `nb_mdf_01` with `release_through_day = 2`, then the pipeline with `run_date = 2026-11-03`.
8. Day 3: the same with `3` and `run_date = 2026-11-04`. Day 3 adds a new column to the entity table – the schema drift scenario.

> **Why `run_date`?** The Copy activity lands each extract in `Files/landing/<system>/<object>/load_date=<run_date>/`. Bronze only picks up load dates newer than its watermark, so each simulated day needs its own date. In production, leave it empty and today's date (Australia/Sydney) is used.

## Path B – files only

1. Run `nb_mdf_00` and `nb_mdf_01` with the defaults.
2. Build the pipeline (below), then open **Is database source ▸ True** and set **Copy source table to Landing** to *Deactivated* (General tab), or delete it. Leave `run_date` empty.
3. Release a day with `nb_mdf_01`, run the pipeline, repeat.

## Build the pipeline

1. **New item ▸ Data pipeline** ▸ `pl_mdf_orchestrator`.
2. Open the JSON code view (`{ }`), replace `activities` and `parameters` with the ones in `pipelines/pl_mdf_orchestrator.json`, save. If JSON editing isn't available to you, build it by hand from the table below (about 10 minutes).
3. Replace the placeholders – the quickest way is to reselect each item in the designer:
   - every **Notebook** activity: pick the workspace and notebook (replaces `<WORKSPACE_ID>` and `<NOTEBOOK_ID_…>`)
   - **Copy ▸ Source**: your SQL database connection. Keep *Use query = Query* with `@item().source_query`
   - **Copy ▸ Destination**: lakehouse `lh_mdf`, root folder **Files**, path `@item().landing_folder` / `@item().landing_file`, DelimitedText, first row as header
4. **Validate**, then **Run**.

| Activity | Type | Key settings |
|---|---|---|
| Get worklist | Notebook `nb_mdf_05_get_worklist` | `load_group`, `object_ids`, `rerun_failed_only`, `run_date` ← pipeline parameters |
| For each object – copy and ingest | ForEach | Items `@json(activity('Get worklist').output.result.exitValue).items`; not sequential; batch 4 |
| ↳ Is database source | If Condition | `@equals(item().ingest_method, 'copy_db')` |
| ↳↳ Copy source table to Landing | Copy | query `@item().source_query`; sink `@item().landing_folder` / `@item().landing_file` |
| ↳ Landing to Bronze | Notebook `nb_mdf_10_landing_to_bronze` | `object_ids` = `@item().object_id`, `parent_run_id` = `@pipeline().RunId`; retry 1 |
| Bronze to Silver | Notebook `nb_mdf_20_bronze_to_silver` | `object_ids` = `@json(activity('Get worklist').output.result.exitValue).object_ids`; dependency **Completed** |
| Build Gold? | If Condition | `@pipeline().parameters.run_gold` → Notebook `nb_mdf_30_silver_to_gold` |

Parameters: `load_group` (string, `all`), `object_ids` (string), `rerun_failed_only` (bool, false), `run_gold` (bool, true), `run_date` (string, empty = today).

## Extra steps you can add

- **Dataflow Gen2** – a Dataflow activity for the reference dataflow (no dependency) and one for the Gold dataflow after Silver. See [dataflows/README.md](../dataflows/README.md).
- **Warehouse in Gold** – a Stored procedure activity after Silver, warehouse `wh_mdf`, procedure `etl.usp_run_all`. See [warehouse/README.md](../warehouse/README.md).
- **Alerts** – an Outlook or Teams activity on the failure path, or an Activator rule on `meta.run_log`.
- **Schedules and events** – a schedule per `load_group` (e.g. `telemetry` every 15 minutes), or a OneLake file-arrival trigger.

## Things to show after a run

- **Incremental extract.** On the day 2 run, open the Copy activity input in the Monitoring hub: `SELECT * FROM src.<table> WHERE last_modified > '…'`. Then query `meta.watermark` (layer `source`).
- **Rerun failed only.** Break the event object's date format as described in the main README (*A controlled failure*), run, fix it, then run with `rerun_failed_only = true`. Only that object is processed.
- **Traceability.** `meta.run_log.parent_run_id` is the pipeline Run ID shown in the Monitoring hub.
- **Isolation.** One failing object doesn't stop the other three.

## CI/CD with Azure DevOps

- Connect the workspace to an Azure DevOps repo. Notebooks, pipelines and `config/` live together.
- Pipeline IDs are environment-specific. Rebind them per stage with `fabric-cicd` `parameter.yml` or deployment pipeline rules; keep connection IDs in a variable library.
- After deploying to a stage, run `nb_mdf_00_setup_framework` (no reset) to load that stage's metadata.
