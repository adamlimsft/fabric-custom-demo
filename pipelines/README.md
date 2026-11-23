# Pipelines – setting up the orchestrator

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

This folder has the two Data Factory pipelines that run the framework end to end. Both are generic: nothing in them is specific to a table or an industry. They read the control tables, so the same pipeline works for all nine packs, and onboarding a new source is new metadata rows, not new activities.

| File | What it does | Use it when |
|---|---|---|
| `pl_mdf_orchestrator.json` | Worklist → parallel copy and ingest → Silver notebook → Gold notebook | The standard demo. Start here |
| `pl_mdf_orchestrator_mlv.json` | Same front half, then one refresh of the materialized lake views builds Silver and Gold | You're showing the MLV path – see [mlv/README.md](../mlv/README.md) |

```
Get worklist ──► For each object (parallel, 4 at a time) ─────────────────► Bronze to Silver ──► Build Gold? ──► Silver to Gold
(nb_mdf_05)       ├─ If copy_db: Copy SQL query → Files/landing/…csv           (nb_mdf_20)          (run_gold)       (nb_mdf_30)
                  └─ Landing to Bronze (nb_mdf_10, one object per iteration)
```

## Before you start

- The main [README](../README.md) quick start is done: lakehouse `lh_mdf` exists, all 15 notebooks are imported with `lh_mdf` as their default lakehouse, and `nb_mdf_00_setup_framework` has run for your pack.
- I'd recommend running the flow once with the notebooks (`nb_mdf_90_run_all`) before you build the pipeline. If the notebooks work, the pipeline will too.
- You're a Contributor (or higher) on the workspace.

## Step 1 – choose your source option (2 min)

| Option | What the audience sees | Extra setup |
|---|---|---|
| **A. With a source database** (my recommendation for customers) | The pack's two operational tables are copied out of a SQL database with incremental watermark queries – the Fabric version of an SSIS metadata-driven extract | A Fabric SQL database, about 10 minutes |
| **B. Files only** | Same pipeline, but every source arrives as a landed file | None |

**Option A – set up the source database**

1. In the same workspace: **New item ▸ SQL database** ▸ name it `sqldb_source`. It stands in for the customer's on-prem system.
2. Open a new query, paste the whole of `sql/source_db/<pack>.sql` (e.g. `sql/source_db/fsi.sql`) and run it. It creates schema `src`, the source tables, three days of staged rows and a procedure that releases one day at a time.
3. Release day 1: `EXEC src.usp_release_day 1;`
4. Open `nb_mdf_01_generate_sample_data` and set `include_db_extracts = False`, `regenerate = True`, `release_through_day = 1`, then run it. This stops the notebook landing those two tables as files, because the pipeline will copy them from the database instead.
   *If you already ran `nb_mdf_01` with the defaults, first run `nb_mdf_00` with `reset_all = True`, then `nb_mdf_01` with `reset_landing = True`.*

**Option B – files only:** run `nb_mdf_00` and `nb_mdf_01` with their defaults. That's it.

## Step 2 – create the pipeline (5 min)

1. In the workspace: **New item ▸ Data pipeline** ▸ name it `pl_mdf_orchestrator`.
2. Open the JSON code view (the `{ }` button on the Home tab).
3. Replace the `activities` and `parameters` sections with the ones from `pl_mdf_orchestrator.json` in this folder, and select **OK**.
4. The designer now shows the activities, but they still point at placeholder IDs. Fix them in the designer:

   | Activity | What to set |
   |---|---|
   | **Get worklist** | Settings ▸ workspace and notebook `nb_mdf_05_get_worklist` |
   | **Landing to Bronze** (inside the ForEach) | notebook `nb_mdf_10_landing_to_bronze` |
   | **Copy source table to Landing** (inside ForEach ▸ If ▸ True) | **Source:** your `sqldb_source` connection, *Use query* = **Query**, query `@item().source_query`. **Destination:** lakehouse `lh_mdf`, root folder **Files**, folder path `@item().landing_folder`, file name `@item().landing_file`, format DelimitedText, first row as header |
   | **Bronze to Silver (priority order)** | notebook `nb_mdf_20_bronze_to_silver` |
   | **Silver to Gold** (inside Build Gold?) | notebook `nb_mdf_30_silver_to_gold` |

   For **option B**, open **Is database source ▸ True** and set **Copy source table to Landing** to *Deactivated* on its General tab.
5. Select **Validate**. It should come back clean. Then **Save**.

> **JSON view not available in your tenant?** Build it by hand – it's 7 activities and takes about 10 minutes. The full activity-by-activity table is in [docs/PIPELINE.md](../docs/PIPELINE.md#build-the-pipeline).

## Step 3 – parameters

| Parameter | Default | What it's for |
|---|---|---|
| `load_group` | `all` | Run one group only: `reference`, `transactional` or `telemetry` |
| `object_ids` | empty | Comma-separated object IDs to run only those objects |
| `rerun_failed_only` | `false` | Picks up only the objects that failed in the last run |
| `run_gold` | `true` | Set to `false` to stop after Silver (not in the MLV pipeline) |
| `run_date` | empty (= today, Sydney time) | Lets you simulate days. Each run lands its extracts in a `load_date=<run_date>` folder |

## Step 4 – run the three demo days

| Day | Option A (database) | Option B (files only) | Pipeline `run_date` |
|---|---|---|---|
| 1 | Already released in step 1 | `nb_mdf_01` with `release_through_day = 1` | `2026-11-02` (or empty for option B) |
| 2 | `EXEC src.usp_release_day 2;` then `nb_mdf_01` with `release_through_day = 2` | `nb_mdf_01` with `release_through_day = 2` | `2026-11-03` |
| 3 | `EXEC src.usp_release_day 3;` then `nb_mdf_01` with `release_through_day = 3` | `nb_mdf_01` with `release_through_day = 3` | `2026-11-04` |

After each run, open `nb_mdf_40_operations_monitor` to see the run status, data-quality scorecard, quarantine and watermarks.

> **Why `run_date` matters with option A:** Bronze only picks up load-date folders newer than its watermark. If you run all three days with the same date, days 2 and 3 land in the same folder and are skipped. In production, leave it empty.

## Step 5 (optional) – the MLV pipeline

Create a second pipeline `pl_mdf_orchestrator_mlv` from `pl_mdf_orchestrator_mlv.json` the same way. The only extra binding is **Refresh MLV lineage (Silver + Gold)** → notebook `nb_mdf_55_mlv_refresh`. Run `nb_mdf_50_mlv_build` once beforehand to create the views (see [mlv/README.md](../mlv/README.md)).

## What I show once it's running

- **Incremental extract:** on the day 2 run, open the Copy activity's input in the Monitoring hub – the query is `WHERE last_modified > '<watermark>'`. Then query `meta.watermark`.
- **Parallelism and isolation:** the ForEach runs four objects at a time, and one failing object doesn't stop the rest.
- **Rerun failed only:** break the event object's date format (see *A controlled failure* in the main README), run, fix it, then run again with `rerun_failed_only = true`. Only that object runs.
- **Traceability:** `meta.run_log.parent_run_id` matches the pipeline Run ID in the Monitoring hub.

## If something goes wrong

| Symptom | Likely cause and fix |
|---|---|
| Validate complains about a notebook | A Notebook activity still has a placeholder ID – reselect the notebook |
| Get worklist returns no items | `nb_mdf_00` hasn't run, or `load_group` / `object_ids` filter everything out |
| Copy fails to connect | The Copy source isn't bound to `sqldb_source`, or the query setting isn't **Query** |
| Copy succeeds but Bronze says SKIPPED | Same `run_date` as a previous run – use the next date (see step 4) |
| Option B fails at the Copy activity | You didn't deactivate **Copy source table to Landing** |
| Silver runs but Gold doesn't | `run_gold` is `false` |
| A notebook fails with "table not found" | That notebook's default lakehouse isn't `lh_mdf` |

## Taking it further

- Add a **Dataflow** activity for the reference-data dataflow ([dataflows/README.md](../dataflows/README.md)), or a **Stored procedure** activity after Silver to load the Warehouse (`etl.usp_run_all`, see [warehouse/README.md](../warehouse/README.md)).
- Add a Teams or Outlook activity on the failure path, or an Activator rule on `meta.run_log`.
- Schedule by load group – for example `telemetry` every 15 minutes and everything else nightly – or use a OneLake file-arrival trigger.
- For CI/CD, the notebook IDs are environment-specific. See the CI/CD section of [docs/PIPELINE.md](../docs/PIPELINE.md#cicd-with-azure-devops).
