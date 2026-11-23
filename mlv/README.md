# Materialized lake views – the same metadata, declarative

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

The kit can build Silver and Gold two ways from one set of control tables:

| | Notebook path (`nb_mdf_20`, `nb_mdf_30`) | MLV path (`nb_mdf_50`, `nb_mdf_55`) |
|---|---|---|
| How Silver is built | PySpark MERGE, watermarks, quarantine writes | `CREATE MATERIALIZED LAKE VIEW` statements generated from `meta.*` |
| Run order | The pipeline or the `nb_mdf_90` DAG | Fabric works it out from the lineage |
| Incremental | Hand-built watermarks | Optimal refresh: skip, incremental or full, chosen by Fabric (needs Change Data Feed) |
| Data quality | `meta.dq_rule` → flags, quarantine table, `meta.dq_result` | Same rules → `CONSTRAINT … CHECK … ON MISMATCH DROP / FAIL`, `_dq_warnings`, `quarantine_mlv.*` |
| SCD2 | MERGE that closes the current row | Rebuilt from Bronze history with `LAG`/`LEAD` (full refresh by design) |
| Late-arriving data | Explicit retry of quarantined rows | Automatic – the next refresh re-evaluates the join |
| Best for | Complex logic, Python, very large incremental MERGE | SQL-friendly cleansing, conforming and aggregation |

What I say in the room: *pipelines move the data in, MLVs keep the layers current, notebooks handle the exceptions.* Ingestion (`nb_mdf_05`, `nb_mdf_10`, the Copy activity) is the same for both paths.

## What gets generated

| Metadata | Generated SQL |
|---|---|
| `column_map` rename / type / `transform_expr` | `try_cast((expr) AS type) AS target` |
| `primary_keys`, `dedupe_order_by` | `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)`, newest wins |
| `load_type = full` | keys missing from the latest snapshot kept with `_is_deleted = true` |
| `silver_write_mode = merge_scd2` | versions from Bronze with `_valid_from`, `_valid_to`, `_is_current` |
| `pii_treatment` | applied in the final SELECT; the hash salt is read from `meta.setting`, never written into a view |
| rule severity `quarantine` | `CONSTRAINT <rule> CHECK (…) ON MISMATCH DROP`, plus the dropped rows in `quarantine_mlv.<entity>` |
| rule severity `fail`, threshold 0 | `ON MISMATCH FAIL` – the refresh stops |
| rule severity `fail`, threshold > 0 | DROP + quarantine (a percentage can't go in a constraint – watch `dq_rule_summary`) |
| rule severity `warn` | `_dq_warnings` array, row kept |
| `gold_transform.sql_text` | `gold_mlv.<table>` – the same SQL pointed at `silver_mlv` |

`dim_date` has no source table, so it stays notebook-built (`gold.dim_date`).

## Run it (about 10 minutes after the main setup)

1. Do the main setup and process at least day 1.
2. Make sure `nb_mdf_mlv_sqlgen`, `nb_mdf_50_mlv_build`, `nb_mdf_55_mlv_refresh` and `nb_mdf_56_compare_paths` are imported with `lh_mdf` attached.
3. Run `nb_mdf_50_mlv_build`. It turns on Change Data Feed on Bronze, creates `silver_mlv`, `quarantine_mlv` and `gold_mlv`, creates every view upstream-first and saves each statement in `meta.mlv_definition`. Use `dry_run = True` first if you want to read the SQL before anything is created.
4. Open the lakehouse ▸ **Materialized lake views ▸ Manage** to see the lineage. Schedule it there, or
5. run `nb_mdf_55_mlv_refresh`, or the `pl_mdf_orchestrator_mlv` pipeline.
6. Release the next day, ingest Bronze, refresh again, and watch it choose skip / incremental / full per view.
7. Run `nb_mdf_56_compare_paths` to reconcile the notebook path and the MLV path table by table.

## Read the SQL without Fabric

`mlv/sql/<pack>.sql` is a preview of what `nb_mdf_50` generates for each pack (14 views). You can paste it into a Spark SQL cell instead of running the notebook, but the notebook is better because it checks your real Bronze columns. `tools/build_kit.py` regenerates these files.

## Demo moments

- **Generated SQL next to the metadata.** Open the event view in `silver_mlv` – each `CONSTRAINT` name is a `rule_id`. Change a rule in `meta.dq_rule`, re-run `nb_mdf_50` with `dry_run = True`, and the SQL changes.
- **Lineage for nothing.** Gold depends on Silver, and the event view depends on the entity view through the referential rule. Nobody wrote orchestration.
- **Late-arriving data with no code.** On day 2 the events for entity 199999 sit in `quarantine_mlv`. Release day 3, refresh, and they move into Silver.
- **Parallel run.** `nb_mdf_56` is *migrate → validate → run alongside → cut over*, inside Fabric.

## Differences worth discussing

- Rules are checked on the newest version of a key. The notebook path keeps the last good version when an update is quarantined; the MLV drops the key until the source is fixed.
- Constraints are row-level and yes/no – no percentage thresholds and no per-run DQ history.
- De-duplication and SCD2 use window functions, so those views full-refresh. For big, high-churn tables keep the notebook MERGE.
- No UDFs, time travel or DML inside a view definition.
- The pipeline activity *Refresh Materialized Lake View* refreshes every MLV in the lakehouse; the kit uses the REST job through `nb_mdf_55` so it can wait for the result.

MLVs are moving quickly – check the current docs: [overview](https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view), [refresh](https://learn.microsoft.com/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view).
