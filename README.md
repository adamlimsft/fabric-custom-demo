# Fabric Medallion Demo Kit – nine industries, one framework

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

This is a working metadata-driven Medallion framework in Microsoft Fabric, with realistic and deliberately messy data, that you can switch between nine industries by changing one parameter.

It's the same pattern most SSIS/SQL teams already run – control tables that say what to load, how and where – rebuilt the Fabric way, plus the Fabric pieces people ask about straight after: materialized lake views, Warehouse, Real-Time Intelligence, Dataflow Gen2, Power BI, and Copilot / data agents / ontology.

All organisations, people, places and identifiers in the data are fictional and randomly generated.

## How to use this repo

1. **Get a copy.** Fork the repo, clone it, or use **Code ▸ Download ZIP** and unzip it on your laptop. You only need the files locally to import them into Fabric – nothing runs on your machine unless you want to change a pack.
2. **Pick a pack.** Read [Use_Case_Explanation.md](Use_Case_Explanation.md) and choose the industry that fits your audience.
3. **Follow the Quick start below.** It takes about 20 minutes to get the Medallion flow running in your own workspace. Every other area (pipelines, MLV, Warehouse, RTI, Dataflow Gen2, Power BI, AI) has its own README in its folder.
4. **Stay up to date.** If you forked it, use **Sync fork** to pull in fixes. If you downloaded the ZIP, check the [Releases](../../releases) page for the latest version.
5. **Found a problem or got an idea?** Open an issue – see [CONTRIBUTING.md](CONTRIBUTING.md). That way everyone using the kit sees the fix.

## Industries

| Pack            | Industry           | Fictional organisation       | Demo story                               |
| --------------- | ------------------ | ---------------------------- | ---------------------------------------- |
| `energy`        | Energy & utilities | Southern Grid Transmission   | Asset health and maintenance             |
| `telco`         | Telecommunications | Southern Cross Telecom       | Network service assurance                |
| `fsi`           | Financial services | Harbour Mutual Bank          | Fraud, credit risk and collections       |
| `manufacturing` | Manufacturing      | Kestrel Components           | Predictive maintenance and downtime cost |
| `healthcare`    | Healthcare         | Coastal Health Network       | Patient flow and ward pressure           |
| `ites`          | IT & ITeS          | Bluegum Digital Services     | Client SLAs and contract health          |
| `government`    | Government         | Riverbend Regional Authority | Infrastructure and citizen services      |
| `retail`        | Retail             | Wattle & Co                  | Stock availability and lost sales        |
| `digital`       | Digital natives    | Lyrebird Apps                | Platform reliability and customer health |

Each pack has 3 source systems – an on-prem database, a file drop and a SaaS CRM:

| Pack            | On-prem database             | File drop                        | SaaS CRM            |
| --------------- | ---------------------------- | -------------------------------- | ------------------- |
| `energy`        | Asset management DB          | SCADA exports                    | Salesforce          |
| `telco`         | Network inventory DB         | OSS counters                     | Dynamics 365        |
| `fsi`           | Core banking DB              | Payments monitoring files        | Salesforce FSC      |
| `manufacturing` | MES DB                       | Machine IoT historian            | Dynamics 365 Sales  |
| `healthcare`    | Patient admin DB             | Bed management feed              | Dynamics 365        |
| `ites`          | ITSM DB                      | Service-level monitoring         | HubSpot             |
| `government`    | Asset & works DB             | Smart infrastructure sensors     | Citizen case portal |
| `retail`        | Back-office and inventory DB | Store POS and stock-on-hand feed | Loyalty platform    |
| `digital`       | Product DB                   | Product telemetry                | HubSpot             |

Every pack has the same shape on purpose, so the notebooks, pipelines and docs work for all of them:

| Data object                                 | Source           | Format             | Load                    | Silver write mode  |
| ------------------------------------------- | ---------------- | ------------------ | ----------------------- | ------------------ |
| Entity (asset, store, ward, account…)       | On-prem database | SQL → CSV          | Incremental (watermark) | SCD2               |
| Event (work order, stock issue, admission…) | On-prem database | SQL → CSV          | Incremental             | SCD1               |
| Reading (telemetry, KPIs, occupancy…)       | File drop        | 15-min CSV         | Incremental             | Insert-only        |
| Party (customer, patient, citizen…)         | SaaS CRM API     | JSON               | Full snapshot           | SCD1 + soft delete |
| Extra feed (weather, promotions…)           | File drop        | Pipe-delimited TXT | –                       | –                  |

What each one is there to show:

- **Entity** – history, plus schema drift on day 3.
- **Event** – late-arriving rows: events for an entity that doesn't exist yet.
- **Reading** – sensor fault values, re-sent rows and a threshold breach (dates arrive as `dd/MM/yyyy`).
- **Party** – PII masked, hashed or dropped; duplicates from the API.
- **Extra feed** – lands every day but is ignored until you onboard it with metadata only.

**Every pack puts a dollar figure on the risk.** The Gold scorecard ranks each entity with a 0–100 risk score *and* an illustrative dollar impact – outage cost at risk (energy), revenue at risk (telco), expected loss (FSI), downtime cost at risk (manufacturing), access-block cost (healthcare), contract revenue at risk (IT services), cost at risk (government), lost sales at risk (retail) and ARR at risk (digital natives). The rates are made up and written into the pack's config (`impact`), so say "illustrative" when you show them. In my experience this is the number the business side of the room reacts to.

Not sure which pack fits your audience? [Use_Case_Explanation.md](Use_Case_Explanation.md) explains the business story behind each one.

Want a tenth industry, or one shaped around a specific customer? See [docs/ADD_INDUSTRY.md](docs/ADD_INDUSTRY.md) – it's one JSON file.

## What's in the repo

```
fabric-demo-kit/
├── README.md               ← start here
├── Use_Case_Explanation.md ← what each industry's demo story is about – read this before you pick a pack
├── CONTRIBUTING.md         ← how to report issues and suggest changes
├── LICENSE
├── notebooks/              ← 15 notebooks – import them all into one workspace
├── pipelines/              ← orchestration pipeline (notebook path and MLV path) + setup README
├── config/<pack>.json      ← the metadata for each industry (9 files)
├── sql/
│   ├── source_db/<pack>.sql   ← optional "on-prem" source database for the pipeline Copy demo
│   └── control_tables.sql     ← the control tables in T-SQL, if you'd rather keep metadata in a Fabric SQL database
├── mlv/                    ← materialized lake views: guide + generated SQL per pack
├── warehouse/              ← Warehouse in Gold: T-SQL ELT, SCD2, RLS, masking, clone, time travel – per pack
├── rti/                    ← Real-Time Intelligence: stream simulator + KQL per pack
├── dataflows/              ← Dataflow Gen2: business Excel → Silver, parameterised Gold – per pack
├── powerbi/                ← generator for a Power BI project (PBIP) for any pack
├── AI/<pack>.md            ← 15-minute Copilot + data agent + ontology build, one guide per industry
├── docs/                   ← pipeline setup, metadata reference, adding an industry
└── tools/                  ← the generators (Python, standard library) – only needed if you change a pack
```

## Architecture

```
 SOURCES (3 per pack)            LANDING                BRONZE                SILVER                          GOLD
                                 Files/landing/…        bronze.<sys>_<obj>    silver.<entity>                 gold.<table>
 On-prem DB ── Copy (watermark) ─┐
 File drop (15-min CSV, TXT) ────┤ raw files kept       all strings,          typed, renamed                  dims, facts,
 SaaS CRM API (JSON) ────────────┘ by load_date    ──►  append per day,  ──►  de-duplicated              ──►  scorecards
                                   (replayable)         audit columns,        DQ checked (technical + business) from SQL held
                                                        schema drift          PII de-identified               in metadata
                                                        absorbed              SCD1 / SCD2 / insert-only
                                                                              ──► quarantine.<entity>
                                 ▲                                               (late-arriving rows retried)
                                 └─────────────── all driven by the meta.* control tables ───────────────┘
                                   meta.run_log · meta.dq_result · meta.watermark → nb_mdf_40 monitoring
```

Design choices I'd ask any SI to explain about their own design:

| Decision      | What the kit does                                                            | Why                                                                                     |
| ------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Landing zone  | Raw files kept unchanged by `load_date`                                      | Replay any day without going back to the source                                         |
| Bronze        | All strings, appended per load date (`replaceWhere`)                         | A rerun replaces the day instead of duplicating it; new columns are absorbed and logged |
| Silver typing | `try_cast` + expressions held in metadata                                    | A bad value becomes NULL and a DQ rule catches it – one bad row doesn't fail the load   |
| Order of work | DQ runs before de-identification, and both happen before anything is written | Rules see raw values; raw PII never lands in Silver or quarantine                       |
| Quarantine    | Failed rows kept with the IDs of the rules they broke                        | Nothing is silently dropped; late-arriving rows are re-checked and merged automatically |
| Gold          | SQL held in metadata, run in order                                           | Business owners can review it; a new Gold table is a new row                            |
| Operations    | Append-only run log, DQ results and watermarks                               | Safe in parallel; "rerun failed only", SLA checks, audit                                |

## Quick start (about 20 minutes)

**You need:** a Fabric workspace on any capacity (F2 or a trial is fine for this part; Copilot and agents need F2+ paid), and Contributor or higher on the workspace.

1. **Lakehouse.** New item ▸ Lakehouse ▸ name it **`lh_mdf`** and tick **Lakehouse schemas**. (The framework creates `meta`, `bronze`, `silver`, `gold` and `quarantine`.)
2. **Notebooks.** Download or clone this repo to your laptop. In the workspace: Import ▸ Notebook ▸ From this computer ▸ select all 15 files in `notebooks/`. Keep the names – the notebooks call each other by name. Open each one and add `lh_mdf` as its **default lakehouse** (or set up an Environment as the workspace default to save clicks).
3. **Load the metadata.** Open `nb_mdf_00_setup_framework`, set `industry` to your pack (e.g. `"retail"`) and Run all.
4. **Generate the data, release day 1.** Open `nb_mdf_01_generate_sample_data` and Run all (defaults: `release_through_day = 1`). The data is generated inside Fabric – no upload.
5. **Run the Medallion flow.** Open `nb_mdf_90_run_all` and Run all: Bronze → Silver → Gold.
6. **Look at what happened.** Run `nb_mdf_40_operations_monitor`: run status, DQ scorecard, quarantine, watermarks.
7. **Days 2 and 3.** Set `release_through_day = 2` in `nb_mdf_01`, run it, run `nb_mdf_90_run_all`. Then the same for day 3.

> **Release one day at a time.** The incremental, late-arriving and SCD2 stories only show up when days are processed separately.

**Switching industry:** I use a separate workspace (or at least a separate lakehouse) per pack. To reuse the same one, run `nb_mdf_00` with `reset_all = True` and the new `industry`, then `nb_mdf_01` with `reset_landing = True`.

### The notebooks

| Notebook                               | What it does                                                                            |
| -------------------------------------- | --------------------------------------------------------------------------------------- |
| `nb_mdf_utils`                         | Shared library, loaded by the others with `%run`. Don't run it on its own               |
| `nb_mdf_00_setup_framework`            | Schemas + control tables for the chosen pack (config is embedded – nothing to upload)   |
| `nb_mdf_01_generate_sample_data`       | Generates 3 days of data and releases them into Landing one day at a time               |
| `nb_mdf_05_get_worklist`               | Worklist + incremental source queries for the pipeline                                  |
| `nb_mdf_06_onboard_new_source`         | Onboards the extra feed with metadata rows only                                         |
| `nb_mdf_10_landing_to_bronze`          | Generic ingestion – csv, txt, json, xlsx                                                |
| `nb_mdf_20_bronze_to_silver`           | Generic typing, de-dup, DQ, PII, SCD1/SCD2/insert-only, quarantine, late-arriving retry |
| `nb_mdf_30_silver_to_gold`             | Gold tables from SQL held in metadata                                                   |
| `nb_mdf_40_operations_monitor`         | Status, failures and reruns, SLA, DQ scorecard, quarantine, watermarks, SCD2 history    |
| `nb_mdf_90_run_all`                    | Bronze → Silver → Gold as one notebook DAG (no pipeline needed)                         |
| `nb_mdf_mlv_sqlgen`, `nb_mdf_50/55/56` | Materialized lake view path – see [mlv/README.md](mlv/README.md)                        |
| `nb_rti_01_stream_simulator`           | Streams events into an Eventstream – see [rti/README.md](rti/README.md)                 |

## Walking through days 1, 2 and 3

The names below are for the pack you loaded – `meta.ingestion_object` shows them. `<entity>` is e.g. `asset` / `store` / `ward`, `<event>` is e.g. `work_order` / `stock_issue` / `admission`. `Files/sample_source/<pack>/dq_injection_manifest.json` lists every planted issue for your pack.

| Day       | What's planted                                                                                                                                                                                                     | What the framework does                                                                           |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| 1         | Unknown types and out-of-range tiers on the entity; events closed before they were opened; negative amounts; urgent events not marked priority 1; sensor fault values (999.9); bad emails and postcodes in the CRM | Technical vs business rules; warn vs quarantine; DQ scorecard                                     |
| 2         | ~25 entity changes; **events for entity 199999, which the source hasn't published yet**; API returns old and new versions of CRM records; re-sent sensor readings; one frozen sensor                               | SCD2 history; late-arriving rows quarantined (retry-eligible); de-duplication; idempotent inserts |
| 3         | **A new column** on the entity extract (schema drift); entity 199999 arrives; 2 CRM records missing from the full snapshot; one entity breaches its threshold in the afternoon                                     | Drift logged; quarantined rows **resolved automatically**; soft deletes; risk score goes up       |
| Every day | The extra feed lands                                                                                                                                                                                               | Ignored until `nb_mdf_06` onboards it                                                             |

Queries I run in a `%%sql` cell (swap in your pack's table names):

```sql
-- The metadata is the product
SELECT object_id, source_system, ingest_method, landing_format, load_type, silver_write_mode, priority, data_owner
FROM meta.ingestion_object ORDER BY priority;
SELECT rule_category, rule_id, object_id, rule_name, check_type, severity, rule_owner FROM meta.dq_rule ORDER BY rule_category;
SELECT object_id, target_column, pii_treatment, sensitivity FROM meta.column_map WHERE pii_treatment <> 'none';

-- Day 1: quarantine says which rule each row broke
SELECT _failed_rules, COUNT(*) FROM quarantine.<event> GROUP BY _failed_rules;

-- Day 2: SCD2 history and late-arriving rows
SELECT * FROM silver.<entity> WHERE _is_current = false LIMIT 20;
SELECT _quarantine_status, _retry_eligible, COUNT(*) FROM quarantine.<event> GROUP BY ALL;

-- Day 3: drift and self-healing
SELECT layer, object_id, message FROM meta.run_log WHERE message LIKE '%DRIFT%' OR message LIKE '%LATE-ARRIVING%';
SELECT * FROM silver.<party> WHERE _is_deleted = true;
DESCRIBE HISTORY silver.<entity>;

-- Gold
SELECT * FROM gold.<entity>_health_scorecard ORDER BY risk_score DESC LIMIT 20;
-- the last column is the dollar figure for your pack (e.g. expected_loss_aud, lost_sales_at_risk_aud)
```

**Onboard a source live (my favourite moment):** run `nb_mdf_06_onboard_new_source`. It inserts the metadata rows, runs the *existing* Bronze and Silver notebooks for the new object, and builds a new Gold table that joins the feed to your events (e.g. weather vs work orders, promotions vs out-of-stocks). No new pipeline, no new notebook.

**A controlled failure:** simulate a source changing its date format. Find your event object and its "opened" column in `meta.column_map`, then:

```sql
UPDATE meta.column_map SET transform_expr = "to_timestamp(<opened column>, 'dd/MM/yyyy HH:mm')"
WHERE object_id = '<event object_id>' AND target_column = '<opened column>';
```

Release the next day and run. Every timestamp now fails to parse, so the `fail` rule `<PREFIX>-EVT-06` (5% threshold) stops that object **before** bad data reaches Silver, while every other object succeeds. `nb_mdf_40` shows the failure and the rerun command. Set `transform_expr = NULL` again and rerun only that object (`nb_mdf_20` with `object_ids = '<event object_id>'`, or the pipeline with `rerun_failed_only = true`). The Silver watermark didn't move, so nothing is lost.

## The modules

| Module                  | Folder                               | Time to set up | What it shows                                                                                                |
| ----------------------- | ------------------------------------ | -------------- | ------------------------------------------------------------------------------------------------------------ |
| Pipelines               | [docs/PIPELINE.md](docs/PIPELINE.md) | 15 min         | Worklist from control tables, parallel ForEach, incremental Copy from SQL with watermarks, rerun-failed-only |
| Materialized lake views | [mlv/](mlv/README.md)                | 10 min         | Silver and Gold generated as declarative SQL from the same metadata; lineage; parallel-run reconciliation    |
| Warehouse in Gold       | [warehouse/](warehouse/README.md)    | 15 min         | Star schema with stored-procedure ELT, SCD2, late-arriving keys, RLS, masking, CLS, clone, time travel       |
| Real-Time Intelligence  | [rti/](rti/README.md)                | 30 min         | Eventstream → Eventhouse, update policies, materialized views, KQL detections, dashboard, Activator          |
| Dataflow Gen2           | [dataflows/](dataflows/README.md)    | 15 min         | Business-owned Excel → Silver with no SharePoint needed; parameterised Gold called from a pipeline           |
| Power BI                | [powerbi/](powerbi/README.md)        | 10 min         | PBIP project (semantic model + report) generated for any pack                                                |
| AI                      | [AI/](AI/)                           | 15 min         | Semantic model prepped for AI, a Fabric data agent, an ontology, and prompts that work on the data           |

## Reset and troubleshooting

**Start again from day 1:** `nb_mdf_00` with `reset_all = True`, then `nb_mdf_01` with `reset_landing = True` and `release_through_day = 1`.

| Symptom                               | Fix                                                                                       |
| ------------------------------------- | ----------------------------------------------------------------------------------------- |
| `Could not create schemas`            | The lakehouse was created without **Lakehouse schemas**. Create a new one with it ticked  |
| `%run nb_mdf_utils` not found         | Import `nb_mdf_utils` into the same workspace and keep its name                           |
| A notebook can't find tables or files | Attach `lh_mdf` as its default lakehouse                                                  |
| `No pack loaded – run nb_mdf_00`      | Run `nb_mdf_00_setup_framework` first                                                     |
| Late-arriving / SCD2 didn't show up   | Several days were released before the first run. Reset and go one day at a time           |
| Everything says `SKIPPED`             | No new `load_date` folders. Release the next day, or use `reprocess_dates` in `nb_mdf_10` |

## Changing things

- **Tweak a pack** (names, value lists, prompts): edit the `pack` section of `config/<pack>.json`, then run `python tools/build_kit.py` and `python tools/local_check.py <pack>`. The first regenerates everything that depends on it (including the copies embedded in the notebooks); the second runs three days through the framework logic and every Gold query and should end with `ALL OK`. Python 3, nothing to install.
- **Quick change without re-importing notebooks:** upload `config/<pack>.json` to `lh_mdf` under `Files/config/`. `nb_mdf_00` uses it instead of the embedded copy.

## Before you present

The Fabric UI and some previews (MLV, ontology, data agent) change often, so do a dry run in your own tenant before you present, and expect to fix the odd small thing on the first run. If you hit something, let me know and I'm happy to help.

– Adam
