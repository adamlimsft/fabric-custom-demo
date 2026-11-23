# Metadata reference – the control tables

The source of truth is `config/<pack>.json`. Each file has a `pack` section (the industry description) and one section per control table. `tools/build_kit.py` writes the control-table sections from the `pack` section, and `nb_mdf_00_setup_framework` loads one pack into `meta.*`. In a real project, keep the JSON in Azure DevOps and change it through pull requests; don't hand-edit `meta.*` in production.

## meta.source_system (section `source_system`)
| Column | Meaning |
|---|---|
| source_system | Short code; also the Landing folder name (`landing/<source_system>/…`) |
| display_name, system_type | What the system is |
| connection_pattern | How data leaves the system today |
| technical_owner | Who to call when the source breaks |
| fabric_options | Fabric choices for this source (Copy job, pipeline, mirroring, shortcut, Dataflow Gen2, Eventstream) – discussion prompt |

## meta.ingestion_object (section `ingestion_object`) – one row per source object
| Column | Values / example | Used by |
|---|---|---|
| object_id | `EN_EAM_ASSETS` | everything |
| source_system, source_object | `eam`, `assets` | Landing path, file names |
| source_type | database \| file \| api \| sharepoint | documentation |
| ingest_method | `copy_db` (pipeline copies from a database) \| `landed_file` (file/API already landed). Production adds `shortcut`, `mirror` | worklist, pipeline |
| landing_format | csv \| txt \| json \| xlsx | Bronze reader |
| landing_path | `landing/eam/assets` (under `Files/`) | Bronze reader, pipeline sink |
| read_options (JSON) | csv/txt: `header`, `delimiter`, `quote`, `encoding` · json: `multiline`, `json_root` (e.g. `d.results`, `records`, `result`), `explode` + `keep_parent` · xlsx: `sheet_name`, `header_row` | Bronze reader |
| source_table | `src.assets` | worklist source query |
| load_type | full \| incremental | full → Silver uses the latest snapshot only and soft-deletes missing rows |
| watermark_column | source column for incremental database queries (`last_modified`) | worklist / Bronze |
| primary_keys | `asset_id` or `asset_id,reading_ts` (Silver names) | de-dup, MERGE |
| dedupe_order_by | `last_modified DESC, _ingested_at DESC` | de-dup; first column also = SCD2 effective date |
| bronze_table / silver_table | `eam_assets` / `asset` | targets |
| silver_write_mode | `merge_scd1` \| `merge_scd2` \| `insert_only` | Silver writer |
| load_group | reference \| transactional \| telemetry | batching / scheduling |
| priority | lower runs first (reference before transactions) | Silver order |
| sla_hours | freshness target | `nb_mdf_40` SLA check |
| data_owner, sensitivity | accountability + classification | governance / Purview |
| active | switch an object off without deleting it | everything |
| fabric_alternative | e.g. "Mirroring candidate" | discussion prompt |

## meta.column_map (section `column_map`) – Bronze → Silver contract
| Column | Meaning |
|---|---|
| source_column | Bronze column (headers made Delta-safe: `Clearance (m)` becomes `Clearance_m`) |
| target_column | Silver column name |
| data_type | Spark type – applied with `try_cast` (bad value → NULL → caught by DQ) |
| transform_expr | Optional Spark SQL over **Bronze** columns, e.g. `to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')`. One source column may feed several targets (derived columns) |
| pii_treatment | `none` \| `hash` (salted SHA-256, joinable) \| `mask_email` \| `mask_phone` \| `year_only` \| `redact` \| `drop` |
| sensitivity | Public \| Internal \| Confidential \| Restricted – maps to sensitivity labels / OneLake security roles |
| description | business meaning – push to Purview / OneLake catalog descriptions |

## meta.dq_rule (section `dq_rule`)
| Column | Meaning |
|---|---|
| rule_category | `technical` (engineering-owned) \| `business` (data-owner-defined) |
| check_type | `not_null`, `regex` {pattern}, `range` {min,max}, `allowed_values` {values}, `unique` (column_name = comma list), `referential` {ref_table, ref_column}, `expression` {expr = Spark SQL predicate that must be TRUE} |
| column_name | Silver column(s); rules run **before** PII treatment so they see raw values |
| severity | `warn` – keep row, list rule in `_dq_warnings` · `quarantine` – divert to `quarantine.<entity>` · `fail` – stop the object if failures exceed `fail_threshold_pct` |
| rule_owner | team accountable (Purview data steward) |

Referential rules with `quarantine` severity make rows **retry-eligible**. If the only failures were referential, each later run re-checks the row and merges it once the parent appears.

## meta.gold_transform (section `gold_transform`)
`sql_text` is a Spark SQL `SELECT` over `silver.*` (and earlier `gold.*` tables). It runs in `run_order` and fully rebuilds `gold.<gold_table>`. `business_owner` and `consumers` document accountability.

## Operational tables
| Table | Grain | Notes |
|---|---|---|
| meta.watermark | append-only row per object/layer/run | layers: `source` (database high-water mark), `bronze` (last load_date), `silver` (last `_ingested_at`). Latest row wins – no update contention under parallel runs |
| meta.run_log | object × layer × run | `parent_run_id` = pipeline RunId or orchestrator id |
| meta.dq_result | rule × run | outcome PASS / WARN / QUARANTINED / FAILED |
| meta.setting | key/value | `industry`, `pii_salt` (demo only – use Key Vault), `pack_spec` (the pack definition, used by nb_mdf_01/06/40 and the RTI notebook), `onboard_spec` (the feed nb_mdf_06 onboards). The MLV path reads the hash salt from here |
| meta.mlv_definition | one row per generated materialized lake view | written by `nb_mdf_50`: layer, name, dependencies, notes, full DDL, CREATED / FAILED + error |
| meta.parallel_run_check | entity × comparison run | written by `nb_mdf_56`: notebook vs MLV row counts, rows only on each side, MATCH / DIFF |

## Columns the framework adds
| Layer | Columns |
|---|---|
| Bronze | `_source_system, _object_id, _load_date, _ingested_at, _batch_id, _source_file` |
| Silver (all) | audit columns + `_dq_warnings` (array), `_record_hash`, `_silver_updated_at` |
| Silver SCD1 | `_is_deleted` |
| Silver SCD2 | `_valid_from, _valid_to, _is_current` |
| Quarantine | `_quarantine_id, _failed_rules, _failed_rule_types, _retry_eligible, _quarantine_status (OPEN/RESOLVED), _quarantined_at, _quarantine_run_id, _resolved_at, _resolved_run_id` |

## Onboarding section (`onboard`)
Not loaded into `meta.*` by `nb_mdf_00`. It describes the extra feed every pack lands from day 1 (pipe-delimited text,
`dd/MM/yyyy` dates). `nb_mdf_06_onboard_new_source` inserts its `object`, `columns`, `rules` and `gold` rows to show that
a new source is metadata, not code.
