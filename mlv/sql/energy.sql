-- Southern Grid Transmission (Energy & utilities): materialized lake views generated from config/energy.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.eam_assets SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.eam_work_orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.scada_transformer_telemetry SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.sfdc_landholder_contacts SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- EN_EAM_ASSETS: bronze.eam_assets -> silver_mlv.asset
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.asset
(
  CONSTRAINT en_ent_01 CHECK (coalesce((`asset_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Asset register. SCD2 keeps status, criticality and rating_mva history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'EN_EAM_ASSETS')
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`asset_tag`) AS string) AS `asset_tag`,
    try_cast((`asset_name`) AS string) AS `asset_name`,
    try_cast((`asset_class`) AS string) AS `asset_class`,
    try_cast((`region`) AS string) AS `region`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`rating_mva`) AS double) AS `rating_mva`,
    try_cast((`install_date`) AS date) AS `install_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.eam_assets
),
versions AS (
  SELECT `asset_id`, `asset_tag`, `asset_name`, `asset_class`, `region`, `status`, `criticality`, `rating_mva`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `asset_id`, `asset_tag`, `asset_name`, `asset_class`, `region`, `status`, `criticality`, `rating_mva`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`asset_tag` AS STRING), '~'), coalesce(CAST(`asset_name` AS STRING), '~'), coalesce(CAST(`asset_class` AS STRING), '~'), coalesce(CAST(`region` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`rating_mva` AS STRING), '~'), coalesce(CAST(`install_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `asset_id`, `asset_tag`, `asset_name`, `asset_class`, `region`, `status`, `criticality`, `rating_mva`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`asset_id` IS NOT NULL), false) AS `_dq_ok_en_ent_01`,
         coalesce((`asset_class` IS NULL OR `asset_class` IN ('SUBSTATION', 'TRANSFORMER', 'CIRCUIT_BREAKER', 'CAPACITOR_BANK', 'TRANSMISSION_LINE')), false) AS `_dq_ok_en_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_en_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('IN_SERVICE', 'STANDBY', 'OUT_OF_SERVICE', 'DECOMMISSIONED')), false) AS `_dq_ok_en_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_ent_02` THEN 'EN-ENT-02' END, CASE WHEN NOT `_dq_ok_en_ent_03` THEN 'EN-ENT-03' END, CASE WHEN NOT `_dq_ok_en_ent_04` THEN 'EN-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_ent_01` THEN 'EN-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `asset_tag`,
  `asset_name`,
  `asset_class`,
  `region`,
  `status`,
  `criticality`,
  `rating_mva`,
  `install_date`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_valid_from`, `_valid_to`, (`_valid_to` IS NULL) AS `_is_current`, `_is_deleted`,
  `_record_hash`
FROM chk;

-- EN_SFDC_LANDHOLDER_CONTACTS: bronze.sfdc_landholder_contacts -> silver_mlv.landholder
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- hash salt read from meta.setting (never written into the view definition)
-- PII dropped before Silver: first_name, last_name
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.landholder
(
  CONSTRAINT en_pty_01 CHECK (coalesce((`landholder_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Landholders - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'EN_SFDC_LANDHOLDER_CONTACTS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `landholder_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`Region__c`) AS string) AS `region`,
    try_cast((`LandholderType__c`) AS string) AS `landholder_type`,
    try_cast((`AssetId__c`) AS int) AS `asset_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`PropertyLotDp__c`) AS string) AS `property_lot_dp_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.sfdc_landholder_contacts
),
ranked AS (
  SELECT `landholder_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `region`, `landholder_type`, `asset_id`, `status`, `property_lot_dp_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `landholder_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`landholder_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`region`, r.`landholder_type`, r.`asset_id`, r.`status`, r.`property_lot_dp_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.sfdc_landholder_contacts) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`landholder_id` IS NOT NULL), false) AS `_dq_ok_en_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_en_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_en_pty_03`,
         coalesce((`landholder_type` IS NULL OR `landholder_type` IN ('Farm', 'Rural residential', 'Commercial', 'Crown land')), false) AS `_dq_ok_en_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_pty_02` THEN 'EN-PTY-02' END, CASE WHEN NOT `_dq_ok_en_pty_03` THEN 'EN-PTY-03' END, CASE WHEN NOT `_dq_ok_en_pty_04` THEN 'EN-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_pty_01` THEN 'EN-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `landholder_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `region`,
  `landholder_type`,
  `asset_id`,
  `status`,
  CASE WHEN `property_lot_dp_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`property_lot_dp_hash` AS STRING)), 256) END AS `property_lot_dp_hash`,
  `last_modified_ts`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk
CROSS JOIN (SELECT coalesce(max(`value`), 'demo-salt-change-me') AS _pii_salt
            FROM meta.setting WHERE `key` = 'pii_salt') _salt;

-- EN_SCADA_TRANSFORMER_TELEMETRY: bronze.scada_transformer_telemetry -> silver_mlv.tx_telemetry
-- EN-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.tx_telemetry
(
  CONSTRAINT en_rdg_01 CHECK (coalesce((`oil_temp_c` IS NULL OR (`oil_temp_c` >= -10 AND `oil_temp_c` <= 140)), false)) ON MISMATCH DROP,
  CONSTRAINT en_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Transformer telemetry every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'EN_SCADA_TRANSFORMER_TELEMETRY')
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`oil_temp_c`) AS double) AS `oil_temp_c`,
    try_cast((`load_mw`) AS double) AS `load_mw`,
    try_cast((`ambient_temp_c`) AS double) AS `ambient_temp_c`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.scada_transformer_telemetry
),
ranked AS (
  SELECT `asset_id`, `reading_ts`, `oil_temp_c`, `load_mw`, `ambient_temp_c`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`asset_id`, r.`reading_ts`, r.`oil_temp_c`, r.`load_mw`, r.`ambient_temp_c`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`oil_temp_c` IS NULL OR (`oil_temp_c` >= -10 AND `oil_temp_c` <= 140)), false) AS `_dq_ok_en_rdg_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_en_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_en_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_rdg_02` THEN 'EN-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_rdg_01` THEN 'EN-RDG-01' END, CASE WHEN NOT `_dq_ok_en_rdg_03` THEN 'EN-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `reading_ts`,
  `oil_temp_c`,
  `load_mw`,
  `ambient_temp_c`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- EN_EAM_WORK_ORDERS: bronze.eam_work_orders -> silver_mlv.work_order
-- EN-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.work_order
(
  CONSTRAINT en_evt_01 CHECK (coalesce((`wo_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT en_evt_03 CHECK (coalesce((completed_ts IS NULL OR completed_ts >= raised_ts), false)) ON MISMATCH DROP,
  CONSTRAINT en_evt_06 CHECK (coalesce((`raised_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT en_evt_02 CHECK (`_dq_ok_en_evt_02`) ON MISMATCH DROP
)
COMMENT "Work orders - incremental by last_modified; can arrive before their asset (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'EN_EAM_WORK_ORDERS')
AS
WITH src AS (
  SELECT
    try_cast((`wo_id`) AS int) AS `wo_id`,
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`wo_type`) AS string) AS `wo_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`raised_ts`) AS timestamp) AS `raised_ts`,
    try_cast((`completed_ts`) AS timestamp) AS `completed_ts`,
    try_cast((`cost_aud`) AS decimal(12,2)) AS `cost_aud`,
    try_cast((`labour_hours`) AS double) AS `labour_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.eam_work_orders
),
ranked AS (
  SELECT `wo_id`, `asset_id`, `wo_type`, `priority`, `status`, `raised_ts`, `completed_ts`, `cost_aud`, `labour_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `wo_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`wo_id`, r.`asset_id`, r.`wo_type`, r.`priority`, r.`status`, r.`raised_ts`, r.`completed_ts`, r.`cost_aud`, r.`labour_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`wo_id` IS NOT NULL), false) AS `_dq_ok_en_evt_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_en_evt_02`,
         coalesce((completed_ts IS NULL OR completed_ts >= raised_ts), false) AS `_dq_ok_en_evt_03`,
         coalesce((`cost_aud` IS NULL OR (`cost_aud` >= 0)), false) AS `_dq_ok_en_evt_04`,
         coalesce((wo_type <> 'EMERG' OR priority = 1), false) AS `_dq_ok_en_evt_05`,
         coalesce((`raised_ts` IS NOT NULL), false) AS `_dq_ok_en_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_evt_04` THEN 'EN-EVT-04' END, CASE WHEN NOT `_dq_ok_en_evt_05` THEN 'EN-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_evt_01` THEN 'EN-EVT-01' END, CASE WHEN NOT `_dq_ok_en_evt_02` THEN 'EN-EVT-02' END, CASE WHEN NOT `_dq_ok_en_evt_03` THEN 'EN-EVT-03' END, CASE WHEN NOT `_dq_ok_en_evt_06` THEN 'EN-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `wo_id`,
  `asset_id`,
  `wo_type`,
  `priority`,
  `status`,
  `raised_ts`,
  `completed_ts`,
  `cost_aud`,
  `labour_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_en_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.asset by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.asset
COMMENT "Quarantine for EN_EAM_ASSETS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`asset_tag`) AS string) AS `asset_tag`,
    try_cast((`asset_name`) AS string) AS `asset_name`,
    try_cast((`asset_class`) AS string) AS `asset_class`,
    try_cast((`region`) AS string) AS `region`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`rating_mva`) AS double) AS `rating_mva`,
    try_cast((`install_date`) AS date) AS `install_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.eam_assets
),
versions AS (
  SELECT `asset_id`, `asset_tag`, `asset_name`, `asset_class`, `region`, `status`, `criticality`, `rating_mva`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `asset_id`, `asset_tag`, `asset_name`, `asset_class`, `region`, `status`, `criticality`, `rating_mva`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`asset_tag` AS STRING), '~'), coalesce(CAST(`asset_name` AS STRING), '~'), coalesce(CAST(`asset_class` AS STRING), '~'), coalesce(CAST(`region` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`rating_mva` AS STRING), '~'), coalesce(CAST(`install_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `asset_id`, `asset_tag`, `asset_name`, `asset_class`, `region`, `status`, `criticality`, `rating_mva`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`asset_id` IS NOT NULL), false) AS `_dq_ok_en_ent_01`,
         coalesce((`asset_class` IS NULL OR `asset_class` IN ('SUBSTATION', 'TRANSFORMER', 'CIRCUIT_BREAKER', 'CAPACITOR_BANK', 'TRANSMISSION_LINE')), false) AS `_dq_ok_en_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_en_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('IN_SERVICE', 'STANDBY', 'OUT_OF_SERVICE', 'DECOMMISSIONED')), false) AS `_dq_ok_en_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_ent_02` THEN 'EN-ENT-02' END, CASE WHEN NOT `_dq_ok_en_ent_03` THEN 'EN-ENT-03' END, CASE WHEN NOT `_dq_ok_en_ent_04` THEN 'EN-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_ent_01` THEN 'EN-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `asset_tag`,
  `asset_name`,
  `asset_class`,
  `region`,
  `status`,
  `criticality`,
  `rating_mva`,
  `install_date`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_valid_from`, `_valid_to`, (`_valid_to` IS NULL) AS `_is_current`, `_is_deleted`,
  `_record_hash`,
  `_dq_failed` AS `_failed_rules`
FROM chk
WHERE size(`_dq_failed`) > 0;

-- rows removed from silver_mlv.work_order by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.work_order
COMMENT "Quarantine for EN_EAM_WORK_ORDERS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`wo_id`) AS int) AS `wo_id`,
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`wo_type`) AS string) AS `wo_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`raised_ts`) AS timestamp) AS `raised_ts`,
    try_cast((`completed_ts`) AS timestamp) AS `completed_ts`,
    try_cast((`cost_aud`) AS decimal(12,2)) AS `cost_aud`,
    try_cast((`labour_hours`) AS double) AS `labour_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.eam_work_orders
),
ranked AS (
  SELECT `wo_id`, `asset_id`, `wo_type`, `priority`, `status`, `raised_ts`, `completed_ts`, `cost_aud`, `labour_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `wo_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`wo_id`, r.`asset_id`, r.`wo_type`, r.`priority`, r.`status`, r.`raised_ts`, r.`completed_ts`, r.`cost_aud`, r.`labour_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`wo_id` IS NOT NULL), false) AS `_dq_ok_en_evt_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_en_evt_02`,
         coalesce((completed_ts IS NULL OR completed_ts >= raised_ts), false) AS `_dq_ok_en_evt_03`,
         coalesce((`cost_aud` IS NULL OR (`cost_aud` >= 0)), false) AS `_dq_ok_en_evt_04`,
         coalesce((wo_type <> 'EMERG' OR priority = 1), false) AS `_dq_ok_en_evt_05`,
         coalesce((`raised_ts` IS NOT NULL), false) AS `_dq_ok_en_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_evt_04` THEN 'EN-EVT-04' END, CASE WHEN NOT `_dq_ok_en_evt_05` THEN 'EN-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_evt_01` THEN 'EN-EVT-01' END, CASE WHEN NOT `_dq_ok_en_evt_02` THEN 'EN-EVT-02' END, CASE WHEN NOT `_dq_ok_en_evt_03` THEN 'EN-EVT-03' END, CASE WHEN NOT `_dq_ok_en_evt_06` THEN 'EN-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `wo_id`,
  `asset_id`,
  `wo_type`,
  `priority`,
  `status`,
  `raised_ts`,
  `completed_ts`,
  `cost_aud`,
  `labour_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`,
  `_dq_failed` AS `_failed_rules`
FROM chk
WHERE size(`_dq_failed`) > 0;

-- rows removed from silver_mlv.tx_telemetry by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.tx_telemetry
COMMENT "Quarantine for EN_SCADA_TRANSFORMER_TELEMETRY - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`oil_temp_c`) AS double) AS `oil_temp_c`,
    try_cast((`load_mw`) AS double) AS `load_mw`,
    try_cast((`ambient_temp_c`) AS double) AS `ambient_temp_c`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.scada_transformer_telemetry
),
ranked AS (
  SELECT `asset_id`, `reading_ts`, `oil_temp_c`, `load_mw`, `ambient_temp_c`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`asset_id`, r.`reading_ts`, r.`oil_temp_c`, r.`load_mw`, r.`ambient_temp_c`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`oil_temp_c` IS NULL OR (`oil_temp_c` >= -10 AND `oil_temp_c` <= 140)), false) AS `_dq_ok_en_rdg_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_en_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_en_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_rdg_02` THEN 'EN-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_rdg_01` THEN 'EN-RDG-01' END, CASE WHEN NOT `_dq_ok_en_rdg_03` THEN 'EN-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `reading_ts`,
  `oil_temp_c`,
  `load_mw`,
  `ambient_temp_c`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`,
  `_dq_failed` AS `_failed_rules`
FROM chk
WHERE size(`_dq_failed`) > 0;

-- rows removed from silver_mlv.landholder by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.landholder
COMMENT "Quarantine for EN_SFDC_LANDHOLDER_CONTACTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `landholder_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`Region__c`) AS string) AS `region`,
    try_cast((`LandholderType__c`) AS string) AS `landholder_type`,
    try_cast((`AssetId__c`) AS int) AS `asset_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`PropertyLotDp__c`) AS string) AS `property_lot_dp_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.sfdc_landholder_contacts
),
ranked AS (
  SELECT `landholder_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `region`, `landholder_type`, `asset_id`, `status`, `property_lot_dp_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `landholder_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`landholder_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`region`, r.`landholder_type`, r.`asset_id`, r.`status`, r.`property_lot_dp_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.sfdc_landholder_contacts) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`landholder_id` IS NOT NULL), false) AS `_dq_ok_en_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_en_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_en_pty_03`,
         coalesce((`landholder_type` IS NULL OR `landholder_type` IN ('Farm', 'Rural residential', 'Commercial', 'Crown land')), false) AS `_dq_ok_en_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_en_pty_02` THEN 'EN-PTY-02' END, CASE WHEN NOT `_dq_ok_en_pty_03` THEN 'EN-PTY-03' END, CASE WHEN NOT `_dq_ok_en_pty_04` THEN 'EN-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_en_pty_01` THEN 'EN-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `landholder_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `region`,
  `landholder_type`,
  `asset_id`,
  `status`,
  CASE WHEN `property_lot_dp_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`property_lot_dp_hash` AS STRING)), 256) END AS `property_lot_dp_hash`,
  `last_modified_ts`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`,
  `_dq_failed` AS `_failed_rules`
FROM chk
CROSS JOIN (SELECT coalesce(max(`value`), 'demo-salt-change-me') AS _pii_salt
            FROM meta.setting WHERE `key` = 'pii_salt') _salt
WHERE size(`_dq_failed`) > 0;

-- one row per rule that flagged or removed rows - feeds the operations dashboard
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.dq_rule_summary
COMMENT "Warnings and quarantined rows per data-quality rule (MLV path)"
AS
SELECT 'EN_EAM_ASSETS' AS object_id, 'asset' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.asset LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'EN_EAM_ASSETS' AS object_id, 'asset' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.asset LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'EN_EAM_WORK_ORDERS' AS object_id, 'work_order' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.work_order LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'EN_EAM_WORK_ORDERS' AS object_id, 'work_order' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.work_order LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'EN_SCADA_TRANSFORMER_TELEMETRY' AS object_id, 'tx_telemetry' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.tx_telemetry LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'EN_SCADA_TRANSFORMER_TELEMETRY' AS object_id, 'tx_telemetry' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.tx_telemetry LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'EN_SFDC_LANDHOLDER_CONTACTS' AS object_id, 'landholder' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.landholder LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'EN_SFDC_LANDHOLDER_CONTACTS' AS object_id, 'landholder' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.landholder LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- EN-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.asset_health_scorecard
COMMENT "Asset risk score - work orders, urgent work and oil temperature breaches, plus outage cost at risk in AUD (weights and dollar rates are illustrative: $6,000 per open work order + $2,500 per 15-min window above the oil-temperature threshold + rating (MVA) x criticality x $20)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'EN-G05')
AS
WITH asat AS (SELECT MAX(to_date(raised_ts)) AS as_at FROM silver_mlv.work_order), ev AS (SELECT v.asset_id, COUNT(*) AS work_orders_12m, SUM(CASE WHEN v.status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END) AS open_work_orders, SUM(CASE WHEN v.wo_type = 'EMERG' THEN 1 ELSE 0 END) AS urgent_work_orders_12m, ROUND(SUM(v.cost_aud), 2) AS cost_aud_12m FROM silver_mlv.work_order v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.raised_ts) > add_months(asat.as_at, -12) GROUP BY v.asset_id), rd AS (SELECT asset_id, MAX(oil_temp_c) AS max_oil_temp_c, SUM(CASE WHEN oil_temp_c > 95 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.tx_telemetry GROUP BY asset_id), s AS (SELECT d.asset_id, d.asset_tag, d.asset_name, d.asset_class, d.region, d.status, d.criticality, COALESCE(ev.work_orders_12m, 0) AS work_orders_12m, COALESCE(ev.open_work_orders, 0) AS open_work_orders, COALESCE(ev.urgent_work_orders_12m, 0) AS urgent_work_orders_12m, COALESCE(ev.cost_aud_12m, 0) AS cost_aud_12m, rd.max_oil_temp_c, COALESCE(rd.breach_readings, 0) AS breach_readings, d.rating_mva FROM silver_mlv.asset d LEFT JOIN ev ON ev.asset_id = d.asset_id LEFT JOIN rd ON rd.asset_id = d.asset_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_work_orders * 6 + s.urgent_work_orders_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_work_orders * 6 + s.urgent_work_orders_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_work_orders * 6 + s.urgent_work_orders_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_work_orders * 6000 + LEAST(s.breach_readings, 20) * 2500 + COALESCE(s.rating_mva, 0) * COALESCE(s.criticality, 1) * 20, 0) AS outage_cost_at_risk_aud FROM s;

-- EN-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_asset
COMMENT "Current view of the asset register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'EN-G02')
AS
SELECT asset_id, asset_tag, asset_name, asset_class, region, status, criticality, rating_mva, install_date, CAST(floor(months_between(current_date(), install_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.asset WHERE _is_current = true;

-- EN-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_tx_load_hourly
COMMENT "Hourly transformer telemetry with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'EN-G04')
AS
SELECT asset_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(oil_temp_c), 2) AS avg_oil_temp_c, ROUND(MAX(oil_temp_c), 2) AS max_oil_temp_c, ROUND(AVG(load_mw), 2) AS avg_load_mw, ROUND(AVG(ambient_temp_c), 2) AS avg_ambient_temp_c, SUM(CASE WHEN oil_temp_c > 95 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.tx_telemetry GROUP BY asset_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- EN-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_work_order
COMMENT "Work orders with duration and asset attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'EN-G03')
AS
SELECT v.wo_id, v.asset_id, v.wo_type, v.priority, v.status, v.raised_ts, v.completed_ts, to_date(v.raised_ts) AS opened_date, CAST(date_format(v.raised_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.completed_ts) - unix_timestamp(v.raised_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END AS is_open, v.cost_aud, v.labour_hours, d.region, d.asset_class, d.criticality FROM silver_mlv.work_order v LEFT JOIN silver_mlv.asset d ON d.asset_id = v.asset_id AND d._is_current = true WHERE v._is_deleted = false;

-- EN-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.landholder_summary
COMMENT "Landholders by region and landholder_type"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'EN-G06')
AS
SELECT region, landholder_type, COUNT(*) AS landholders, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_landholders, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT asset_id) AS linked_assets FROM silver_mlv.landholder WHERE _is_deleted = false GROUP BY region, landholder_type;
