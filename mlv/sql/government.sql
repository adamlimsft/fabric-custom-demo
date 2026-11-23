-- Riverbend Regional Authority (Government): materialized lake views generated from config/government.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.ams_public_assets SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.ams_citizen_cases SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.iot_sensor_readings SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.crm_citizens SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- GV_CRM_CITIZENS: bronze.crm_citizens -> silver_mlv.citizen
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- hash salt read from meta.setting (never written into the view definition)
-- PII dropped before Silver: first_name, last_name
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.citizen
(
  CONSTRAINT gv_pty_01 CHECK (coalesce((`citizen_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Citizens - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'GV_CRM_CITIZENS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `citizen_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`District__c`) AS string) AS `district`,
    try_cast((`CitizenType__c`) AS string) AS `citizen_type`,
    try_cast((`AssetId__c`) AS int) AS `asset_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`ConcessionCardNo__c`) AS string) AS `concession_card_no_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.crm_citizens
),
ranked AS (
  SELECT `citizen_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `district`, `citizen_type`, `asset_id`, `status`, `concession_card_no_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `citizen_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`citizen_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`district`, r.`citizen_type`, r.`asset_id`, r.`status`, r.`concession_card_no_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.crm_citizens) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`citizen_id` IS NOT NULL), false) AS `_dq_ok_gv_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_gv_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_gv_pty_03`,
         coalesce((`citizen_type` IS NULL OR `citizen_type` IN ('Resident', 'Business', 'Concession holder', 'Visitor')), false) AS `_dq_ok_gv_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_pty_02` THEN 'GV-PTY-02' END, CASE WHEN NOT `_dq_ok_gv_pty_03` THEN 'GV-PTY-03' END, CASE WHEN NOT `_dq_ok_gv_pty_04` THEN 'GV-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_pty_01` THEN 'GV-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `citizen_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `district`,
  `citizen_type`,
  `asset_id`,
  `status`,
  CASE WHEN `concession_card_no_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`concession_card_no_hash` AS STRING)), 256) END AS `concession_card_no_hash`,
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

-- GV_AMS_PUBLIC_ASSETS: bronze.ams_public_assets -> silver_mlv.public_asset
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.public_asset
(
  CONSTRAINT gv_ent_01 CHECK (coalesce((`asset_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Public asset register. SCD2 keeps status, criticality and replacement_value_kaud history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'GV_AMS_PUBLIC_ASSETS')
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`asset_ref`) AS string) AS `asset_ref`,
    try_cast((`asset_name`) AS string) AS `asset_name`,
    try_cast((`asset_type`) AS string) AS `asset_type`,
    try_cast((`district`) AS string) AS `district`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`replacement_value_kaud`) AS double) AS `replacement_value_kaud`,
    try_cast((`constructed_date`) AS date) AS `constructed_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ams_public_assets
),
versions AS (
  SELECT `asset_id`, `asset_ref`, `asset_name`, `asset_type`, `district`, `status`, `criticality`, `replacement_value_kaud`, `constructed_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `asset_id`, `asset_ref`, `asset_name`, `asset_type`, `district`, `status`, `criticality`, `replacement_value_kaud`, `constructed_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`asset_ref` AS STRING), '~'), coalesce(CAST(`asset_name` AS STRING), '~'), coalesce(CAST(`asset_type` AS STRING), '~'), coalesce(CAST(`district` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`replacement_value_kaud` AS STRING), '~'), coalesce(CAST(`constructed_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `asset_id`, `asset_ref`, `asset_name`, `asset_type`, `district`, `status`, `criticality`, `replacement_value_kaud`, `constructed_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`asset_id` IS NOT NULL), false) AS `_dq_ok_gv_ent_01`,
         coalesce((`asset_type` IS NULL OR `asset_type` IN ('ROAD_SEGMENT', 'BRIDGE', 'PUBLIC_BUILDING', 'STORMWATER_PIT', 'STREETLIGHT')), false) AS `_dq_ok_gv_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_gv_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('IN_SERVICE', 'UNDER_REPAIR', 'CLOSED', 'DISPOSED')), false) AS `_dq_ok_gv_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_ent_02` THEN 'GV-ENT-02' END, CASE WHEN NOT `_dq_ok_gv_ent_03` THEN 'GV-ENT-03' END, CASE WHEN NOT `_dq_ok_gv_ent_04` THEN 'GV-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_ent_01` THEN 'GV-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `asset_ref`,
  `asset_name`,
  `asset_type`,
  `district`,
  `status`,
  `criticality`,
  `replacement_value_kaud`,
  `constructed_date`,
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

-- GV_AMS_CITIZEN_CASES: bronze.ams_citizen_cases -> silver_mlv.citizen_case
-- GV-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.citizen_case
(
  CONSTRAINT gv_evt_01 CHECK (coalesce((`case_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT gv_evt_03 CHECK (coalesce((completed_ts IS NULL OR completed_ts >= lodged_ts), false)) ON MISMATCH DROP,
  CONSTRAINT gv_evt_06 CHECK (coalesce((`lodged_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT gv_evt_02 CHECK (`_dq_ok_gv_evt_02`) ON MISMATCH DROP
)
COMMENT "Citizen cases - incremental by last_modified; can arrive before their public asset (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'GV_AMS_CITIZEN_CASES')
AS
WITH src AS (
  SELECT
    try_cast((`case_id`) AS int) AS `case_id`,
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`case_type`) AS string) AS `case_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`lodged_ts`) AS timestamp) AS `lodged_ts`,
    try_cast((`completed_ts`) AS timestamp) AS `completed_ts`,
    try_cast((`cost_aud`) AS decimal(12,2)) AS `cost_aud`,
    try_cast((`crew_hours`) AS double) AS `crew_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ams_citizen_cases
),
ranked AS (
  SELECT `case_id`, `asset_id`, `case_type`, `priority`, `status`, `lodged_ts`, `completed_ts`, `cost_aud`, `crew_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `case_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`case_id`, r.`asset_id`, r.`case_type`, r.`priority`, r.`status`, r.`lodged_ts`, r.`completed_ts`, r.`cost_aud`, r.`crew_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`case_id` IS NOT NULL), false) AS `_dq_ok_gv_evt_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_gv_evt_02`,
         coalesce((completed_ts IS NULL OR completed_ts >= lodged_ts), false) AS `_dq_ok_gv_evt_03`,
         coalesce((`cost_aud` IS NULL OR (`cost_aud` >= 0)), false) AS `_dq_ok_gv_evt_04`,
         coalesce((case_type <> 'FLOOD_DRAINAGE' OR priority = 1), false) AS `_dq_ok_gv_evt_05`,
         coalesce((`lodged_ts` IS NOT NULL), false) AS `_dq_ok_gv_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.public_asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_evt_04` THEN 'GV-EVT-04' END, CASE WHEN NOT `_dq_ok_gv_evt_05` THEN 'GV-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_evt_01` THEN 'GV-EVT-01' END, CASE WHEN NOT `_dq_ok_gv_evt_02` THEN 'GV-EVT-02' END, CASE WHEN NOT `_dq_ok_gv_evt_03` THEN 'GV-EVT-03' END, CASE WHEN NOT `_dq_ok_gv_evt_06` THEN 'GV-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `case_id`,
  `asset_id`,
  `case_type`,
  `priority`,
  `status`,
  `lodged_ts`,
  `completed_ts`,
  `cost_aud`,
  `crew_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_gv_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- GV_IOT_SENSOR_READINGS: bronze.iot_sensor_readings -> silver_mlv.sensor_reading
-- GV-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.sensor_reading
(
  CONSTRAINT gv_rdg_01 CHECK (coalesce((`water_level_cm` IS NULL OR (`water_level_cm` >= 0 AND `water_level_cm` <= 500)), false)) ON MISMATCH DROP,
  CONSTRAINT gv_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Smart infrastructure sensor readings every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'GV_IOT_SENSOR_READINGS')
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`water_level_cm`) AS double) AS `water_level_cm`,
    try_cast((`traffic_count`) AS double) AS `traffic_count`,
    try_cast((`pm25_ugm3`) AS double) AS `pm25_ugm3`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.iot_sensor_readings
),
ranked AS (
  SELECT `asset_id`, `reading_ts`, `water_level_cm`, `traffic_count`, `pm25_ugm3`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`asset_id`, r.`reading_ts`, r.`water_level_cm`, r.`traffic_count`, r.`pm25_ugm3`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`water_level_cm` IS NULL OR (`water_level_cm` >= 0 AND `water_level_cm` <= 500)), false) AS `_dq_ok_gv_rdg_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_gv_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_gv_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.public_asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_rdg_02` THEN 'GV-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_rdg_01` THEN 'GV-RDG-01' END, CASE WHEN NOT `_dq_ok_gv_rdg_03` THEN 'GV-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `reading_ts`,
  `water_level_cm`,
  `traffic_count`,
  `pm25_ugm3`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.citizen by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.citizen
COMMENT "Quarantine for GV_CRM_CITIZENS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `citizen_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`District__c`) AS string) AS `district`,
    try_cast((`CitizenType__c`) AS string) AS `citizen_type`,
    try_cast((`AssetId__c`) AS int) AS `asset_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`ConcessionCardNo__c`) AS string) AS `concession_card_no_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.crm_citizens
),
ranked AS (
  SELECT `citizen_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `district`, `citizen_type`, `asset_id`, `status`, `concession_card_no_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `citizen_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`citizen_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`district`, r.`citizen_type`, r.`asset_id`, r.`status`, r.`concession_card_no_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.crm_citizens) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`citizen_id` IS NOT NULL), false) AS `_dq_ok_gv_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_gv_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_gv_pty_03`,
         coalesce((`citizen_type` IS NULL OR `citizen_type` IN ('Resident', 'Business', 'Concession holder', 'Visitor')), false) AS `_dq_ok_gv_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_pty_02` THEN 'GV-PTY-02' END, CASE WHEN NOT `_dq_ok_gv_pty_03` THEN 'GV-PTY-03' END, CASE WHEN NOT `_dq_ok_gv_pty_04` THEN 'GV-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_pty_01` THEN 'GV-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `citizen_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `district`,
  `citizen_type`,
  `asset_id`,
  `status`,
  CASE WHEN `concession_card_no_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`concession_card_no_hash` AS STRING)), 256) END AS `concession_card_no_hash`,
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

-- rows removed from silver_mlv.citizen_case by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.citizen_case
COMMENT "Quarantine for GV_AMS_CITIZEN_CASES - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`case_id`) AS int) AS `case_id`,
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`case_type`) AS string) AS `case_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`lodged_ts`) AS timestamp) AS `lodged_ts`,
    try_cast((`completed_ts`) AS timestamp) AS `completed_ts`,
    try_cast((`cost_aud`) AS decimal(12,2)) AS `cost_aud`,
    try_cast((`crew_hours`) AS double) AS `crew_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ams_citizen_cases
),
ranked AS (
  SELECT `case_id`, `asset_id`, `case_type`, `priority`, `status`, `lodged_ts`, `completed_ts`, `cost_aud`, `crew_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `case_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`case_id`, r.`asset_id`, r.`case_type`, r.`priority`, r.`status`, r.`lodged_ts`, r.`completed_ts`, r.`cost_aud`, r.`crew_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`case_id` IS NOT NULL), false) AS `_dq_ok_gv_evt_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_gv_evt_02`,
         coalesce((completed_ts IS NULL OR completed_ts >= lodged_ts), false) AS `_dq_ok_gv_evt_03`,
         coalesce((`cost_aud` IS NULL OR (`cost_aud` >= 0)), false) AS `_dq_ok_gv_evt_04`,
         coalesce((case_type <> 'FLOOD_DRAINAGE' OR priority = 1), false) AS `_dq_ok_gv_evt_05`,
         coalesce((`lodged_ts` IS NOT NULL), false) AS `_dq_ok_gv_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.public_asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_evt_04` THEN 'GV-EVT-04' END, CASE WHEN NOT `_dq_ok_gv_evt_05` THEN 'GV-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_evt_01` THEN 'GV-EVT-01' END, CASE WHEN NOT `_dq_ok_gv_evt_02` THEN 'GV-EVT-02' END, CASE WHEN NOT `_dq_ok_gv_evt_03` THEN 'GV-EVT-03' END, CASE WHEN NOT `_dq_ok_gv_evt_06` THEN 'GV-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `case_id`,
  `asset_id`,
  `case_type`,
  `priority`,
  `status`,
  `lodged_ts`,
  `completed_ts`,
  `cost_aud`,
  `crew_hours`,
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

-- rows removed from silver_mlv.public_asset by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.public_asset
COMMENT "Quarantine for GV_AMS_PUBLIC_ASSETS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((`asset_ref`) AS string) AS `asset_ref`,
    try_cast((`asset_name`) AS string) AS `asset_name`,
    try_cast((`asset_type`) AS string) AS `asset_type`,
    try_cast((`district`) AS string) AS `district`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`replacement_value_kaud`) AS double) AS `replacement_value_kaud`,
    try_cast((`constructed_date`) AS date) AS `constructed_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ams_public_assets
),
versions AS (
  SELECT `asset_id`, `asset_ref`, `asset_name`, `asset_type`, `district`, `status`, `criticality`, `replacement_value_kaud`, `constructed_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `asset_id`, `asset_ref`, `asset_name`, `asset_type`, `district`, `status`, `criticality`, `replacement_value_kaud`, `constructed_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`asset_ref` AS STRING), '~'), coalesce(CAST(`asset_name` AS STRING), '~'), coalesce(CAST(`asset_type` AS STRING), '~'), coalesce(CAST(`district` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`replacement_value_kaud` AS STRING), '~'), coalesce(CAST(`constructed_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `asset_id`, `asset_ref`, `asset_name`, `asset_type`, `district`, `status`, `criticality`, `replacement_value_kaud`, `constructed_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `asset_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`asset_id` IS NOT NULL), false) AS `_dq_ok_gv_ent_01`,
         coalesce((`asset_type` IS NULL OR `asset_type` IN ('ROAD_SEGMENT', 'BRIDGE', 'PUBLIC_BUILDING', 'STORMWATER_PIT', 'STREETLIGHT')), false) AS `_dq_ok_gv_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_gv_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('IN_SERVICE', 'UNDER_REPAIR', 'CLOSED', 'DISPOSED')), false) AS `_dq_ok_gv_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_ent_02` THEN 'GV-ENT-02' END, CASE WHEN NOT `_dq_ok_gv_ent_03` THEN 'GV-ENT-03' END, CASE WHEN NOT `_dq_ok_gv_ent_04` THEN 'GV-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_ent_01` THEN 'GV-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `asset_ref`,
  `asset_name`,
  `asset_type`,
  `district`,
  `status`,
  `criticality`,
  `replacement_value_kaud`,
  `constructed_date`,
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

-- rows removed from silver_mlv.sensor_reading by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.sensor_reading
COMMENT "Quarantine for GV_IOT_SENSOR_READINGS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`asset_id`) AS int) AS `asset_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`water_level_cm`) AS double) AS `water_level_cm`,
    try_cast((`traffic_count`) AS double) AS `traffic_count`,
    try_cast((`pm25_ugm3`) AS double) AS `pm25_ugm3`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.iot_sensor_readings
),
ranked AS (
  SELECT `asset_id`, `reading_ts`, `water_level_cm`, `traffic_count`, `pm25_ugm3`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `asset_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`asset_id`, r.`reading_ts`, r.`water_level_cm`, r.`traffic_count`, r.`pm25_ugm3`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`water_level_cm` IS NULL OR (`water_level_cm` >= 0 AND `water_level_cm` <= 500)), false) AS `_dq_ok_gv_rdg_01`,
         (c.`asset_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_gv_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_gv_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `asset_id` AS _rk FROM silver_mlv.public_asset) ref1
    ON c.`asset_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_gv_rdg_02` THEN 'GV-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_gv_rdg_01` THEN 'GV-RDG-01' END, CASE WHEN NOT `_dq_ok_gv_rdg_03` THEN 'GV-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `asset_id`,
  `reading_ts`,
  `water_level_cm`,
  `traffic_count`,
  `pm25_ugm3`,
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

-- one row per rule that flagged or removed rows - feeds the operations dashboard
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.dq_rule_summary
COMMENT "Warnings and quarantined rows per data-quality rule (MLV path)"
AS
SELECT 'GV_AMS_PUBLIC_ASSETS' AS object_id, 'public_asset' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.public_asset LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'GV_AMS_PUBLIC_ASSETS' AS object_id, 'public_asset' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.public_asset LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'GV_AMS_CITIZEN_CASES' AS object_id, 'citizen_case' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.citizen_case LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'GV_AMS_CITIZEN_CASES' AS object_id, 'citizen_case' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.citizen_case LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'GV_IOT_SENSOR_READINGS' AS object_id, 'sensor_reading' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.sensor_reading LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'GV_IOT_SENSOR_READINGS' AS object_id, 'sensor_reading' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.sensor_reading LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'GV_CRM_CITIZENS' AS object_id, 'citizen' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.citizen LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'GV_CRM_CITIZENS' AS object_id, 'citizen' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.citizen LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- GV-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.citizen_summary
COMMENT "Citizens by district and citizen_type"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'GV-G06')
AS
SELECT district, citizen_type, COUNT(*) AS citizens, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_citizens, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT asset_id) AS linked_public_assets FROM silver_mlv.citizen WHERE _is_deleted = false GROUP BY district, citizen_type;

-- GV-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_public_asset
COMMENT "Current view of the public asset register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'GV-G02')
AS
SELECT asset_id, asset_ref, asset_name, asset_type, district, status, criticality, replacement_value_kaud, constructed_date, CAST(floor(months_between(current_date(), constructed_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.public_asset WHERE _is_current = true;

-- GV-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_citizen_case
COMMENT "Citizen cases with duration and public asset attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'GV-G03')
AS
SELECT v.case_id, v.asset_id, v.case_type, v.priority, v.status, v.lodged_ts, v.completed_ts, to_date(v.lodged_ts) AS opened_date, CAST(date_format(v.lodged_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.completed_ts) - unix_timestamp(v.lodged_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('LODGED', 'ASSIGNED') THEN 1 ELSE 0 END AS is_open, v.cost_aud, v.crew_hours, d.district, d.asset_type, d.criticality FROM silver_mlv.citizen_case v LEFT JOIN silver_mlv.public_asset d ON d.asset_id = v.asset_id AND d._is_current = true WHERE v._is_deleted = false;

-- GV-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_sensor_hourly
COMMENT "Hourly smart infrastructure sensor readings with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'GV-G04')
AS
SELECT asset_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(water_level_cm), 2) AS avg_water_level_cm, ROUND(MAX(water_level_cm), 2) AS max_water_level_cm, ROUND(AVG(traffic_count), 2) AS avg_traffic_count, ROUND(AVG(pm25_ugm3), 2) AS avg_pm25_ugm3, SUM(CASE WHEN water_level_cm > 150 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.sensor_reading GROUP BY asset_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- GV-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.public_asset_health_scorecard
COMMENT "Public asset risk score - citizen cases, urgent work and water level breaches, plus cost at risk in AUD (weights and dollar rates are illustrative: $2,500 per open case + $1,200 per 15-min window above the water-level threshold + replacement value (k) x criticality x $0.50)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'GV-G05')
AS
WITH asat AS (SELECT MAX(to_date(lodged_ts)) AS as_at FROM silver_mlv.citizen_case), ev AS (SELECT v.asset_id, COUNT(*) AS citizen_cases_12m, SUM(CASE WHEN v.status IN ('LODGED', 'ASSIGNED') THEN 1 ELSE 0 END) AS open_citizen_cases, SUM(CASE WHEN v.case_type = 'FLOOD_DRAINAGE' THEN 1 ELSE 0 END) AS urgent_citizen_cases_12m, ROUND(SUM(v.cost_aud), 2) AS cost_aud_12m FROM silver_mlv.citizen_case v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.lodged_ts) > add_months(asat.as_at, -12) GROUP BY v.asset_id), rd AS (SELECT asset_id, MAX(water_level_cm) AS max_water_level_cm, SUM(CASE WHEN water_level_cm > 150 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.sensor_reading GROUP BY asset_id), s AS (SELECT d.asset_id, d.asset_ref, d.asset_name, d.asset_type, d.district, d.status, d.criticality, COALESCE(ev.citizen_cases_12m, 0) AS citizen_cases_12m, COALESCE(ev.open_citizen_cases, 0) AS open_citizen_cases, COALESCE(ev.urgent_citizen_cases_12m, 0) AS urgent_citizen_cases_12m, COALESCE(ev.cost_aud_12m, 0) AS cost_aud_12m, rd.max_water_level_cm, COALESCE(rd.breach_readings, 0) AS breach_readings, d.replacement_value_kaud FROM silver_mlv.public_asset d LEFT JOIN ev ON ev.asset_id = d.asset_id LEFT JOIN rd ON rd.asset_id = d.asset_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_citizen_cases * 6 + s.urgent_citizen_cases_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_citizen_cases * 6 + s.urgent_citizen_cases_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_citizen_cases * 6 + s.urgent_citizen_cases_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_citizen_cases * 2500 + LEAST(s.breach_readings, 20) * 1200 + COALESCE(s.replacement_value_kaud, 0) * COALESCE(s.criticality, 1) * 0.5, 0) AS cost_at_risk_aud FROM s;
