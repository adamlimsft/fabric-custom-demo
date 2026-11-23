-- Coastal Health Network (Healthcare): materialized lake views generated from config/healthcare.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.pas_wards SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.pas_admissions SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.bms_ward_occupancy SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.crm_patients SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- HC_PAS_WARDS: bronze.pas_wards -> silver_mlv.ward
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.ward
(
  CONSTRAINT hc_ent_01 CHECK (coalesce((`ward_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Ward register. SCD2 keeps status, acuity_level and bed_count history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'HC_PAS_WARDS')
AS
WITH src AS (
  SELECT
    try_cast((`ward_id`) AS int) AS `ward_id`,
    try_cast((`ward_code`) AS string) AS `ward_code`,
    try_cast((`ward_name`) AS string) AS `ward_name`,
    try_cast((`ward_type`) AS string) AS `ward_type`,
    try_cast((`hospital`) AS string) AS `hospital`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`acuity_level`) AS int) AS `acuity_level`,
    try_cast((`bed_count`) AS double) AS `bed_count`,
    try_cast((`opened_date`) AS date) AS `opened_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pas_wards
),
versions AS (
  SELECT `ward_id`, `ward_code`, `ward_name`, `ward_type`, `hospital`, `status`, `acuity_level`, `bed_count`, `opened_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `ward_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `ward_id`, `ward_code`, `ward_name`, `ward_type`, `hospital`, `status`, `acuity_level`, `bed_count`, `opened_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`ward_code` AS STRING), '~'), coalesce(CAST(`ward_name` AS STRING), '~'), coalesce(CAST(`ward_type` AS STRING), '~'), coalesce(CAST(`hospital` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`acuity_level` AS STRING), '~'), coalesce(CAST(`bed_count` AS STRING), '~'), coalesce(CAST(`opened_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `ward_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `ward_id`, `ward_code`, `ward_name`, `ward_type`, `hospital`, `status`, `acuity_level`, `bed_count`, `opened_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `ward_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`ward_id` IS NOT NULL), false) AS `_dq_ok_hc_ent_01`,
         coalesce((`ward_type` IS NULL OR `ward_type` IN ('EMERGENCY', 'ICU', 'MEDICAL', 'SURGICAL', 'MATERNITY')), false) AS `_dq_ok_hc_ent_02`,
         coalesce((`acuity_level` IS NULL OR (`acuity_level` >= 1 AND `acuity_level` <= 5)), false) AS `_dq_ok_hc_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('OPEN', 'PARTIAL', 'CLOSED', 'DECOMMISSIONED')), false) AS `_dq_ok_hc_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_ent_02` THEN 'HC-ENT-02' END, CASE WHEN NOT `_dq_ok_hc_ent_03` THEN 'HC-ENT-03' END, CASE WHEN NOT `_dq_ok_hc_ent_04` THEN 'HC-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_ent_01` THEN 'HC-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ward_id`,
  `ward_code`,
  `ward_name`,
  `ward_type`,
  `hospital`,
  `status`,
  `acuity_level`,
  `bed_count`,
  `opened_date`,
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

-- HC_PAS_ADMISSIONS: bronze.pas_admissions -> silver_mlv.admission
-- HC-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.admission
(
  CONSTRAINT hc_evt_01 CHECK (coalesce((`admission_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT hc_evt_03 CHECK (coalesce((discharge_ts IS NULL OR discharge_ts >= admit_ts), false)) ON MISMATCH DROP,
  CONSTRAINT hc_evt_06 CHECK (coalesce((`admit_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT hc_evt_02 CHECK (`_dq_ok_hc_evt_02`) ON MISMATCH DROP
)
COMMENT "Admissions - incremental by last_modified; can arrive before their ward (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'HC_PAS_ADMISSIONS')
AS
WITH src AS (
  SELECT
    try_cast((`admission_id`) AS int) AS `admission_id`,
    try_cast((`ward_id`) AS int) AS `ward_id`,
    try_cast((`admission_type`) AS string) AS `admission_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`admit_ts`) AS timestamp) AS `admit_ts`,
    try_cast((`discharge_ts`) AS timestamp) AS `discharge_ts`,
    try_cast((`episode_cost_aud`) AS decimal(12,2)) AS `episode_cost_aud`,
    try_cast((`nursing_hours`) AS double) AS `nursing_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pas_admissions
),
ranked AS (
  SELECT `admission_id`, `ward_id`, `admission_type`, `priority`, `status`, `admit_ts`, `discharge_ts`, `episode_cost_aud`, `nursing_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `admission_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`admission_id`, r.`ward_id`, r.`admission_type`, r.`priority`, r.`status`, r.`admit_ts`, r.`discharge_ts`, r.`episode_cost_aud`, r.`nursing_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`admission_id` IS NOT NULL), false) AS `_dq_ok_hc_evt_01`,
         (c.`ward_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_hc_evt_02`,
         coalesce((discharge_ts IS NULL OR discharge_ts >= admit_ts), false) AS `_dq_ok_hc_evt_03`,
         coalesce((`episode_cost_aud` IS NULL OR (`episode_cost_aud` >= 0)), false) AS `_dq_ok_hc_evt_04`,
         coalesce((admission_type <> 'EMERGENCY' OR priority = 1), false) AS `_dq_ok_hc_evt_05`,
         coalesce((`admit_ts` IS NOT NULL), false) AS `_dq_ok_hc_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `ward_id` AS _rk FROM silver_mlv.ward) ref1
    ON c.`ward_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_evt_04` THEN 'HC-EVT-04' END, CASE WHEN NOT `_dq_ok_hc_evt_05` THEN 'HC-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_evt_01` THEN 'HC-EVT-01' END, CASE WHEN NOT `_dq_ok_hc_evt_02` THEN 'HC-EVT-02' END, CASE WHEN NOT `_dq_ok_hc_evt_03` THEN 'HC-EVT-03' END, CASE WHEN NOT `_dq_ok_hc_evt_06` THEN 'HC-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `admission_id`,
  `ward_id`,
  `admission_type`,
  `priority`,
  `status`,
  `admit_ts`,
  `discharge_ts`,
  `episode_cost_aud`,
  `nursing_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_hc_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- HC_CRM_PATIENTS: bronze.crm_patients -> silver_mlv.patient
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- hash salt read from meta.setting (never written into the view definition)
-- PII dropped before Silver: first_name, last_name
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.patient
(
  CONSTRAINT hc_pty_01 CHECK (coalesce((`patient_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Patients - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'HC_CRM_PATIENTS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `patient_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`Hospital__c`) AS string) AS `hospital`,
    try_cast((`FundingType__c`) AS string) AS `funding_type`,
    try_cast((`WardId__c`) AS int) AS `ward_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`MedicareNo__c`) AS string) AS `medicare_no_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.crm_patients
),
ranked AS (
  SELECT `patient_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `hospital`, `funding_type`, `ward_id`, `status`, `medicare_no_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `patient_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`patient_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`hospital`, r.`funding_type`, r.`ward_id`, r.`status`, r.`medicare_no_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.crm_patients) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`patient_id` IS NOT NULL), false) AS `_dq_ok_hc_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_hc_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_hc_pty_03`,
         coalesce((`funding_type` IS NULL OR `funding_type` IN ('Public', 'Private insured', 'DVA', 'Overseas visitor')), false) AS `_dq_ok_hc_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_pty_02` THEN 'HC-PTY-02' END, CASE WHEN NOT `_dq_ok_hc_pty_03` THEN 'HC-PTY-03' END, CASE WHEN NOT `_dq_ok_hc_pty_04` THEN 'HC-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_pty_01` THEN 'HC-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `patient_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `hospital`,
  `funding_type`,
  `ward_id`,
  `status`,
  CASE WHEN `medicare_no_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`medicare_no_hash` AS STRING)), 256) END AS `medicare_no_hash`,
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

-- HC_BMS_WARD_OCCUPANCY: bronze.bms_ward_occupancy -> silver_mlv.ward_occupancy
-- HC-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.ward_occupancy
(
  CONSTRAINT hc_rdg_01 CHECK (coalesce((`occupancy_pct` IS NULL OR (`occupancy_pct` >= 0 AND `occupancy_pct` <= 120)), false)) ON MISMATCH DROP,
  CONSTRAINT hc_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Ward occupancy (15-min) every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'HC_BMS_WARD_OCCUPANCY')
AS
WITH src AS (
  SELECT
    try_cast((`ward_id`) AS int) AS `ward_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`occupancy_pct`) AS double) AS `occupancy_pct`,
    try_cast((`patients_waiting`) AS double) AS `patients_waiting`,
    try_cast((`nurses_on_shift`) AS double) AS `nurses_on_shift`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.bms_ward_occupancy
),
ranked AS (
  SELECT `ward_id`, `reading_ts`, `occupancy_pct`, `patients_waiting`, `nurses_on_shift`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `ward_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`ward_id`, r.`reading_ts`, r.`occupancy_pct`, r.`patients_waiting`, r.`nurses_on_shift`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`occupancy_pct` IS NULL OR (`occupancy_pct` >= 0 AND `occupancy_pct` <= 120)), false) AS `_dq_ok_hc_rdg_01`,
         (c.`ward_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_hc_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_hc_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `ward_id` AS _rk FROM silver_mlv.ward) ref1
    ON c.`ward_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_rdg_02` THEN 'HC-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_rdg_01` THEN 'HC-RDG-01' END, CASE WHEN NOT `_dq_ok_hc_rdg_03` THEN 'HC-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ward_id`,
  `reading_ts`,
  `occupancy_pct`,
  `patients_waiting`,
  `nurses_on_shift`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.admission by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.admission
COMMENT "Quarantine for HC_PAS_ADMISSIONS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`admission_id`) AS int) AS `admission_id`,
    try_cast((`ward_id`) AS int) AS `ward_id`,
    try_cast((`admission_type`) AS string) AS `admission_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`admit_ts`) AS timestamp) AS `admit_ts`,
    try_cast((`discharge_ts`) AS timestamp) AS `discharge_ts`,
    try_cast((`episode_cost_aud`) AS decimal(12,2)) AS `episode_cost_aud`,
    try_cast((`nursing_hours`) AS double) AS `nursing_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pas_admissions
),
ranked AS (
  SELECT `admission_id`, `ward_id`, `admission_type`, `priority`, `status`, `admit_ts`, `discharge_ts`, `episode_cost_aud`, `nursing_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `admission_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`admission_id`, r.`ward_id`, r.`admission_type`, r.`priority`, r.`status`, r.`admit_ts`, r.`discharge_ts`, r.`episode_cost_aud`, r.`nursing_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`admission_id` IS NOT NULL), false) AS `_dq_ok_hc_evt_01`,
         (c.`ward_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_hc_evt_02`,
         coalesce((discharge_ts IS NULL OR discharge_ts >= admit_ts), false) AS `_dq_ok_hc_evt_03`,
         coalesce((`episode_cost_aud` IS NULL OR (`episode_cost_aud` >= 0)), false) AS `_dq_ok_hc_evt_04`,
         coalesce((admission_type <> 'EMERGENCY' OR priority = 1), false) AS `_dq_ok_hc_evt_05`,
         coalesce((`admit_ts` IS NOT NULL), false) AS `_dq_ok_hc_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `ward_id` AS _rk FROM silver_mlv.ward) ref1
    ON c.`ward_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_evt_04` THEN 'HC-EVT-04' END, CASE WHEN NOT `_dq_ok_hc_evt_05` THEN 'HC-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_evt_01` THEN 'HC-EVT-01' END, CASE WHEN NOT `_dq_ok_hc_evt_02` THEN 'HC-EVT-02' END, CASE WHEN NOT `_dq_ok_hc_evt_03` THEN 'HC-EVT-03' END, CASE WHEN NOT `_dq_ok_hc_evt_06` THEN 'HC-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `admission_id`,
  `ward_id`,
  `admission_type`,
  `priority`,
  `status`,
  `admit_ts`,
  `discharge_ts`,
  `episode_cost_aud`,
  `nursing_hours`,
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

-- rows removed from silver_mlv.ward by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.ward
COMMENT "Quarantine for HC_PAS_WARDS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`ward_id`) AS int) AS `ward_id`,
    try_cast((`ward_code`) AS string) AS `ward_code`,
    try_cast((`ward_name`) AS string) AS `ward_name`,
    try_cast((`ward_type`) AS string) AS `ward_type`,
    try_cast((`hospital`) AS string) AS `hospital`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`acuity_level`) AS int) AS `acuity_level`,
    try_cast((`bed_count`) AS double) AS `bed_count`,
    try_cast((`opened_date`) AS date) AS `opened_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pas_wards
),
versions AS (
  SELECT `ward_id`, `ward_code`, `ward_name`, `ward_type`, `hospital`, `status`, `acuity_level`, `bed_count`, `opened_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `ward_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `ward_id`, `ward_code`, `ward_name`, `ward_type`, `hospital`, `status`, `acuity_level`, `bed_count`, `opened_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`ward_code` AS STRING), '~'), coalesce(CAST(`ward_name` AS STRING), '~'), coalesce(CAST(`ward_type` AS STRING), '~'), coalesce(CAST(`hospital` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`acuity_level` AS STRING), '~'), coalesce(CAST(`bed_count` AS STRING), '~'), coalesce(CAST(`opened_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `ward_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `ward_id`, `ward_code`, `ward_name`, `ward_type`, `hospital`, `status`, `acuity_level`, `bed_count`, `opened_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `ward_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`ward_id` IS NOT NULL), false) AS `_dq_ok_hc_ent_01`,
         coalesce((`ward_type` IS NULL OR `ward_type` IN ('EMERGENCY', 'ICU', 'MEDICAL', 'SURGICAL', 'MATERNITY')), false) AS `_dq_ok_hc_ent_02`,
         coalesce((`acuity_level` IS NULL OR (`acuity_level` >= 1 AND `acuity_level` <= 5)), false) AS `_dq_ok_hc_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('OPEN', 'PARTIAL', 'CLOSED', 'DECOMMISSIONED')), false) AS `_dq_ok_hc_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_ent_02` THEN 'HC-ENT-02' END, CASE WHEN NOT `_dq_ok_hc_ent_03` THEN 'HC-ENT-03' END, CASE WHEN NOT `_dq_ok_hc_ent_04` THEN 'HC-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_ent_01` THEN 'HC-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ward_id`,
  `ward_code`,
  `ward_name`,
  `ward_type`,
  `hospital`,
  `status`,
  `acuity_level`,
  `bed_count`,
  `opened_date`,
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

-- rows removed from silver_mlv.ward_occupancy by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.ward_occupancy
COMMENT "Quarantine for HC_BMS_WARD_OCCUPANCY - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`ward_id`) AS int) AS `ward_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`occupancy_pct`) AS double) AS `occupancy_pct`,
    try_cast((`patients_waiting`) AS double) AS `patients_waiting`,
    try_cast((`nurses_on_shift`) AS double) AS `nurses_on_shift`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.bms_ward_occupancy
),
ranked AS (
  SELECT `ward_id`, `reading_ts`, `occupancy_pct`, `patients_waiting`, `nurses_on_shift`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `ward_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`ward_id`, r.`reading_ts`, r.`occupancy_pct`, r.`patients_waiting`, r.`nurses_on_shift`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`occupancy_pct` IS NULL OR (`occupancy_pct` >= 0 AND `occupancy_pct` <= 120)), false) AS `_dq_ok_hc_rdg_01`,
         (c.`ward_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_hc_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_hc_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `ward_id` AS _rk FROM silver_mlv.ward) ref1
    ON c.`ward_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_rdg_02` THEN 'HC-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_rdg_01` THEN 'HC-RDG-01' END, CASE WHEN NOT `_dq_ok_hc_rdg_03` THEN 'HC-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ward_id`,
  `reading_ts`,
  `occupancy_pct`,
  `patients_waiting`,
  `nurses_on_shift`,
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

-- rows removed from silver_mlv.patient by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.patient
COMMENT "Quarantine for HC_CRM_PATIENTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `patient_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`Hospital__c`) AS string) AS `hospital`,
    try_cast((`FundingType__c`) AS string) AS `funding_type`,
    try_cast((`WardId__c`) AS int) AS `ward_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`MedicareNo__c`) AS string) AS `medicare_no_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.crm_patients
),
ranked AS (
  SELECT `patient_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `hospital`, `funding_type`, `ward_id`, `status`, `medicare_no_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `patient_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`patient_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`hospital`, r.`funding_type`, r.`ward_id`, r.`status`, r.`medicare_no_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.crm_patients) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`patient_id` IS NOT NULL), false) AS `_dq_ok_hc_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_hc_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_hc_pty_03`,
         coalesce((`funding_type` IS NULL OR `funding_type` IN ('Public', 'Private insured', 'DVA', 'Overseas visitor')), false) AS `_dq_ok_hc_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_hc_pty_02` THEN 'HC-PTY-02' END, CASE WHEN NOT `_dq_ok_hc_pty_03` THEN 'HC-PTY-03' END, CASE WHEN NOT `_dq_ok_hc_pty_04` THEN 'HC-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_hc_pty_01` THEN 'HC-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `patient_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `hospital`,
  `funding_type`,
  `ward_id`,
  `status`,
  CASE WHEN `medicare_no_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`medicare_no_hash` AS STRING)), 256) END AS `medicare_no_hash`,
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
SELECT 'HC_PAS_WARDS' AS object_id, 'ward' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.ward LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'HC_PAS_WARDS' AS object_id, 'ward' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.ward LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'HC_PAS_ADMISSIONS' AS object_id, 'admission' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.admission LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'HC_PAS_ADMISSIONS' AS object_id, 'admission' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.admission LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'HC_BMS_WARD_OCCUPANCY' AS object_id, 'ward_occupancy' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.ward_occupancy LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'HC_BMS_WARD_OCCUPANCY' AS object_id, 'ward_occupancy' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.ward_occupancy LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'HC_CRM_PATIENTS' AS object_id, 'patient' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.patient LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'HC_CRM_PATIENTS' AS object_id, 'patient' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.patient LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- HC-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_ward
COMMENT "Current view of the ward register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'HC-G02')
AS
SELECT ward_id, ward_code, ward_name, ward_type, hospital, status, acuity_level, bed_count, opened_date, CAST(floor(months_between(current_date(), opened_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.ward WHERE _is_current = true;

-- HC-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_admission
COMMENT "Admissions with duration and ward attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'HC-G03')
AS
SELECT v.admission_id, v.ward_id, v.admission_type, v.priority, v.status, v.admit_ts, v.discharge_ts, to_date(v.admit_ts) AS opened_date, CAST(date_format(v.admit_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.discharge_ts) - unix_timestamp(v.admit_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('ADMITTED', 'IN_TREATMENT') THEN 1 ELSE 0 END AS is_open, v.episode_cost_aud, v.nursing_hours, d.hospital, d.ward_type, d.acuity_level FROM silver_mlv.admission v LEFT JOIN silver_mlv.ward d ON d.ward_id = v.ward_id AND d._is_current = true WHERE v._is_deleted = false;

-- HC-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_ward_occupancy_hourly
COMMENT "Hourly ward occupancy (15-min) with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'HC-G04')
AS
SELECT ward_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(occupancy_pct), 2) AS avg_occupancy_pct, ROUND(MAX(occupancy_pct), 2) AS max_occupancy_pct, ROUND(AVG(patients_waiting), 2) AS avg_patients_waiting, ROUND(AVG(nurses_on_shift), 2) AS avg_nurses_on_shift, SUM(CASE WHEN occupancy_pct > 95 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.ward_occupancy GROUP BY ward_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- HC-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.patient_summary
COMMENT "Patients by hospital and funding_type"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'HC-G06')
AS
SELECT hospital, funding_type, COUNT(*) AS patients, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_patients, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT ward_id) AS linked_wards FROM silver_mlv.patient WHERE _is_deleted = false GROUP BY hospital, funding_type;

-- HC-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.ward_health_scorecard
COMMENT "Ward risk score - admissions, urgent work and bed occupancy breaches, plus access-block cost at risk in AUD (weights and dollar rates are illustrative: $1,500 per open admission + $2,000 per 15-min window above 95% occupancy + beds x acuity x $500)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'HC-G05')
AS
WITH asat AS (SELECT MAX(to_date(admit_ts)) AS as_at FROM silver_mlv.admission), ev AS (SELECT v.ward_id, COUNT(*) AS admissions_12m, SUM(CASE WHEN v.status IN ('ADMITTED', 'IN_TREATMENT') THEN 1 ELSE 0 END) AS open_admissions, SUM(CASE WHEN v.admission_type = 'EMERGENCY' THEN 1 ELSE 0 END) AS urgent_admissions_12m, ROUND(SUM(v.episode_cost_aud), 2) AS episode_cost_aud_12m FROM silver_mlv.admission v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.admit_ts) > add_months(asat.as_at, -12) GROUP BY v.ward_id), rd AS (SELECT ward_id, MAX(occupancy_pct) AS max_occupancy_pct, SUM(CASE WHEN occupancy_pct > 95 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.ward_occupancy GROUP BY ward_id), s AS (SELECT d.ward_id, d.ward_code, d.ward_name, d.ward_type, d.hospital, d.status, d.acuity_level, COALESCE(ev.admissions_12m, 0) AS admissions_12m, COALESCE(ev.open_admissions, 0) AS open_admissions, COALESCE(ev.urgent_admissions_12m, 0) AS urgent_admissions_12m, COALESCE(ev.episode_cost_aud_12m, 0) AS episode_cost_aud_12m, rd.max_occupancy_pct, COALESCE(rd.breach_readings, 0) AS breach_readings, d.bed_count FROM silver_mlv.ward d LEFT JOIN ev ON ev.ward_id = d.ward_id LEFT JOIN rd ON rd.ward_id = d.ward_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.acuity_level, 1) * 8 + s.open_admissions * 6 + s.urgent_admissions_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.acuity_level, 1) * 8 + s.open_admissions * 6 + s.urgent_admissions_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.acuity_level, 1) * 8 + s.open_admissions * 6 + s.urgent_admissions_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_admissions * 1500 + LEAST(s.breach_readings, 20) * 2000 + COALESCE(s.bed_count, 0) * COALESCE(s.acuity_level, 1) * 500, 0) AS access_block_cost_aud FROM s;
