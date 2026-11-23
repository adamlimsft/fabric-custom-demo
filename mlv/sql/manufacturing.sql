-- Kestrel Components (Manufacturing): materialized lake views generated from config/manufacturing.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.mes_machines SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.mes_maintenance_orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.iot_machine_telemetry SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.d365_distributor_contacts SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- MF_D365_DISTRIBUTOR_CONTACTS: bronze.d365_distributor_contacts -> silver_mlv.distributor_contact
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- PII dropped before Silver: first_name, last_name, driver_licence_no
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.distributor_contact
(
  CONSTRAINT mf_pty_01 CHECK (coalesce((`contact_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Distributor contacts - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'MF_D365_DISTRIBUTOR_CONTACTS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `contact_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`Plant__c`) AS string) AS `plant`,
    try_cast((`Channel__c`) AS string) AS `channel`,
    try_cast((`MachineId__c`) AS int) AS `machine_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`DriverLicenceNo__c`) AS string) AS `driver_licence_no`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.d365_distributor_contacts
),
ranked AS (
  SELECT `contact_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `plant`, `channel`, `machine_id`, `status`, `driver_licence_no`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `contact_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`contact_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`plant`, r.`channel`, r.`machine_id`, r.`status`, r.`driver_licence_no`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.d365_distributor_contacts) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`contact_id` IS NOT NULL), false) AS `_dq_ok_mf_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_mf_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_mf_pty_03`,
         coalesce((`channel` IS NULL OR `channel` IN ('Distributor', 'OEM', 'Retail chain', 'Export')), false) AS `_dq_ok_mf_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_pty_02` THEN 'MF-PTY-02' END, CASE WHEN NOT `_dq_ok_mf_pty_03` THEN 'MF-PTY-03' END, CASE WHEN NOT `_dq_ok_mf_pty_04` THEN 'MF-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_pty_01` THEN 'MF-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `contact_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `plant`,
  `channel`,
  `machine_id`,
  `status`,
  `last_modified_ts`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- MF_MES_MACHINES: bronze.mes_machines -> silver_mlv.machine
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.machine
(
  CONSTRAINT mf_ent_01 CHECK (coalesce((`machine_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Machine register. SCD2 keeps status, criticality and rated_units_per_hour history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'MF_MES_MACHINES')
AS
WITH src AS (
  SELECT
    try_cast((`machine_id`) AS int) AS `machine_id`,
    try_cast((`machine_code`) AS string) AS `machine_code`,
    try_cast((`machine_name`) AS string) AS `machine_name`,
    try_cast((`machine_type`) AS string) AS `machine_type`,
    try_cast((`plant`) AS string) AS `plant`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`rated_units_per_hour`) AS double) AS `rated_units_per_hour`,
    try_cast((`commissioned_date`) AS date) AS `commissioned_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.mes_machines
),
versions AS (
  SELECT `machine_id`, `machine_code`, `machine_name`, `machine_type`, `plant`, `status`, `criticality`, `rated_units_per_hour`, `commissioned_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `machine_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `machine_id`, `machine_code`, `machine_name`, `machine_type`, `plant`, `status`, `criticality`, `rated_units_per_hour`, `commissioned_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`machine_code` AS STRING), '~'), coalesce(CAST(`machine_name` AS STRING), '~'), coalesce(CAST(`machine_type` AS STRING), '~'), coalesce(CAST(`plant` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`rated_units_per_hour` AS STRING), '~'), coalesce(CAST(`commissioned_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `machine_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `machine_id`, `machine_code`, `machine_name`, `machine_type`, `plant`, `status`, `criticality`, `rated_units_per_hour`, `commissioned_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `machine_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`machine_id` IS NOT NULL), false) AS `_dq_ok_mf_ent_01`,
         coalesce((`machine_type` IS NULL OR `machine_type` IN ('CNC_MILL', 'INJECTION_MOULDER', 'STAMPING_PRESS', 'ROBOT_CELL', 'CONVEYOR')), false) AS `_dq_ok_mf_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_mf_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('RUNNING', 'IDLE', 'MAINTENANCE', 'RETIRED')), false) AS `_dq_ok_mf_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_ent_02` THEN 'MF-ENT-02' END, CASE WHEN NOT `_dq_ok_mf_ent_03` THEN 'MF-ENT-03' END, CASE WHEN NOT `_dq_ok_mf_ent_04` THEN 'MF-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_ent_01` THEN 'MF-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `machine_id`,
  `machine_code`,
  `machine_name`,
  `machine_type`,
  `plant`,
  `status`,
  `criticality`,
  `rated_units_per_hour`,
  `commissioned_date`,
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

-- MF_IOT_MACHINE_TELEMETRY: bronze.iot_machine_telemetry -> silver_mlv.machine_telemetry
-- MF-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.machine_telemetry
(
  CONSTRAINT mf_rdg_01 CHECK (coalesce((`vibration_mm_s` IS NULL OR (`vibration_mm_s` >= 0 AND `vibration_mm_s` <= 50)), false)) ON MISMATCH DROP,
  CONSTRAINT mf_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Machine telemetry every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'MF_IOT_MACHINE_TELEMETRY')
AS
WITH src AS (
  SELECT
    try_cast((`machine_id`) AS int) AS `machine_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`vibration_mm_s`) AS double) AS `vibration_mm_s`,
    try_cast((`units_produced`) AS double) AS `units_produced`,
    try_cast((`motor_temp_c`) AS double) AS `motor_temp_c`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.iot_machine_telemetry
),
ranked AS (
  SELECT `machine_id`, `reading_ts`, `vibration_mm_s`, `units_produced`, `motor_temp_c`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `machine_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`machine_id`, r.`reading_ts`, r.`vibration_mm_s`, r.`units_produced`, r.`motor_temp_c`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`vibration_mm_s` IS NULL OR (`vibration_mm_s` >= 0 AND `vibration_mm_s` <= 50)), false) AS `_dq_ok_mf_rdg_01`,
         (c.`machine_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_mf_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_mf_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `machine_id` AS _rk FROM silver_mlv.machine) ref1
    ON c.`machine_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_rdg_02` THEN 'MF-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_rdg_01` THEN 'MF-RDG-01' END, CASE WHEN NOT `_dq_ok_mf_rdg_03` THEN 'MF-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `machine_id`,
  `reading_ts`,
  `vibration_mm_s`,
  `units_produced`,
  `motor_temp_c`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- MF_MES_MAINTENANCE_ORDERS: bronze.mes_maintenance_orders -> silver_mlv.maintenance_order
-- MF-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.maintenance_order
(
  CONSTRAINT mf_evt_01 CHECK (coalesce((`mo_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT mf_evt_03 CHECK (coalesce((finished_ts IS NULL OR finished_ts >= reported_ts), false)) ON MISMATCH DROP,
  CONSTRAINT mf_evt_06 CHECK (coalesce((`reported_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT mf_evt_02 CHECK (`_dq_ok_mf_evt_02`) ON MISMATCH DROP
)
COMMENT "Maintenance orders - incremental by last_modified; can arrive before their machine (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'MF_MES_MAINTENANCE_ORDERS')
AS
WITH src AS (
  SELECT
    try_cast((`mo_id`) AS int) AS `mo_id`,
    try_cast((`machine_id`) AS int) AS `machine_id`,
    try_cast((`mo_type`) AS string) AS `mo_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`reported_ts`) AS timestamp) AS `reported_ts`,
    try_cast((`finished_ts`) AS timestamp) AS `finished_ts`,
    try_cast((`parts_cost_aud`) AS decimal(12,2)) AS `parts_cost_aud`,
    try_cast((`downtime_hours`) AS double) AS `downtime_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.mes_maintenance_orders
),
ranked AS (
  SELECT `mo_id`, `machine_id`, `mo_type`, `priority`, `status`, `reported_ts`, `finished_ts`, `parts_cost_aud`, `downtime_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `mo_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`mo_id`, r.`machine_id`, r.`mo_type`, r.`priority`, r.`status`, r.`reported_ts`, r.`finished_ts`, r.`parts_cost_aud`, r.`downtime_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`mo_id` IS NOT NULL), false) AS `_dq_ok_mf_evt_01`,
         (c.`machine_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_mf_evt_02`,
         coalesce((finished_ts IS NULL OR finished_ts >= reported_ts), false) AS `_dq_ok_mf_evt_03`,
         coalesce((`parts_cost_aud` IS NULL OR (`parts_cost_aud` >= 0)), false) AS `_dq_ok_mf_evt_04`,
         coalesce((mo_type <> 'BREAKDOWN' OR priority = 1), false) AS `_dq_ok_mf_evt_05`,
         coalesce((`reported_ts` IS NOT NULL), false) AS `_dq_ok_mf_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `machine_id` AS _rk FROM silver_mlv.machine) ref1
    ON c.`machine_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_evt_04` THEN 'MF-EVT-04' END, CASE WHEN NOT `_dq_ok_mf_evt_05` THEN 'MF-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_evt_01` THEN 'MF-EVT-01' END, CASE WHEN NOT `_dq_ok_mf_evt_02` THEN 'MF-EVT-02' END, CASE WHEN NOT `_dq_ok_mf_evt_03` THEN 'MF-EVT-03' END, CASE WHEN NOT `_dq_ok_mf_evt_06` THEN 'MF-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `mo_id`,
  `machine_id`,
  `mo_type`,
  `priority`,
  `status`,
  `reported_ts`,
  `finished_ts`,
  `parts_cost_aud`,
  `downtime_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_mf_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.distributor_contact by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.distributor_contact
COMMENT "Quarantine for MF_D365_DISTRIBUTOR_CONTACTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `contact_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`Plant__c`) AS string) AS `plant`,
    try_cast((`Channel__c`) AS string) AS `channel`,
    try_cast((`MachineId__c`) AS int) AS `machine_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`DriverLicenceNo__c`) AS string) AS `driver_licence_no`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.d365_distributor_contacts
),
ranked AS (
  SELECT `contact_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `plant`, `channel`, `machine_id`, `status`, `driver_licence_no`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `contact_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`contact_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`plant`, r.`channel`, r.`machine_id`, r.`status`, r.`driver_licence_no`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.d365_distributor_contacts) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`contact_id` IS NOT NULL), false) AS `_dq_ok_mf_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_mf_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_mf_pty_03`,
         coalesce((`channel` IS NULL OR `channel` IN ('Distributor', 'OEM', 'Retail chain', 'Export')), false) AS `_dq_ok_mf_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_pty_02` THEN 'MF-PTY-02' END, CASE WHEN NOT `_dq_ok_mf_pty_03` THEN 'MF-PTY-03' END, CASE WHEN NOT `_dq_ok_mf_pty_04` THEN 'MF-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_pty_01` THEN 'MF-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `contact_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `plant`,
  `channel`,
  `machine_id`,
  `status`,
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
WHERE size(`_dq_failed`) > 0;

-- rows removed from silver_mlv.machine by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.machine
COMMENT "Quarantine for MF_MES_MACHINES - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`machine_id`) AS int) AS `machine_id`,
    try_cast((`machine_code`) AS string) AS `machine_code`,
    try_cast((`machine_name`) AS string) AS `machine_name`,
    try_cast((`machine_type`) AS string) AS `machine_type`,
    try_cast((`plant`) AS string) AS `plant`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`rated_units_per_hour`) AS double) AS `rated_units_per_hour`,
    try_cast((`commissioned_date`) AS date) AS `commissioned_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.mes_machines
),
versions AS (
  SELECT `machine_id`, `machine_code`, `machine_name`, `machine_type`, `plant`, `status`, `criticality`, `rated_units_per_hour`, `commissioned_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `machine_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `machine_id`, `machine_code`, `machine_name`, `machine_type`, `plant`, `status`, `criticality`, `rated_units_per_hour`, `commissioned_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`machine_code` AS STRING), '~'), coalesce(CAST(`machine_name` AS STRING), '~'), coalesce(CAST(`machine_type` AS STRING), '~'), coalesce(CAST(`plant` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`rated_units_per_hour` AS STRING), '~'), coalesce(CAST(`commissioned_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `machine_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `machine_id`, `machine_code`, `machine_name`, `machine_type`, `plant`, `status`, `criticality`, `rated_units_per_hour`, `commissioned_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `machine_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`machine_id` IS NOT NULL), false) AS `_dq_ok_mf_ent_01`,
         coalesce((`machine_type` IS NULL OR `machine_type` IN ('CNC_MILL', 'INJECTION_MOULDER', 'STAMPING_PRESS', 'ROBOT_CELL', 'CONVEYOR')), false) AS `_dq_ok_mf_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_mf_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('RUNNING', 'IDLE', 'MAINTENANCE', 'RETIRED')), false) AS `_dq_ok_mf_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_ent_02` THEN 'MF-ENT-02' END, CASE WHEN NOT `_dq_ok_mf_ent_03` THEN 'MF-ENT-03' END, CASE WHEN NOT `_dq_ok_mf_ent_04` THEN 'MF-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_ent_01` THEN 'MF-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `machine_id`,
  `machine_code`,
  `machine_name`,
  `machine_type`,
  `plant`,
  `status`,
  `criticality`,
  `rated_units_per_hour`,
  `commissioned_date`,
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

-- rows removed from silver_mlv.maintenance_order by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.maintenance_order
COMMENT "Quarantine for MF_MES_MAINTENANCE_ORDERS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`mo_id`) AS int) AS `mo_id`,
    try_cast((`machine_id`) AS int) AS `machine_id`,
    try_cast((`mo_type`) AS string) AS `mo_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`reported_ts`) AS timestamp) AS `reported_ts`,
    try_cast((`finished_ts`) AS timestamp) AS `finished_ts`,
    try_cast((`parts_cost_aud`) AS decimal(12,2)) AS `parts_cost_aud`,
    try_cast((`downtime_hours`) AS double) AS `downtime_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.mes_maintenance_orders
),
ranked AS (
  SELECT `mo_id`, `machine_id`, `mo_type`, `priority`, `status`, `reported_ts`, `finished_ts`, `parts_cost_aud`, `downtime_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `mo_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`mo_id`, r.`machine_id`, r.`mo_type`, r.`priority`, r.`status`, r.`reported_ts`, r.`finished_ts`, r.`parts_cost_aud`, r.`downtime_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`mo_id` IS NOT NULL), false) AS `_dq_ok_mf_evt_01`,
         (c.`machine_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_mf_evt_02`,
         coalesce((finished_ts IS NULL OR finished_ts >= reported_ts), false) AS `_dq_ok_mf_evt_03`,
         coalesce((`parts_cost_aud` IS NULL OR (`parts_cost_aud` >= 0)), false) AS `_dq_ok_mf_evt_04`,
         coalesce((mo_type <> 'BREAKDOWN' OR priority = 1), false) AS `_dq_ok_mf_evt_05`,
         coalesce((`reported_ts` IS NOT NULL), false) AS `_dq_ok_mf_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `machine_id` AS _rk FROM silver_mlv.machine) ref1
    ON c.`machine_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_evt_04` THEN 'MF-EVT-04' END, CASE WHEN NOT `_dq_ok_mf_evt_05` THEN 'MF-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_evt_01` THEN 'MF-EVT-01' END, CASE WHEN NOT `_dq_ok_mf_evt_02` THEN 'MF-EVT-02' END, CASE WHEN NOT `_dq_ok_mf_evt_03` THEN 'MF-EVT-03' END, CASE WHEN NOT `_dq_ok_mf_evt_06` THEN 'MF-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `mo_id`,
  `machine_id`,
  `mo_type`,
  `priority`,
  `status`,
  `reported_ts`,
  `finished_ts`,
  `parts_cost_aud`,
  `downtime_hours`,
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

-- rows removed from silver_mlv.machine_telemetry by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.machine_telemetry
COMMENT "Quarantine for MF_IOT_MACHINE_TELEMETRY - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`machine_id`) AS int) AS `machine_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`vibration_mm_s`) AS double) AS `vibration_mm_s`,
    try_cast((`units_produced`) AS double) AS `units_produced`,
    try_cast((`motor_temp_c`) AS double) AS `motor_temp_c`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.iot_machine_telemetry
),
ranked AS (
  SELECT `machine_id`, `reading_ts`, `vibration_mm_s`, `units_produced`, `motor_temp_c`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `machine_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`machine_id`, r.`reading_ts`, r.`vibration_mm_s`, r.`units_produced`, r.`motor_temp_c`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`vibration_mm_s` IS NULL OR (`vibration_mm_s` >= 0 AND `vibration_mm_s` <= 50)), false) AS `_dq_ok_mf_rdg_01`,
         (c.`machine_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_mf_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_mf_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `machine_id` AS _rk FROM silver_mlv.machine) ref1
    ON c.`machine_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_mf_rdg_02` THEN 'MF-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_mf_rdg_01` THEN 'MF-RDG-01' END, CASE WHEN NOT `_dq_ok_mf_rdg_03` THEN 'MF-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `machine_id`,
  `reading_ts`,
  `vibration_mm_s`,
  `units_produced`,
  `motor_temp_c`,
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
SELECT 'MF_MES_MACHINES' AS object_id, 'machine' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.machine LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'MF_MES_MACHINES' AS object_id, 'machine' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.machine LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'MF_MES_MAINTENANCE_ORDERS' AS object_id, 'maintenance_order' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.maintenance_order LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'MF_MES_MAINTENANCE_ORDERS' AS object_id, 'maintenance_order' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.maintenance_order LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'MF_IOT_MACHINE_TELEMETRY' AS object_id, 'machine_telemetry' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.machine_telemetry LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'MF_IOT_MACHINE_TELEMETRY' AS object_id, 'machine_telemetry' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.machine_telemetry LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'MF_D365_DISTRIBUTOR_CONTACTS' AS object_id, 'distributor_contact' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.distributor_contact LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'MF_D365_DISTRIBUTOR_CONTACTS' AS object_id, 'distributor_contact' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.distributor_contact LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- MF-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_machine
COMMENT "Current view of the machine register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'MF-G02')
AS
SELECT machine_id, machine_code, machine_name, machine_type, plant, status, criticality, rated_units_per_hour, commissioned_date, CAST(floor(months_between(current_date(), commissioned_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.machine WHERE _is_current = true;

-- MF-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.distributor_contact_summary
COMMENT "Distributor contacts by plant and channel"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'MF-G06')
AS
SELECT plant, channel, COUNT(*) AS distributor_contacts, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_distributor_contacts, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT machine_id) AS linked_machines FROM silver_mlv.distributor_contact WHERE _is_deleted = false GROUP BY plant, channel;

-- MF-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_machine_telemetry_hourly
COMMENT "Hourly machine telemetry with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'MF-G04')
AS
SELECT machine_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(vibration_mm_s), 2) AS avg_vibration_mm_s, ROUND(MAX(vibration_mm_s), 2) AS max_vibration_mm_s, ROUND(AVG(units_produced), 2) AS avg_units_produced, ROUND(AVG(motor_temp_c), 2) AS avg_motor_temp_c, SUM(CASE WHEN vibration_mm_s > 7.1 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.machine_telemetry GROUP BY machine_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- MF-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_maintenance_order
COMMENT "Maintenance orders with duration and machine attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'MF-G03')
AS
SELECT v.mo_id, v.machine_id, v.mo_type, v.priority, v.status, v.reported_ts, v.finished_ts, to_date(v.reported_ts) AS opened_date, CAST(date_format(v.reported_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.finished_ts) - unix_timestamp(v.reported_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END AS is_open, v.parts_cost_aud, v.downtime_hours, d.plant, d.machine_type, d.criticality FROM silver_mlv.maintenance_order v LEFT JOIN silver_mlv.machine d ON d.machine_id = v.machine_id AND d._is_current = true WHERE v._is_deleted = false;

-- MF-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.machine_health_scorecard
COMMENT "Machine risk score - maintenance orders, urgent work and vibration breaches, plus downtime cost at risk in AUD (weights and dollar rates are illustrative: $5,000 per open maintenance order + $3,000 per 15-min window above the vibration limit + rated units/hour x criticality x $2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'MF-G05')
AS
WITH asat AS (SELECT MAX(to_date(reported_ts)) AS as_at FROM silver_mlv.maintenance_order), ev AS (SELECT v.machine_id, COUNT(*) AS maintenance_orders_12m, SUM(CASE WHEN v.status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END) AS open_maintenance_orders, SUM(CASE WHEN v.mo_type = 'BREAKDOWN' THEN 1 ELSE 0 END) AS urgent_maintenance_orders_12m, ROUND(SUM(v.parts_cost_aud), 2) AS parts_cost_aud_12m FROM silver_mlv.maintenance_order v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.reported_ts) > add_months(asat.as_at, -12) GROUP BY v.machine_id), rd AS (SELECT machine_id, MAX(vibration_mm_s) AS max_vibration_mm_s, SUM(CASE WHEN vibration_mm_s > 7.1 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.machine_telemetry GROUP BY machine_id), s AS (SELECT d.machine_id, d.machine_code, d.machine_name, d.machine_type, d.plant, d.status, d.criticality, COALESCE(ev.maintenance_orders_12m, 0) AS maintenance_orders_12m, COALESCE(ev.open_maintenance_orders, 0) AS open_maintenance_orders, COALESCE(ev.urgent_maintenance_orders_12m, 0) AS urgent_maintenance_orders_12m, COALESCE(ev.parts_cost_aud_12m, 0) AS parts_cost_aud_12m, rd.max_vibration_mm_s, COALESCE(rd.breach_readings, 0) AS breach_readings, d.rated_units_per_hour FROM silver_mlv.machine d LEFT JOIN ev ON ev.machine_id = d.machine_id LEFT JOIN rd ON rd.machine_id = d.machine_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_maintenance_orders * 6 + s.urgent_maintenance_orders_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_maintenance_orders * 6 + s.urgent_maintenance_orders_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_maintenance_orders * 6 + s.urgent_maintenance_orders_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_maintenance_orders * 5000 + LEAST(s.breach_readings, 20) * 3000 + COALESCE(s.rated_units_per_hour, 0) * COALESCE(s.criticality, 1) * 2, 0) AS downtime_cost_at_risk_aud FROM s;
