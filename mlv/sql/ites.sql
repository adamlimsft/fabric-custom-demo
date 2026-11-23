-- Bluegum Digital Services (IT & ITeS): materialized lake views generated from config/ites.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.itsm_client_services SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.itsm_incidents SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.mon_service_metrics SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.hub_client_contacts SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- IT_HUB_CLIENT_CONTACTS: bronze.hub_client_contacts -> silver_mlv.client_contact
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- PII dropped before Silver: first_name, last_name, personal_mobile_2
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.client_contact
(
  CONSTRAINT it_pty_01 CHECK (coalesce((`contact_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Client contacts - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'IT_HUB_CLIENT_CONTACTS')
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
    try_cast((`DeliveryCentre__c`) AS string) AS `delivery_centre`,
    try_cast((`ContactRole__c`) AS string) AS `contact_role`,
    try_cast((`ServiceId__c`) AS int) AS `service_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`PersonalMobile2__c`) AS string) AS `personal_mobile_2`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.hub_client_contacts
),
ranked AS (
  SELECT `contact_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `delivery_centre`, `contact_role`, `service_id`, `status`, `personal_mobile_2`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `contact_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`contact_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`delivery_centre`, r.`contact_role`, r.`service_id`, r.`status`, r.`personal_mobile_2`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.hub_client_contacts) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`contact_id` IS NOT NULL), false) AS `_dq_ok_it_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_it_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_it_pty_03`,
         coalesce((`contact_role` IS NULL OR `contact_role` IN ('Executive sponsor', 'Service owner', 'IT manager', 'Procurement')), false) AS `_dq_ok_it_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_pty_02` THEN 'IT-PTY-02' END, CASE WHEN NOT `_dq_ok_it_pty_03` THEN 'IT-PTY-03' END, CASE WHEN NOT `_dq_ok_it_pty_04` THEN 'IT-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_pty_01` THEN 'IT-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `contact_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `delivery_centre`,
  `contact_role`,
  `service_id`,
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

-- IT_ITSM_CLIENT_SERVICES: bronze.itsm_client_services -> silver_mlv.client_service
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.client_service
(
  CONSTRAINT it_ent_01 CHECK (coalesce((`service_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Client service register. SCD2 keeps status, sla_tier and monthly_fee_aud history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'IT_ITSM_CLIENT_SERVICES')
AS
WITH src AS (
  SELECT
    try_cast((`service_id`) AS int) AS `service_id`,
    try_cast((`contract_ref`) AS string) AS `contract_ref`,
    try_cast((`service_name`) AS string) AS `service_name`,
    try_cast((`service_line`) AS string) AS `service_line`,
    try_cast((`delivery_centre`) AS string) AS `delivery_centre`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`sla_tier`) AS int) AS `sla_tier`,
    try_cast((`monthly_fee_aud`) AS double) AS `monthly_fee_aud`,
    try_cast((`contract_start`) AS date) AS `contract_start`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.itsm_client_services
),
versions AS (
  SELECT `service_id`, `contract_ref`, `service_name`, `service_line`, `delivery_centre`, `status`, `sla_tier`, `monthly_fee_aud`, `contract_start`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `service_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `service_id`, `contract_ref`, `service_name`, `service_line`, `delivery_centre`, `status`, `sla_tier`, `monthly_fee_aud`, `contract_start`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`contract_ref` AS STRING), '~'), coalesce(CAST(`service_name` AS STRING), '~'), coalesce(CAST(`service_line` AS STRING), '~'), coalesce(CAST(`delivery_centre` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`sla_tier` AS STRING), '~'), coalesce(CAST(`monthly_fee_aud` AS STRING), '~'), coalesce(CAST(`contract_start` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `service_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `service_id`, `contract_ref`, `service_name`, `service_line`, `delivery_centre`, `status`, `sla_tier`, `monthly_fee_aud`, `contract_start`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `service_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`service_id` IS NOT NULL), false) AS `_dq_ok_it_ent_01`,
         coalesce((`service_line` IS NULL OR `service_line` IN ('MANAGED_INFRA', 'APP_SUPPORT', 'CLOUD_OPS', 'SERVICE_DESK', 'SECURITY_OPS')), false) AS `_dq_ok_it_ent_02`,
         coalesce((`sla_tier` IS NULL OR (`sla_tier` >= 1 AND `sla_tier` <= 5)), false) AS `_dq_ok_it_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('LIVE', 'AT_RISK', 'IN_TRANSITION', 'EXITED')), false) AS `_dq_ok_it_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_ent_02` THEN 'IT-ENT-02' END, CASE WHEN NOT `_dq_ok_it_ent_03` THEN 'IT-ENT-03' END, CASE WHEN NOT `_dq_ok_it_ent_04` THEN 'IT-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_ent_01` THEN 'IT-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `service_id`,
  `contract_ref`,
  `service_name`,
  `service_line`,
  `delivery_centre`,
  `status`,
  `sla_tier`,
  `monthly_fee_aud`,
  `contract_start`,
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

-- IT_ITSM_INCIDENTS: bronze.itsm_incidents -> silver_mlv.incident
-- IT-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.incident
(
  CONSTRAINT it_evt_01 CHECK (coalesce((`incident_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT it_evt_03 CHECK (coalesce((resolved_ts IS NULL OR resolved_ts >= opened_ts), false)) ON MISMATCH DROP,
  CONSTRAINT it_evt_06 CHECK (coalesce((`opened_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT it_evt_02 CHECK (`_dq_ok_it_evt_02`) ON MISMATCH DROP
)
COMMENT "Incidents - incremental by last_modified; can arrive before their client service (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'IT_ITSM_INCIDENTS')
AS
WITH src AS (
  SELECT
    try_cast((`incident_id`) AS int) AS `incident_id`,
    try_cast((`service_id`) AS int) AS `service_id`,
    try_cast((`category`) AS string) AS `category`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`opened_ts`) AS timestamp) AS `opened_ts`,
    try_cast((`resolved_ts`) AS timestamp) AS `resolved_ts`,
    try_cast((`sla_penalty_aud`) AS decimal(12,2)) AS `sla_penalty_aud`,
    try_cast((`effort_hours`) AS double) AS `effort_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.itsm_incidents
),
ranked AS (
  SELECT `incident_id`, `service_id`, `category`, `priority`, `status`, `opened_ts`, `resolved_ts`, `sla_penalty_aud`, `effort_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `incident_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`incident_id`, r.`service_id`, r.`category`, r.`priority`, r.`status`, r.`opened_ts`, r.`resolved_ts`, r.`sla_penalty_aud`, r.`effort_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`incident_id` IS NOT NULL), false) AS `_dq_ok_it_evt_01`,
         (c.`service_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_it_evt_02`,
         coalesce((resolved_ts IS NULL OR resolved_ts >= opened_ts), false) AS `_dq_ok_it_evt_03`,
         coalesce((`sla_penalty_aud` IS NULL OR (`sla_penalty_aud` >= 0)), false) AS `_dq_ok_it_evt_04`,
         coalesce((category <> 'OUTAGE' OR priority = 1), false) AS `_dq_ok_it_evt_05`,
         coalesce((`opened_ts` IS NOT NULL), false) AS `_dq_ok_it_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `service_id` AS _rk FROM silver_mlv.client_service) ref1
    ON c.`service_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_evt_04` THEN 'IT-EVT-04' END, CASE WHEN NOT `_dq_ok_it_evt_05` THEN 'IT-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_evt_01` THEN 'IT-EVT-01' END, CASE WHEN NOT `_dq_ok_it_evt_02` THEN 'IT-EVT-02' END, CASE WHEN NOT `_dq_ok_it_evt_03` THEN 'IT-EVT-03' END, CASE WHEN NOT `_dq_ok_it_evt_06` THEN 'IT-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `incident_id`,
  `service_id`,
  `category`,
  `priority`,
  `status`,
  `opened_ts`,
  `resolved_ts`,
  `sla_penalty_aud`,
  `effort_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_it_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- IT_MON_SERVICE_METRICS: bronze.mon_service_metrics -> silver_mlv.service_metric
-- IT-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.service_metric
(
  CONSTRAINT it_rdg_01 CHECK (coalesce((`p95_response_ms` IS NULL OR (`p95_response_ms` >= 0 AND `p95_response_ms` <= 120000)), false)) ON MISMATCH DROP,
  CONSTRAINT it_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Service performance (15-min) every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'IT_MON_SERVICE_METRICS')
AS
WITH src AS (
  SELECT
    try_cast((`service_id`) AS int) AS `service_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`p95_response_ms`) AS double) AS `p95_response_ms`,
    try_cast((`availability_pct`) AS double) AS `availability_pct`,
    try_cast((`ticket_backlog`) AS double) AS `ticket_backlog`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.mon_service_metrics
),
ranked AS (
  SELECT `service_id`, `reading_ts`, `p95_response_ms`, `availability_pct`, `ticket_backlog`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `service_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`service_id`, r.`reading_ts`, r.`p95_response_ms`, r.`availability_pct`, r.`ticket_backlog`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`p95_response_ms` IS NULL OR (`p95_response_ms` >= 0 AND `p95_response_ms` <= 120000)), false) AS `_dq_ok_it_rdg_01`,
         (c.`service_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_it_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_it_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `service_id` AS _rk FROM silver_mlv.client_service) ref1
    ON c.`service_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_rdg_02` THEN 'IT-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_rdg_01` THEN 'IT-RDG-01' END, CASE WHEN NOT `_dq_ok_it_rdg_03` THEN 'IT-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `service_id`,
  `reading_ts`,
  `p95_response_ms`,
  `availability_pct`,
  `ticket_backlog`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.client_contact by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.client_contact
COMMENT "Quarantine for IT_HUB_CLIENT_CONTACTS - rows failing quarantine/fail rules"
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
    try_cast((`DeliveryCentre__c`) AS string) AS `delivery_centre`,
    try_cast((`ContactRole__c`) AS string) AS `contact_role`,
    try_cast((`ServiceId__c`) AS int) AS `service_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`PersonalMobile2__c`) AS string) AS `personal_mobile_2`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.hub_client_contacts
),
ranked AS (
  SELECT `contact_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `delivery_centre`, `contact_role`, `service_id`, `status`, `personal_mobile_2`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `contact_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`contact_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`delivery_centre`, r.`contact_role`, r.`service_id`, r.`status`, r.`personal_mobile_2`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.hub_client_contacts) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`contact_id` IS NOT NULL), false) AS `_dq_ok_it_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_it_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_it_pty_03`,
         coalesce((`contact_role` IS NULL OR `contact_role` IN ('Executive sponsor', 'Service owner', 'IT manager', 'Procurement')), false) AS `_dq_ok_it_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_pty_02` THEN 'IT-PTY-02' END, CASE WHEN NOT `_dq_ok_it_pty_03` THEN 'IT-PTY-03' END, CASE WHEN NOT `_dq_ok_it_pty_04` THEN 'IT-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_pty_01` THEN 'IT-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `contact_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `delivery_centre`,
  `contact_role`,
  `service_id`,
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

-- rows removed from silver_mlv.client_service by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.client_service
COMMENT "Quarantine for IT_ITSM_CLIENT_SERVICES - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`service_id`) AS int) AS `service_id`,
    try_cast((`contract_ref`) AS string) AS `contract_ref`,
    try_cast((`service_name`) AS string) AS `service_name`,
    try_cast((`service_line`) AS string) AS `service_line`,
    try_cast((`delivery_centre`) AS string) AS `delivery_centre`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`sla_tier`) AS int) AS `sla_tier`,
    try_cast((`monthly_fee_aud`) AS double) AS `monthly_fee_aud`,
    try_cast((`contract_start`) AS date) AS `contract_start`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.itsm_client_services
),
versions AS (
  SELECT `service_id`, `contract_ref`, `service_name`, `service_line`, `delivery_centre`, `status`, `sla_tier`, `monthly_fee_aud`, `contract_start`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `service_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `service_id`, `contract_ref`, `service_name`, `service_line`, `delivery_centre`, `status`, `sla_tier`, `monthly_fee_aud`, `contract_start`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`contract_ref` AS STRING), '~'), coalesce(CAST(`service_name` AS STRING), '~'), coalesce(CAST(`service_line` AS STRING), '~'), coalesce(CAST(`delivery_centre` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`sla_tier` AS STRING), '~'), coalesce(CAST(`monthly_fee_aud` AS STRING), '~'), coalesce(CAST(`contract_start` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `service_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `service_id`, `contract_ref`, `service_name`, `service_line`, `delivery_centre`, `status`, `sla_tier`, `monthly_fee_aud`, `contract_start`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `service_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`service_id` IS NOT NULL), false) AS `_dq_ok_it_ent_01`,
         coalesce((`service_line` IS NULL OR `service_line` IN ('MANAGED_INFRA', 'APP_SUPPORT', 'CLOUD_OPS', 'SERVICE_DESK', 'SECURITY_OPS')), false) AS `_dq_ok_it_ent_02`,
         coalesce((`sla_tier` IS NULL OR (`sla_tier` >= 1 AND `sla_tier` <= 5)), false) AS `_dq_ok_it_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('LIVE', 'AT_RISK', 'IN_TRANSITION', 'EXITED')), false) AS `_dq_ok_it_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_ent_02` THEN 'IT-ENT-02' END, CASE WHEN NOT `_dq_ok_it_ent_03` THEN 'IT-ENT-03' END, CASE WHEN NOT `_dq_ok_it_ent_04` THEN 'IT-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_ent_01` THEN 'IT-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `service_id`,
  `contract_ref`,
  `service_name`,
  `service_line`,
  `delivery_centre`,
  `status`,
  `sla_tier`,
  `monthly_fee_aud`,
  `contract_start`,
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

-- rows removed from silver_mlv.incident by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.incident
COMMENT "Quarantine for IT_ITSM_INCIDENTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`incident_id`) AS int) AS `incident_id`,
    try_cast((`service_id`) AS int) AS `service_id`,
    try_cast((`category`) AS string) AS `category`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`opened_ts`) AS timestamp) AS `opened_ts`,
    try_cast((`resolved_ts`) AS timestamp) AS `resolved_ts`,
    try_cast((`sla_penalty_aud`) AS decimal(12,2)) AS `sla_penalty_aud`,
    try_cast((`effort_hours`) AS double) AS `effort_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.itsm_incidents
),
ranked AS (
  SELECT `incident_id`, `service_id`, `category`, `priority`, `status`, `opened_ts`, `resolved_ts`, `sla_penalty_aud`, `effort_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `incident_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`incident_id`, r.`service_id`, r.`category`, r.`priority`, r.`status`, r.`opened_ts`, r.`resolved_ts`, r.`sla_penalty_aud`, r.`effort_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`incident_id` IS NOT NULL), false) AS `_dq_ok_it_evt_01`,
         (c.`service_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_it_evt_02`,
         coalesce((resolved_ts IS NULL OR resolved_ts >= opened_ts), false) AS `_dq_ok_it_evt_03`,
         coalesce((`sla_penalty_aud` IS NULL OR (`sla_penalty_aud` >= 0)), false) AS `_dq_ok_it_evt_04`,
         coalesce((category <> 'OUTAGE' OR priority = 1), false) AS `_dq_ok_it_evt_05`,
         coalesce((`opened_ts` IS NOT NULL), false) AS `_dq_ok_it_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `service_id` AS _rk FROM silver_mlv.client_service) ref1
    ON c.`service_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_evt_04` THEN 'IT-EVT-04' END, CASE WHEN NOT `_dq_ok_it_evt_05` THEN 'IT-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_evt_01` THEN 'IT-EVT-01' END, CASE WHEN NOT `_dq_ok_it_evt_02` THEN 'IT-EVT-02' END, CASE WHEN NOT `_dq_ok_it_evt_03` THEN 'IT-EVT-03' END, CASE WHEN NOT `_dq_ok_it_evt_06` THEN 'IT-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `incident_id`,
  `service_id`,
  `category`,
  `priority`,
  `status`,
  `opened_ts`,
  `resolved_ts`,
  `sla_penalty_aud`,
  `effort_hours`,
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

-- rows removed from silver_mlv.service_metric by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.service_metric
COMMENT "Quarantine for IT_MON_SERVICE_METRICS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`service_id`) AS int) AS `service_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`p95_response_ms`) AS double) AS `p95_response_ms`,
    try_cast((`availability_pct`) AS double) AS `availability_pct`,
    try_cast((`ticket_backlog`) AS double) AS `ticket_backlog`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.mon_service_metrics
),
ranked AS (
  SELECT `service_id`, `reading_ts`, `p95_response_ms`, `availability_pct`, `ticket_backlog`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `service_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`service_id`, r.`reading_ts`, r.`p95_response_ms`, r.`availability_pct`, r.`ticket_backlog`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`p95_response_ms` IS NULL OR (`p95_response_ms` >= 0 AND `p95_response_ms` <= 120000)), false) AS `_dq_ok_it_rdg_01`,
         (c.`service_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_it_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_it_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `service_id` AS _rk FROM silver_mlv.client_service) ref1
    ON c.`service_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_it_rdg_02` THEN 'IT-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_it_rdg_01` THEN 'IT-RDG-01' END, CASE WHEN NOT `_dq_ok_it_rdg_03` THEN 'IT-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `service_id`,
  `reading_ts`,
  `p95_response_ms`,
  `availability_pct`,
  `ticket_backlog`,
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
SELECT 'IT_ITSM_CLIENT_SERVICES' AS object_id, 'client_service' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.client_service LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'IT_ITSM_CLIENT_SERVICES' AS object_id, 'client_service' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.client_service LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'IT_ITSM_INCIDENTS' AS object_id, 'incident' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.incident LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'IT_ITSM_INCIDENTS' AS object_id, 'incident' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.incident LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'IT_MON_SERVICE_METRICS' AS object_id, 'service_metric' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.service_metric LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'IT_MON_SERVICE_METRICS' AS object_id, 'service_metric' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.service_metric LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'IT_HUB_CLIENT_CONTACTS' AS object_id, 'client_contact' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.client_contact LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'IT_HUB_CLIENT_CONTACTS' AS object_id, 'client_contact' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.client_contact LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- IT-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.client_contact_summary
COMMENT "Client contacts by delivery centre and contact_role"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'IT-G06')
AS
SELECT delivery_centre, contact_role, COUNT(*) AS client_contacts, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_client_contacts, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT service_id) AS linked_client_services FROM silver_mlv.client_contact WHERE _is_deleted = false GROUP BY delivery_centre, contact_role;

-- IT-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.client_service_health_scorecard
COMMENT "Client service risk score - incidents, urgent work and p95 response time breaches, plus contract revenue at risk in AUD (weights and dollar rates are illustrative: $2,000 SLA penalty exposure per open incident + $500 per slow 15-min window + monthly fee x SLA tier x 1%)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'IT-G05')
AS
WITH asat AS (SELECT MAX(to_date(opened_ts)) AS as_at FROM silver_mlv.incident), ev AS (SELECT v.service_id, COUNT(*) AS incidents_12m, SUM(CASE WHEN v.status IN ('NEW', 'IN_PROGRESS') THEN 1 ELSE 0 END) AS open_incidents, SUM(CASE WHEN v.category = 'OUTAGE' THEN 1 ELSE 0 END) AS urgent_incidents_12m, ROUND(SUM(v.sla_penalty_aud), 2) AS sla_penalty_aud_12m FROM silver_mlv.incident v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.opened_ts) > add_months(asat.as_at, -12) GROUP BY v.service_id), rd AS (SELECT service_id, MAX(p95_response_ms) AS max_p95_response_ms, SUM(CASE WHEN p95_response_ms > 2500 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.service_metric GROUP BY service_id), s AS (SELECT d.service_id, d.contract_ref, d.service_name, d.service_line, d.delivery_centre, d.status, d.sla_tier, COALESCE(ev.incidents_12m, 0) AS incidents_12m, COALESCE(ev.open_incidents, 0) AS open_incidents, COALESCE(ev.urgent_incidents_12m, 0) AS urgent_incidents_12m, COALESCE(ev.sla_penalty_aud_12m, 0) AS sla_penalty_aud_12m, rd.max_p95_response_ms, COALESCE(rd.breach_readings, 0) AS breach_readings, d.monthly_fee_aud FROM silver_mlv.client_service d LEFT JOIN ev ON ev.service_id = d.service_id LEFT JOIN rd ON rd.service_id = d.service_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.sla_tier, 1) * 8 + s.open_incidents * 6 + s.urgent_incidents_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.sla_tier, 1) * 8 + s.open_incidents * 6 + s.urgent_incidents_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.sla_tier, 1) * 8 + s.open_incidents * 6 + s.urgent_incidents_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_incidents * 2000 + LEAST(s.breach_readings, 20) * 500 + COALESCE(s.monthly_fee_aud, 0) * COALESCE(s.sla_tier, 1) * 0.01, 0) AS revenue_at_risk_aud FROM s;

-- IT-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_client_service
COMMENT "Current view of the client service register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'IT-G02')
AS
SELECT service_id, contract_ref, service_name, service_line, delivery_centre, status, sla_tier, monthly_fee_aud, contract_start, CAST(floor(months_between(current_date(), contract_start) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.client_service WHERE _is_current = true;

-- IT-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_incident
COMMENT "Incidents with duration and client service attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'IT-G03')
AS
SELECT v.incident_id, v.service_id, v.category, v.priority, v.status, v.opened_ts, v.resolved_ts, to_date(v.opened_ts) AS opened_date, CAST(date_format(v.opened_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.resolved_ts) - unix_timestamp(v.opened_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('NEW', 'IN_PROGRESS') THEN 1 ELSE 0 END AS is_open, v.sla_penalty_aud, v.effort_hours, d.delivery_centre, d.service_line, d.sla_tier FROM silver_mlv.incident v LEFT JOIN silver_mlv.client_service d ON d.service_id = v.service_id AND d._is_current = true WHERE v._is_deleted = false;

-- IT-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_service_perf_hourly
COMMENT "Hourly service performance (15-min) with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'IT-G04')
AS
SELECT service_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(p95_response_ms), 2) AS avg_p95_response_ms, ROUND(MAX(p95_response_ms), 2) AS max_p95_response_ms, ROUND(AVG(availability_pct), 2) AS avg_availability_pct, ROUND(AVG(ticket_backlog), 2) AS avg_ticket_backlog, SUM(CASE WHEN p95_response_ms > 2500 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.service_metric GROUP BY service_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);
