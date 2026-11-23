-- Lyrebird Apps (Digital natives): materialized lake views generated from config/digital.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.app_tenants SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.app_support_tickets SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.tel_usage_metrics SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.hub_app_users SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- DN_HUB_APP_USERS: bronze.hub_app_users -> silver_mlv.app_user
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- hash salt read from meta.setting (never written into the view definition)
-- PII dropped before Silver: first_name, last_name
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.app_user
(
  CONSTRAINT dn_pty_01 CHECK (coalesce((`user_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "App users - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'DN_HUB_APP_USERS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `user_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`CloudRegion__c`) AS string) AS `cloud_region`,
    try_cast((`UserRole__c`) AS string) AS `user_role`,
    try_cast((`TenantId__c`) AS int) AS `tenant_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`IpAddress__c`) AS string) AS `ip_address_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.hub_app_users
),
ranked AS (
  SELECT `user_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `cloud_region`, `user_role`, `tenant_id`, `status`, `ip_address_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`user_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`cloud_region`, r.`user_role`, r.`tenant_id`, r.`status`, r.`ip_address_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.hub_app_users) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`user_id` IS NOT NULL), false) AS `_dq_ok_dn_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_dn_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_dn_pty_03`,
         coalesce((`user_role` IS NULL OR `user_role` IN ('Admin', 'Member', 'Guest', 'Billing')), false) AS `_dq_ok_dn_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_pty_02` THEN 'DN-PTY-02' END, CASE WHEN NOT `_dq_ok_dn_pty_03` THEN 'DN-PTY-03' END, CASE WHEN NOT `_dq_ok_dn_pty_04` THEN 'DN-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_pty_01` THEN 'DN-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `user_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `cloud_region`,
  `user_role`,
  `tenant_id`,
  `status`,
  CASE WHEN `ip_address_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`ip_address_hash` AS STRING)), 256) END AS `ip_address_hash`,
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

-- DN_APP_TENANTS: bronze.app_tenants -> silver_mlv.tenant
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.tenant
(
  CONSTRAINT dn_ent_01 CHECK (coalesce((`tenant_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Tenant register. SCD2 keeps status, account_tier and seats history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'DN_APP_TENANTS')
AS
WITH src AS (
  SELECT
    try_cast((`tenant_id`) AS int) AS `tenant_id`,
    try_cast((`tenant_slug`) AS string) AS `tenant_slug`,
    try_cast((`tenant_name`) AS string) AS `tenant_name`,
    try_cast((`plan`) AS string) AS `plan`,
    try_cast((`cloud_region`) AS string) AS `cloud_region`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`account_tier`) AS int) AS `account_tier`,
    try_cast((`seats`) AS double) AS `seats`,
    try_cast((`signup_date`) AS date) AS `signup_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.app_tenants
),
versions AS (
  SELECT `tenant_id`, `tenant_slug`, `tenant_name`, `plan`, `cloud_region`, `status`, `account_tier`, `seats`, `signup_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `tenant_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `tenant_id`, `tenant_slug`, `tenant_name`, `plan`, `cloud_region`, `status`, `account_tier`, `seats`, `signup_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`tenant_slug` AS STRING), '~'), coalesce(CAST(`tenant_name` AS STRING), '~'), coalesce(CAST(`plan` AS STRING), '~'), coalesce(CAST(`cloud_region` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`account_tier` AS STRING), '~'), coalesce(CAST(`seats` AS STRING), '~'), coalesce(CAST(`signup_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `tenant_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `tenant_id`, `tenant_slug`, `tenant_name`, `plan`, `cloud_region`, `status`, `account_tier`, `seats`, `signup_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `tenant_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`tenant_id` IS NOT NULL), false) AS `_dq_ok_dn_ent_01`,
         coalesce((`plan` IS NULL OR `plan` IN ('FREE', 'STARTER', 'PRO', 'BUSINESS', 'ENTERPRISE')), false) AS `_dq_ok_dn_ent_02`,
         coalesce((`account_tier` IS NULL OR (`account_tier` >= 1 AND `account_tier` <= 5)), false) AS `_dq_ok_dn_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('ACTIVE', 'TRIAL', 'PAST_DUE', 'CHURNED')), false) AS `_dq_ok_dn_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_ent_02` THEN 'DN-ENT-02' END, CASE WHEN NOT `_dq_ok_dn_ent_03` THEN 'DN-ENT-03' END, CASE WHEN NOT `_dq_ok_dn_ent_04` THEN 'DN-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_ent_01` THEN 'DN-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `tenant_id`,
  `tenant_slug`,
  `tenant_name`,
  `plan`,
  `cloud_region`,
  `status`,
  `account_tier`,
  `seats`,
  `signup_date`,
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

-- DN_APP_SUPPORT_TICKETS: bronze.app_support_tickets -> silver_mlv.support_ticket
-- DN-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.support_ticket
(
  CONSTRAINT dn_evt_01 CHECK (coalesce((`ticket_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT dn_evt_03 CHECK (coalesce((solved_ts IS NULL OR solved_ts >= opened_ts), false)) ON MISMATCH DROP,
  CONSTRAINT dn_evt_06 CHECK (coalesce((`opened_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT dn_evt_02 CHECK (`_dq_ok_dn_evt_02`) ON MISMATCH DROP
)
COMMENT "Support tickets - incremental by last_modified; can arrive before their tenant (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'DN_APP_SUPPORT_TICKETS')
AS
WITH src AS (
  SELECT
    try_cast((`ticket_id`) AS int) AS `ticket_id`,
    try_cast((`tenant_id`) AS int) AS `tenant_id`,
    try_cast((`ticket_type`) AS string) AS `ticket_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`opened_ts`) AS timestamp) AS `opened_ts`,
    try_cast((`solved_ts`) AS timestamp) AS `solved_ts`,
    try_cast((`credit_aud`) AS decimal(12,2)) AS `credit_aud`,
    try_cast((`agent_hours`) AS double) AS `agent_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.app_support_tickets
),
ranked AS (
  SELECT `ticket_id`, `tenant_id`, `ticket_type`, `priority`, `status`, `opened_ts`, `solved_ts`, `credit_aud`, `agent_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `ticket_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`ticket_id`, r.`tenant_id`, r.`ticket_type`, r.`priority`, r.`status`, r.`opened_ts`, r.`solved_ts`, r.`credit_aud`, r.`agent_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`ticket_id` IS NOT NULL), false) AS `_dq_ok_dn_evt_01`,
         (c.`tenant_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_dn_evt_02`,
         coalesce((solved_ts IS NULL OR solved_ts >= opened_ts), false) AS `_dq_ok_dn_evt_03`,
         coalesce((`credit_aud` IS NULL OR (`credit_aud` >= 0)), false) AS `_dq_ok_dn_evt_04`,
         coalesce((ticket_type <> 'OUTAGE' OR priority = 1), false) AS `_dq_ok_dn_evt_05`,
         coalesce((`opened_ts` IS NOT NULL), false) AS `_dq_ok_dn_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `tenant_id` AS _rk FROM silver_mlv.tenant) ref1
    ON c.`tenant_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_evt_04` THEN 'DN-EVT-04' END, CASE WHEN NOT `_dq_ok_dn_evt_05` THEN 'DN-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_evt_01` THEN 'DN-EVT-01' END, CASE WHEN NOT `_dq_ok_dn_evt_02` THEN 'DN-EVT-02' END, CASE WHEN NOT `_dq_ok_dn_evt_03` THEN 'DN-EVT-03' END, CASE WHEN NOT `_dq_ok_dn_evt_06` THEN 'DN-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ticket_id`,
  `tenant_id`,
  `ticket_type`,
  `priority`,
  `status`,
  `opened_ts`,
  `solved_ts`,
  `credit_aud`,
  `agent_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_dn_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- DN_TEL_USAGE_METRICS: bronze.tel_usage_metrics -> silver_mlv.usage_metric
-- DN-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.usage_metric
(
  CONSTRAINT dn_rdg_01 CHECK (coalesce((`p95_latency_ms` IS NULL OR (`p95_latency_ms` >= 0 AND `p95_latency_ms` <= 60000)), false)) ON MISMATCH DROP,
  CONSTRAINT dn_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Product usage metrics every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'DN_TEL_USAGE_METRICS')
AS
WITH src AS (
  SELECT
    try_cast((`tenant_id`) AS int) AS `tenant_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`p95_latency_ms`) AS double) AS `p95_latency_ms`,
    try_cast((`active_users`) AS double) AS `active_users`,
    try_cast((`error_rate_pct`) AS double) AS `error_rate_pct`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.tel_usage_metrics
),
ranked AS (
  SELECT `tenant_id`, `reading_ts`, `p95_latency_ms`, `active_users`, `error_rate_pct`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `tenant_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`tenant_id`, r.`reading_ts`, r.`p95_latency_ms`, r.`active_users`, r.`error_rate_pct`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`p95_latency_ms` IS NULL OR (`p95_latency_ms` >= 0 AND `p95_latency_ms` <= 60000)), false) AS `_dq_ok_dn_rdg_01`,
         (c.`tenant_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_dn_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_dn_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `tenant_id` AS _rk FROM silver_mlv.tenant) ref1
    ON c.`tenant_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_rdg_02` THEN 'DN-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_rdg_01` THEN 'DN-RDG-01' END, CASE WHEN NOT `_dq_ok_dn_rdg_03` THEN 'DN-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `tenant_id`,
  `reading_ts`,
  `p95_latency_ms`,
  `active_users`,
  `error_rate_pct`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.app_user by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.app_user
COMMENT "Quarantine for DN_HUB_APP_USERS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `user_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`CloudRegion__c`) AS string) AS `cloud_region`,
    try_cast((`UserRole__c`) AS string) AS `user_role`,
    try_cast((`TenantId__c`) AS int) AS `tenant_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`IpAddress__c`) AS string) AS `ip_address_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.hub_app_users
),
ranked AS (
  SELECT `user_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `cloud_region`, `user_role`, `tenant_id`, `status`, `ip_address_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `user_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`user_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`cloud_region`, r.`user_role`, r.`tenant_id`, r.`status`, r.`ip_address_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.hub_app_users) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`user_id` IS NOT NULL), false) AS `_dq_ok_dn_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_dn_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_dn_pty_03`,
         coalesce((`user_role` IS NULL OR `user_role` IN ('Admin', 'Member', 'Guest', 'Billing')), false) AS `_dq_ok_dn_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_pty_02` THEN 'DN-PTY-02' END, CASE WHEN NOT `_dq_ok_dn_pty_03` THEN 'DN-PTY-03' END, CASE WHEN NOT `_dq_ok_dn_pty_04` THEN 'DN-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_pty_01` THEN 'DN-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `user_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `cloud_region`,
  `user_role`,
  `tenant_id`,
  `status`,
  CASE WHEN `ip_address_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`ip_address_hash` AS STRING)), 256) END AS `ip_address_hash`,
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

-- rows removed from silver_mlv.tenant by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.tenant
COMMENT "Quarantine for DN_APP_TENANTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`tenant_id`) AS int) AS `tenant_id`,
    try_cast((`tenant_slug`) AS string) AS `tenant_slug`,
    try_cast((`tenant_name`) AS string) AS `tenant_name`,
    try_cast((`plan`) AS string) AS `plan`,
    try_cast((`cloud_region`) AS string) AS `cloud_region`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`account_tier`) AS int) AS `account_tier`,
    try_cast((`seats`) AS double) AS `seats`,
    try_cast((`signup_date`) AS date) AS `signup_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.app_tenants
),
versions AS (
  SELECT `tenant_id`, `tenant_slug`, `tenant_name`, `plan`, `cloud_region`, `status`, `account_tier`, `seats`, `signup_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `tenant_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `tenant_id`, `tenant_slug`, `tenant_name`, `plan`, `cloud_region`, `status`, `account_tier`, `seats`, `signup_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`tenant_slug` AS STRING), '~'), coalesce(CAST(`tenant_name` AS STRING), '~'), coalesce(CAST(`plan` AS STRING), '~'), coalesce(CAST(`cloud_region` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`account_tier` AS STRING), '~'), coalesce(CAST(`seats` AS STRING), '~'), coalesce(CAST(`signup_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `tenant_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `tenant_id`, `tenant_slug`, `tenant_name`, `plan`, `cloud_region`, `status`, `account_tier`, `seats`, `signup_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `tenant_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`tenant_id` IS NOT NULL), false) AS `_dq_ok_dn_ent_01`,
         coalesce((`plan` IS NULL OR `plan` IN ('FREE', 'STARTER', 'PRO', 'BUSINESS', 'ENTERPRISE')), false) AS `_dq_ok_dn_ent_02`,
         coalesce((`account_tier` IS NULL OR (`account_tier` >= 1 AND `account_tier` <= 5)), false) AS `_dq_ok_dn_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('ACTIVE', 'TRIAL', 'PAST_DUE', 'CHURNED')), false) AS `_dq_ok_dn_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_ent_02` THEN 'DN-ENT-02' END, CASE WHEN NOT `_dq_ok_dn_ent_03` THEN 'DN-ENT-03' END, CASE WHEN NOT `_dq_ok_dn_ent_04` THEN 'DN-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_ent_01` THEN 'DN-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `tenant_id`,
  `tenant_slug`,
  `tenant_name`,
  `plan`,
  `cloud_region`,
  `status`,
  `account_tier`,
  `seats`,
  `signup_date`,
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

-- rows removed from silver_mlv.support_ticket by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.support_ticket
COMMENT "Quarantine for DN_APP_SUPPORT_TICKETS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`ticket_id`) AS int) AS `ticket_id`,
    try_cast((`tenant_id`) AS int) AS `tenant_id`,
    try_cast((`ticket_type`) AS string) AS `ticket_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`opened_ts`) AS timestamp) AS `opened_ts`,
    try_cast((`solved_ts`) AS timestamp) AS `solved_ts`,
    try_cast((`credit_aud`) AS decimal(12,2)) AS `credit_aud`,
    try_cast((`agent_hours`) AS double) AS `agent_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.app_support_tickets
),
ranked AS (
  SELECT `ticket_id`, `tenant_id`, `ticket_type`, `priority`, `status`, `opened_ts`, `solved_ts`, `credit_aud`, `agent_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `ticket_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`ticket_id`, r.`tenant_id`, r.`ticket_type`, r.`priority`, r.`status`, r.`opened_ts`, r.`solved_ts`, r.`credit_aud`, r.`agent_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`ticket_id` IS NOT NULL), false) AS `_dq_ok_dn_evt_01`,
         (c.`tenant_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_dn_evt_02`,
         coalesce((solved_ts IS NULL OR solved_ts >= opened_ts), false) AS `_dq_ok_dn_evt_03`,
         coalesce((`credit_aud` IS NULL OR (`credit_aud` >= 0)), false) AS `_dq_ok_dn_evt_04`,
         coalesce((ticket_type <> 'OUTAGE' OR priority = 1), false) AS `_dq_ok_dn_evt_05`,
         coalesce((`opened_ts` IS NOT NULL), false) AS `_dq_ok_dn_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `tenant_id` AS _rk FROM silver_mlv.tenant) ref1
    ON c.`tenant_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_evt_04` THEN 'DN-EVT-04' END, CASE WHEN NOT `_dq_ok_dn_evt_05` THEN 'DN-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_evt_01` THEN 'DN-EVT-01' END, CASE WHEN NOT `_dq_ok_dn_evt_02` THEN 'DN-EVT-02' END, CASE WHEN NOT `_dq_ok_dn_evt_03` THEN 'DN-EVT-03' END, CASE WHEN NOT `_dq_ok_dn_evt_06` THEN 'DN-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ticket_id`,
  `tenant_id`,
  `ticket_type`,
  `priority`,
  `status`,
  `opened_ts`,
  `solved_ts`,
  `credit_aud`,
  `agent_hours`,
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

-- rows removed from silver_mlv.usage_metric by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.usage_metric
COMMENT "Quarantine for DN_TEL_USAGE_METRICS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`tenant_id`) AS int) AS `tenant_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`p95_latency_ms`) AS double) AS `p95_latency_ms`,
    try_cast((`active_users`) AS double) AS `active_users`,
    try_cast((`error_rate_pct`) AS double) AS `error_rate_pct`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.tel_usage_metrics
),
ranked AS (
  SELECT `tenant_id`, `reading_ts`, `p95_latency_ms`, `active_users`, `error_rate_pct`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `tenant_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`tenant_id`, r.`reading_ts`, r.`p95_latency_ms`, r.`active_users`, r.`error_rate_pct`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`p95_latency_ms` IS NULL OR (`p95_latency_ms` >= 0 AND `p95_latency_ms` <= 60000)), false) AS `_dq_ok_dn_rdg_01`,
         (c.`tenant_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_dn_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_dn_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `tenant_id` AS _rk FROM silver_mlv.tenant) ref1
    ON c.`tenant_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_dn_rdg_02` THEN 'DN-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_dn_rdg_01` THEN 'DN-RDG-01' END, CASE WHEN NOT `_dq_ok_dn_rdg_03` THEN 'DN-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `tenant_id`,
  `reading_ts`,
  `p95_latency_ms`,
  `active_users`,
  `error_rate_pct`,
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
SELECT 'DN_APP_TENANTS' AS object_id, 'tenant' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.tenant LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'DN_APP_TENANTS' AS object_id, 'tenant' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.tenant LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'DN_APP_SUPPORT_TICKETS' AS object_id, 'support_ticket' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.support_ticket LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'DN_APP_SUPPORT_TICKETS' AS object_id, 'support_ticket' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.support_ticket LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'DN_TEL_USAGE_METRICS' AS object_id, 'usage_metric' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.usage_metric LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'DN_TEL_USAGE_METRICS' AS object_id, 'usage_metric' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.usage_metric LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'DN_HUB_APP_USERS' AS object_id, 'app_user' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.app_user LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'DN_HUB_APP_USERS' AS object_id, 'app_user' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.app_user LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- DN-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.app_user_summary
COMMENT "App users by cloud region and user_role"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'DN-G06')
AS
SELECT cloud_region, user_role, COUNT(*) AS app_users, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_app_users, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT tenant_id) AS linked_tenants FROM silver_mlv.app_user WHERE _is_deleted = false GROUP BY cloud_region, user_role;

-- DN-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_tenant
COMMENT "Current view of the tenant register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'DN-G02')
AS
SELECT tenant_id, tenant_slug, tenant_name, plan, cloud_region, status, account_tier, seats, signup_date, CAST(floor(months_between(current_date(), signup_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.tenant WHERE _is_current = true;

-- DN-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_support_ticket
COMMENT "Support tickets with duration and tenant attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'DN-G03')
AS
SELECT v.ticket_id, v.tenant_id, v.ticket_type, v.priority, v.status, v.opened_ts, v.solved_ts, to_date(v.opened_ts) AS opened_date, CAST(date_format(v.opened_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.solved_ts) - unix_timestamp(v.opened_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('OPEN', 'PENDING') THEN 1 ELSE 0 END AS is_open, v.credit_aud, v.agent_hours, d.cloud_region, d.plan, d.account_tier FROM silver_mlv.support_ticket v LEFT JOIN silver_mlv.tenant d ON d.tenant_id = v.tenant_id AND d._is_current = true WHERE v._is_deleted = false;

-- DN-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_usage_hourly
COMMENT "Hourly product usage metrics with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'DN-G04')
AS
SELECT tenant_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(p95_latency_ms), 2) AS avg_p95_latency_ms, ROUND(MAX(p95_latency_ms), 2) AS max_p95_latency_ms, ROUND(AVG(active_users), 2) AS avg_active_users, ROUND(AVG(error_rate_pct), 2) AS avg_error_rate_pct, SUM(CASE WHEN p95_latency_ms > 800 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.usage_metric GROUP BY tenant_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- DN-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.tenant_health_scorecard
COMMENT "Tenant risk score - support tickets, urgent work and p95 API latency breaches, plus ARR at risk in AUD (weights and dollar rates are illustrative: $1,500 per open ticket + $400 per slow 15-min window + seats x account tier x $12)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'DN-G05')
AS
WITH asat AS (SELECT MAX(to_date(opened_ts)) AS as_at FROM silver_mlv.support_ticket), ev AS (SELECT v.tenant_id, COUNT(*) AS support_tickets_12m, SUM(CASE WHEN v.status IN ('OPEN', 'PENDING') THEN 1 ELSE 0 END) AS open_support_tickets, SUM(CASE WHEN v.ticket_type = 'OUTAGE' THEN 1 ELSE 0 END) AS urgent_support_tickets_12m, ROUND(SUM(v.credit_aud), 2) AS credit_aud_12m FROM silver_mlv.support_ticket v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.opened_ts) > add_months(asat.as_at, -12) GROUP BY v.tenant_id), rd AS (SELECT tenant_id, MAX(p95_latency_ms) AS max_p95_latency_ms, SUM(CASE WHEN p95_latency_ms > 800 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.usage_metric GROUP BY tenant_id), s AS (SELECT d.tenant_id, d.tenant_slug, d.tenant_name, d.plan, d.cloud_region, d.status, d.account_tier, COALESCE(ev.support_tickets_12m, 0) AS support_tickets_12m, COALESCE(ev.open_support_tickets, 0) AS open_support_tickets, COALESCE(ev.urgent_support_tickets_12m, 0) AS urgent_support_tickets_12m, COALESCE(ev.credit_aud_12m, 0) AS credit_aud_12m, rd.max_p95_latency_ms, COALESCE(rd.breach_readings, 0) AS breach_readings, d.seats FROM silver_mlv.tenant d LEFT JOIN ev ON ev.tenant_id = d.tenant_id LEFT JOIN rd ON rd.tenant_id = d.tenant_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.account_tier, 1) * 8 + s.open_support_tickets * 6 + s.urgent_support_tickets_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.account_tier, 1) * 8 + s.open_support_tickets * 6 + s.urgent_support_tickets_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.account_tier, 1) * 8 + s.open_support_tickets * 6 + s.urgent_support_tickets_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_support_tickets * 1500 + LEAST(s.breach_readings, 20) * 400 + COALESCE(s.seats, 0) * COALESCE(s.account_tier, 1) * 12, 0) AS arr_at_risk_aud FROM s;
