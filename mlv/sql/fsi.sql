-- Harbour Mutual Bank (Financial services): materialized lake views generated from config/fsi.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.core_accounts SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.core_risk_cases SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.pay_txn_monitoring SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.sfdc_customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- FS_CORE_ACCOUNTS: bronze.core_accounts -> silver_mlv.account
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.account
(
  CONSTRAINT fs_ent_01 CHECK (coalesce((`account_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Account register. SCD2 keeps status, risk_grade and balance_aud history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'FS_CORE_ACCOUNTS')
AS
WITH src AS (
  SELECT
    try_cast((`account_id`) AS int) AS `account_id`,
    try_cast((`account_number`) AS string) AS `account_number`,
    try_cast((`account_name`) AS string) AS `account_name`,
    try_cast((`product_type`) AS string) AS `product_type`,
    try_cast((`state`) AS string) AS `state`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`risk_grade`) AS int) AS `risk_grade`,
    try_cast((`balance_aud`) AS double) AS `balance_aud`,
    try_cast((`open_date`) AS date) AS `open_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.core_accounts
),
versions AS (
  SELECT `account_id`, `account_number`, `account_name`, `product_type`, `state`, `status`, `risk_grade`, `balance_aud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `account_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `account_id`, `account_number`, `account_name`, `product_type`, `state`, `status`, `risk_grade`, `balance_aud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`account_number` AS STRING), '~'), coalesce(CAST(`account_name` AS STRING), '~'), coalesce(CAST(`product_type` AS STRING), '~'), coalesce(CAST(`state` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`risk_grade` AS STRING), '~'), coalesce(CAST(`balance_aud` AS STRING), '~'), coalesce(CAST(`open_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `account_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `account_id`, `account_number`, `account_name`, `product_type`, `state`, `status`, `risk_grade`, `balance_aud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `account_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`account_id` IS NOT NULL), false) AS `_dq_ok_fs_ent_01`,
         coalesce((`product_type` IS NULL OR `product_type` IN ('TRANSACTION', 'SAVINGS', 'PERSONAL_LOAN', 'HOME_LOAN', 'CREDIT_CARD')), false) AS `_dq_ok_fs_ent_02`,
         coalesce((`risk_grade` IS NULL OR (`risk_grade` >= 1 AND `risk_grade` <= 5)), false) AS `_dq_ok_fs_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('PERFORMING', 'DORMANT', 'ARREARS', 'CLOSED')), false) AS `_dq_ok_fs_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_ent_02` THEN 'FS-ENT-02' END, CASE WHEN NOT `_dq_ok_fs_ent_03` THEN 'FS-ENT-03' END, CASE WHEN NOT `_dq_ok_fs_ent_04` THEN 'FS-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_ent_01` THEN 'FS-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `account_id`,
  `account_number`,
  `account_name`,
  `product_type`,
  `state`,
  `status`,
  `risk_grade`,
  `balance_aud`,
  `open_date`,
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

-- FS_SFDC_CUSTOMERS: bronze.sfdc_customers -> silver_mlv.customer
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- PII dropped before Silver: first_name, last_name, tfn
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.customer
(
  CONSTRAINT fs_pty_01 CHECK (coalesce((`customer_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Customers - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'FS_SFDC_CUSTOMERS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `customer_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`State__c`) AS string) AS `state`,
    try_cast((`Segment__c`) AS string) AS `segment`,
    try_cast((`AccountId__c`) AS int) AS `account_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`Tfn__c`) AS string) AS `tfn`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.sfdc_customers
),
ranked AS (
  SELECT `customer_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `state`, `segment`, `account_id`, `status`, `tfn`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`customer_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`state`, r.`segment`, r.`account_id`, r.`status`, r.`tfn`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.sfdc_customers) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`customer_id` IS NOT NULL), false) AS `_dq_ok_fs_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_fs_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_fs_pty_03`,
         coalesce((`segment` IS NULL OR `segment` IN ('Retail', 'Premium', 'Business', 'Agribusiness')), false) AS `_dq_ok_fs_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_pty_02` THEN 'FS-PTY-02' END, CASE WHEN NOT `_dq_ok_fs_pty_03` THEN 'FS-PTY-03' END, CASE WHEN NOT `_dq_ok_fs_pty_04` THEN 'FS-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_pty_01` THEN 'FS-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `customer_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `state`,
  `segment`,
  `account_id`,
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

-- FS_CORE_RISK_CASES: bronze.core_risk_cases -> silver_mlv.risk_case
-- FS-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.risk_case
(
  CONSTRAINT fs_evt_01 CHECK (coalesce((`case_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT fs_evt_03 CHECK (coalesce((closed_ts IS NULL OR closed_ts >= raised_ts), false)) ON MISMATCH DROP,
  CONSTRAINT fs_evt_06 CHECK (coalesce((`raised_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT fs_evt_02 CHECK (`_dq_ok_fs_evt_02`) ON MISMATCH DROP
)
COMMENT "Risk cases - incremental by last_modified; can arrive before their account (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'FS_CORE_RISK_CASES')
AS
WITH src AS (
  SELECT
    try_cast((`case_id`) AS int) AS `case_id`,
    try_cast((`account_id`) AS int) AS `account_id`,
    try_cast((`case_type`) AS string) AS `case_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`raised_ts`) AS timestamp) AS `raised_ts`,
    try_cast((`closed_ts`) AS timestamp) AS `closed_ts`,
    try_cast((`case_amount_aud`) AS decimal(12,2)) AS `case_amount_aud`,
    try_cast((`analyst_hours`) AS double) AS `analyst_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.core_risk_cases
),
ranked AS (
  SELECT `case_id`, `account_id`, `case_type`, `priority`, `status`, `raised_ts`, `closed_ts`, `case_amount_aud`, `analyst_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `case_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`case_id`, r.`account_id`, r.`case_type`, r.`priority`, r.`status`, r.`raised_ts`, r.`closed_ts`, r.`case_amount_aud`, r.`analyst_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`case_id` IS NOT NULL), false) AS `_dq_ok_fs_evt_01`,
         (c.`account_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_fs_evt_02`,
         coalesce((closed_ts IS NULL OR closed_ts >= raised_ts), false) AS `_dq_ok_fs_evt_03`,
         coalesce((`case_amount_aud` IS NULL OR (`case_amount_aud` >= 0)), false) AS `_dq_ok_fs_evt_04`,
         coalesce((case_type <> 'ACCOUNT_TAKEOVER' OR priority = 1), false) AS `_dq_ok_fs_evt_05`,
         coalesce((`raised_ts` IS NOT NULL), false) AS `_dq_ok_fs_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `account_id` AS _rk FROM silver_mlv.account) ref1
    ON c.`account_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_evt_04` THEN 'FS-EVT-04' END, CASE WHEN NOT `_dq_ok_fs_evt_05` THEN 'FS-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_evt_01` THEN 'FS-EVT-01' END, CASE WHEN NOT `_dq_ok_fs_evt_02` THEN 'FS-EVT-02' END, CASE WHEN NOT `_dq_ok_fs_evt_03` THEN 'FS-EVT-03' END, CASE WHEN NOT `_dq_ok_fs_evt_06` THEN 'FS-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `case_id`,
  `account_id`,
  `case_type`,
  `priority`,
  `status`,
  `raised_ts`,
  `closed_ts`,
  `case_amount_aud`,
  `analyst_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_fs_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- FS_PAY_TXN_MONITORING: bronze.pay_txn_monitoring -> silver_mlv.txn_monitor
-- FS-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.txn_monitor
(
  CONSTRAINT fs_rdg_01 CHECK (coalesce((`decline_pct` IS NULL OR (`decline_pct` >= 0 AND `decline_pct` <= 100)), false)) ON MISMATCH DROP,
  CONSTRAINT fs_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Account transaction monitoring (15-min windows) every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'FS_PAY_TXN_MONITORING')
AS
WITH src AS (
  SELECT
    try_cast((`account_id`) AS int) AS `account_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`decline_pct`) AS double) AS `decline_pct`,
    try_cast((`txn_value_aud`) AS double) AS `txn_value_aud`,
    try_cast((`txn_count`) AS double) AS `txn_count`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pay_txn_monitoring
),
ranked AS (
  SELECT `account_id`, `reading_ts`, `decline_pct`, `txn_value_aud`, `txn_count`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `account_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`account_id`, r.`reading_ts`, r.`decline_pct`, r.`txn_value_aud`, r.`txn_count`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`decline_pct` IS NULL OR (`decline_pct` >= 0 AND `decline_pct` <= 100)), false) AS `_dq_ok_fs_rdg_01`,
         (c.`account_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_fs_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_fs_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `account_id` AS _rk FROM silver_mlv.account) ref1
    ON c.`account_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_rdg_02` THEN 'FS-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_rdg_01` THEN 'FS-RDG-01' END, CASE WHEN NOT `_dq_ok_fs_rdg_03` THEN 'FS-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `account_id`,
  `reading_ts`,
  `decline_pct`,
  `txn_value_aud`,
  `txn_count`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.account by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.account
COMMENT "Quarantine for FS_CORE_ACCOUNTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`account_id`) AS int) AS `account_id`,
    try_cast((`account_number`) AS string) AS `account_number`,
    try_cast((`account_name`) AS string) AS `account_name`,
    try_cast((`product_type`) AS string) AS `product_type`,
    try_cast((`state`) AS string) AS `state`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`risk_grade`) AS int) AS `risk_grade`,
    try_cast((`balance_aud`) AS double) AS `balance_aud`,
    try_cast((`open_date`) AS date) AS `open_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.core_accounts
),
versions AS (
  SELECT `account_id`, `account_number`, `account_name`, `product_type`, `state`, `status`, `risk_grade`, `balance_aud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `account_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `account_id`, `account_number`, `account_name`, `product_type`, `state`, `status`, `risk_grade`, `balance_aud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`account_number` AS STRING), '~'), coalesce(CAST(`account_name` AS STRING), '~'), coalesce(CAST(`product_type` AS STRING), '~'), coalesce(CAST(`state` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`risk_grade` AS STRING), '~'), coalesce(CAST(`balance_aud` AS STRING), '~'), coalesce(CAST(`open_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `account_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `account_id`, `account_number`, `account_name`, `product_type`, `state`, `status`, `risk_grade`, `balance_aud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `account_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`account_id` IS NOT NULL), false) AS `_dq_ok_fs_ent_01`,
         coalesce((`product_type` IS NULL OR `product_type` IN ('TRANSACTION', 'SAVINGS', 'PERSONAL_LOAN', 'HOME_LOAN', 'CREDIT_CARD')), false) AS `_dq_ok_fs_ent_02`,
         coalesce((`risk_grade` IS NULL OR (`risk_grade` >= 1 AND `risk_grade` <= 5)), false) AS `_dq_ok_fs_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('PERFORMING', 'DORMANT', 'ARREARS', 'CLOSED')), false) AS `_dq_ok_fs_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_ent_02` THEN 'FS-ENT-02' END, CASE WHEN NOT `_dq_ok_fs_ent_03` THEN 'FS-ENT-03' END, CASE WHEN NOT `_dq_ok_fs_ent_04` THEN 'FS-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_ent_01` THEN 'FS-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `account_id`,
  `account_number`,
  `account_name`,
  `product_type`,
  `state`,
  `status`,
  `risk_grade`,
  `balance_aud`,
  `open_date`,
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

-- rows removed from silver_mlv.customer by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.customer
COMMENT "Quarantine for FS_SFDC_CUSTOMERS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `customer_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`State__c`) AS string) AS `state`,
    try_cast((`Segment__c`) AS string) AS `segment`,
    try_cast((`AccountId__c`) AS int) AS `account_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`Tfn__c`) AS string) AS `tfn`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.sfdc_customers
),
ranked AS (
  SELECT `customer_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `state`, `segment`, `account_id`, `status`, `tfn`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`customer_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`state`, r.`segment`, r.`account_id`, r.`status`, r.`tfn`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.sfdc_customers) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`customer_id` IS NOT NULL), false) AS `_dq_ok_fs_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_fs_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_fs_pty_03`,
         coalesce((`segment` IS NULL OR `segment` IN ('Retail', 'Premium', 'Business', 'Agribusiness')), false) AS `_dq_ok_fs_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_pty_02` THEN 'FS-PTY-02' END, CASE WHEN NOT `_dq_ok_fs_pty_03` THEN 'FS-PTY-03' END, CASE WHEN NOT `_dq_ok_fs_pty_04` THEN 'FS-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_pty_01` THEN 'FS-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `customer_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `state`,
  `segment`,
  `account_id`,
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

-- rows removed from silver_mlv.risk_case by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.risk_case
COMMENT "Quarantine for FS_CORE_RISK_CASES - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`case_id`) AS int) AS `case_id`,
    try_cast((`account_id`) AS int) AS `account_id`,
    try_cast((`case_type`) AS string) AS `case_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`raised_ts`) AS timestamp) AS `raised_ts`,
    try_cast((`closed_ts`) AS timestamp) AS `closed_ts`,
    try_cast((`case_amount_aud`) AS decimal(12,2)) AS `case_amount_aud`,
    try_cast((`analyst_hours`) AS double) AS `analyst_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.core_risk_cases
),
ranked AS (
  SELECT `case_id`, `account_id`, `case_type`, `priority`, `status`, `raised_ts`, `closed_ts`, `case_amount_aud`, `analyst_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `case_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`case_id`, r.`account_id`, r.`case_type`, r.`priority`, r.`status`, r.`raised_ts`, r.`closed_ts`, r.`case_amount_aud`, r.`analyst_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`case_id` IS NOT NULL), false) AS `_dq_ok_fs_evt_01`,
         (c.`account_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_fs_evt_02`,
         coalesce((closed_ts IS NULL OR closed_ts >= raised_ts), false) AS `_dq_ok_fs_evt_03`,
         coalesce((`case_amount_aud` IS NULL OR (`case_amount_aud` >= 0)), false) AS `_dq_ok_fs_evt_04`,
         coalesce((case_type <> 'ACCOUNT_TAKEOVER' OR priority = 1), false) AS `_dq_ok_fs_evt_05`,
         coalesce((`raised_ts` IS NOT NULL), false) AS `_dq_ok_fs_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `account_id` AS _rk FROM silver_mlv.account) ref1
    ON c.`account_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_evt_04` THEN 'FS-EVT-04' END, CASE WHEN NOT `_dq_ok_fs_evt_05` THEN 'FS-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_evt_01` THEN 'FS-EVT-01' END, CASE WHEN NOT `_dq_ok_fs_evt_02` THEN 'FS-EVT-02' END, CASE WHEN NOT `_dq_ok_fs_evt_03` THEN 'FS-EVT-03' END, CASE WHEN NOT `_dq_ok_fs_evt_06` THEN 'FS-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `case_id`,
  `account_id`,
  `case_type`,
  `priority`,
  `status`,
  `raised_ts`,
  `closed_ts`,
  `case_amount_aud`,
  `analyst_hours`,
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

-- rows removed from silver_mlv.txn_monitor by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.txn_monitor
COMMENT "Quarantine for FS_PAY_TXN_MONITORING - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`account_id`) AS int) AS `account_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`decline_pct`) AS double) AS `decline_pct`,
    try_cast((`txn_value_aud`) AS double) AS `txn_value_aud`,
    try_cast((`txn_count`) AS double) AS `txn_count`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pay_txn_monitoring
),
ranked AS (
  SELECT `account_id`, `reading_ts`, `decline_pct`, `txn_value_aud`, `txn_count`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `account_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`account_id`, r.`reading_ts`, r.`decline_pct`, r.`txn_value_aud`, r.`txn_count`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`decline_pct` IS NULL OR (`decline_pct` >= 0 AND `decline_pct` <= 100)), false) AS `_dq_ok_fs_rdg_01`,
         (c.`account_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_fs_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_fs_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `account_id` AS _rk FROM silver_mlv.account) ref1
    ON c.`account_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_fs_rdg_02` THEN 'FS-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_fs_rdg_01` THEN 'FS-RDG-01' END, CASE WHEN NOT `_dq_ok_fs_rdg_03` THEN 'FS-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `account_id`,
  `reading_ts`,
  `decline_pct`,
  `txn_value_aud`,
  `txn_count`,
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
SELECT 'FS_CORE_ACCOUNTS' AS object_id, 'account' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.account LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'FS_CORE_ACCOUNTS' AS object_id, 'account' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.account LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'FS_CORE_RISK_CASES' AS object_id, 'risk_case' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.risk_case LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'FS_CORE_RISK_CASES' AS object_id, 'risk_case' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.risk_case LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'FS_PAY_TXN_MONITORING' AS object_id, 'txn_monitor' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.txn_monitor LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'FS_PAY_TXN_MONITORING' AS object_id, 'txn_monitor' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.txn_monitor LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'FS_SFDC_CUSTOMERS' AS object_id, 'customer' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.customer LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'FS_SFDC_CUSTOMERS' AS object_id, 'customer' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.customer LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- FS-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.account_health_scorecard
COMMENT "Account risk score - risk cases, urgent work and declined transaction rate breaches, plus expected loss in AUD (weights and dollar rates are illustrative: $3,500 per open case + $250 per suspicious 15-min window + balance x risk grade x 0.2%)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'FS-G05')
AS
WITH asat AS (SELECT MAX(to_date(raised_ts)) AS as_at FROM silver_mlv.risk_case), ev AS (SELECT v.account_id, COUNT(*) AS risk_cases_12m, SUM(CASE WHEN v.status IN ('OPEN', 'INVESTIGATING') THEN 1 ELSE 0 END) AS open_risk_cases, SUM(CASE WHEN v.case_type = 'ACCOUNT_TAKEOVER' THEN 1 ELSE 0 END) AS urgent_risk_cases_12m, ROUND(SUM(v.case_amount_aud), 2) AS case_amount_aud_12m FROM silver_mlv.risk_case v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.raised_ts) > add_months(asat.as_at, -12) GROUP BY v.account_id), rd AS (SELECT account_id, MAX(decline_pct) AS max_decline_pct, SUM(CASE WHEN decline_pct > 30 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.txn_monitor GROUP BY account_id), s AS (SELECT d.account_id, d.account_number, d.account_name, d.product_type, d.state, d.status, d.risk_grade, COALESCE(ev.risk_cases_12m, 0) AS risk_cases_12m, COALESCE(ev.open_risk_cases, 0) AS open_risk_cases, COALESCE(ev.urgent_risk_cases_12m, 0) AS urgent_risk_cases_12m, COALESCE(ev.case_amount_aud_12m, 0) AS case_amount_aud_12m, rd.max_decline_pct, COALESCE(rd.breach_readings, 0) AS breach_readings, d.balance_aud FROM silver_mlv.account d LEFT JOIN ev ON ev.account_id = d.account_id LEFT JOIN rd ON rd.account_id = d.account_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.risk_grade, 1) * 8 + s.open_risk_cases * 6 + s.urgent_risk_cases_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.risk_grade, 1) * 8 + s.open_risk_cases * 6 + s.urgent_risk_cases_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.risk_grade, 1) * 8 + s.open_risk_cases * 6 + s.urgent_risk_cases_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_risk_cases * 3500 + LEAST(s.breach_readings, 20) * 250 + COALESCE(s.balance_aud, 0) * COALESCE(s.risk_grade, 1) * 0.002, 0) AS expected_loss_aud FROM s;

-- FS-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.customer_summary
COMMENT "Customers by state and segment"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'FS-G06')
AS
SELECT state, segment, COUNT(*) AS customers, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_customers, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT account_id) AS linked_accounts FROM silver_mlv.customer WHERE _is_deleted = false GROUP BY state, segment;

-- FS-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_account
COMMENT "Current view of the account register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'FS-G02')
AS
SELECT account_id, account_number, account_name, product_type, state, status, risk_grade, balance_aud, open_date, CAST(floor(months_between(current_date(), open_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.account WHERE _is_current = true;

-- FS-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_risk_case
COMMENT "Risk cases with duration and account attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'FS-G03')
AS
SELECT v.case_id, v.account_id, v.case_type, v.priority, v.status, v.raised_ts, v.closed_ts, to_date(v.raised_ts) AS opened_date, CAST(date_format(v.raised_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.closed_ts) - unix_timestamp(v.raised_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('OPEN', 'INVESTIGATING') THEN 1 ELSE 0 END AS is_open, v.case_amount_aud, v.analyst_hours, d.state, d.product_type, d.risk_grade FROM silver_mlv.risk_case v LEFT JOIN silver_mlv.account d ON d.account_id = v.account_id AND d._is_current = true WHERE v._is_deleted = false;

-- FS-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_txn_monitor_hourly
COMMENT "Hourly account transaction monitoring (15-min windows) with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'FS-G04')
AS
SELECT account_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(decline_pct), 2) AS avg_decline_pct, ROUND(MAX(decline_pct), 2) AS max_decline_pct, ROUND(AVG(txn_value_aud), 2) AS avg_txn_value_aud, ROUND(AVG(txn_count), 2) AS avg_txn_count, SUM(CASE WHEN decline_pct > 30 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.txn_monitor GROUP BY account_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);
