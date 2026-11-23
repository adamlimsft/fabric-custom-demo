-- Wattle & Co (Retail): materialized lake views generated from config/retail.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.ret_stores SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.ret_stock_issues SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.pos_store_trading SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.loy_loyalty_members SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- RT_LOY_LOYALTY_MEMBERS: bronze.loy_loyalty_members -> silver_mlv.loyalty_member
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- hash salt read from meta.setting (never written into the view definition)
-- PII dropped before Silver: first_name, last_name
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.loyalty_member
(
  CONSTRAINT rt_pty_01 CHECK (coalesce((`member_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Loyalty members - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'RT_LOY_LOYALTY_MEMBERS')
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `member_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`State__c`) AS string) AS `state`,
    try_cast((`Tier__c`) AS string) AS `tier`,
    try_cast((`StoreId__c`) AS int) AS `store_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`CardToken__c`) AS string) AS `card_token_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.loy_loyalty_members
),
ranked AS (
  SELECT `member_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `state`, `tier`, `store_id`, `status`, `card_token_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `member_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`member_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`state`, r.`tier`, r.`store_id`, r.`status`, r.`card_token_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.loy_loyalty_members) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`member_id` IS NOT NULL), false) AS `_dq_ok_rt_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_rt_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_rt_pty_03`,
         coalesce((`tier` IS NULL OR `tier` IN ('Gold', 'Silver', 'Bronze', 'Staff')), false) AS `_dq_ok_rt_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_pty_02` THEN 'RT-PTY-02' END, CASE WHEN NOT `_dq_ok_rt_pty_03` THEN 'RT-PTY-03' END, CASE WHEN NOT `_dq_ok_rt_pty_04` THEN 'RT-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_pty_01` THEN 'RT-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `member_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `state`,
  `tier`,
  `store_id`,
  `status`,
  CASE WHEN `card_token_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`card_token_hash` AS STRING)), 256) END AS `card_token_hash`,
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

-- RT_RET_STORES: bronze.ret_stores -> silver_mlv.store
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.store
(
  CONSTRAINT rt_ent_01 CHECK (coalesce((`store_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Store register. SCD2 keeps status, store_tier and weekly_sales_kaud history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'RT_RET_STORES')
AS
WITH src AS (
  SELECT
    try_cast((`store_id`) AS int) AS `store_id`,
    try_cast((`store_code`) AS string) AS `store_code`,
    try_cast((`store_name`) AS string) AS `store_name`,
    try_cast((`store_format`) AS string) AS `store_format`,
    try_cast((`state`) AS string) AS `state`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`store_tier`) AS int) AS `store_tier`,
    try_cast((`weekly_sales_kaud`) AS double) AS `weekly_sales_kaud`,
    try_cast((`open_date`) AS date) AS `open_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ret_stores
),
versions AS (
  SELECT `store_id`, `store_code`, `store_name`, `store_format`, `state`, `status`, `store_tier`, `weekly_sales_kaud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `store_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `store_id`, `store_code`, `store_name`, `store_format`, `state`, `status`, `store_tier`, `weekly_sales_kaud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`store_code` AS STRING), '~'), coalesce(CAST(`store_name` AS STRING), '~'), coalesce(CAST(`store_format` AS STRING), '~'), coalesce(CAST(`state` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`store_tier` AS STRING), '~'), coalesce(CAST(`weekly_sales_kaud` AS STRING), '~'), coalesce(CAST(`open_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `store_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `store_id`, `store_code`, `store_name`, `store_format`, `state`, `status`, `store_tier`, `weekly_sales_kaud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `store_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`store_id` IS NOT NULL), false) AS `_dq_ok_rt_ent_01`,
         coalesce((`store_format` IS NULL OR `store_format` IN ('SUPERMARKET', 'EXPRESS', 'MEGASTORE', 'DARK_STORE', 'PHARMACY')), false) AS `_dq_ok_rt_ent_02`,
         coalesce((`store_tier` IS NULL OR (`store_tier` >= 1 AND `store_tier` <= 5)), false) AS `_dq_ok_rt_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('TRADING', 'REFURBISHING', 'TEMP_CLOSED', 'CLOSED')), false) AS `_dq_ok_rt_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_ent_02` THEN 'RT-ENT-02' END, CASE WHEN NOT `_dq_ok_rt_ent_03` THEN 'RT-ENT-03' END, CASE WHEN NOT `_dq_ok_rt_ent_04` THEN 'RT-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_ent_01` THEN 'RT-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `store_id`,
  `store_code`,
  `store_name`,
  `store_format`,
  `state`,
  `status`,
  `store_tier`,
  `weekly_sales_kaud`,
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

-- RT_RET_STOCK_ISSUES: bronze.ret_stock_issues -> silver_mlv.stock_issue
-- RT-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.stock_issue
(
  CONSTRAINT rt_evt_01 CHECK (coalesce((`issue_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT rt_evt_03 CHECK (coalesce((resolved_ts IS NULL OR resolved_ts >= raised_ts), false)) ON MISMATCH DROP,
  CONSTRAINT rt_evt_06 CHECK (coalesce((`raised_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT rt_evt_02 CHECK (`_dq_ok_rt_evt_02`) ON MISMATCH DROP
)
COMMENT "Stock issues - incremental by last_modified; can arrive before their store (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'RT_RET_STOCK_ISSUES')
AS
WITH src AS (
  SELECT
    try_cast((`issue_id`) AS int) AS `issue_id`,
    try_cast((`store_id`) AS int) AS `store_id`,
    try_cast((`issue_type`) AS string) AS `issue_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`raised_ts`) AS timestamp) AS `raised_ts`,
    try_cast((`resolved_ts`) AS timestamp) AS `resolved_ts`,
    try_cast((`lost_sales_aud`) AS decimal(12,2)) AS `lost_sales_aud`,
    try_cast((`units_affected`) AS double) AS `units_affected`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ret_stock_issues
),
ranked AS (
  SELECT `issue_id`, `store_id`, `issue_type`, `priority`, `status`, `raised_ts`, `resolved_ts`, `lost_sales_aud`, `units_affected`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `issue_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`issue_id`, r.`store_id`, r.`issue_type`, r.`priority`, r.`status`, r.`raised_ts`, r.`resolved_ts`, r.`lost_sales_aud`, r.`units_affected`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`issue_id` IS NOT NULL), false) AS `_dq_ok_rt_evt_01`,
         (c.`store_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_rt_evt_02`,
         coalesce((resolved_ts IS NULL OR resolved_ts >= raised_ts), false) AS `_dq_ok_rt_evt_03`,
         coalesce((`lost_sales_aud` IS NULL OR (`lost_sales_aud` >= 0)), false) AS `_dq_ok_rt_evt_04`,
         coalesce((issue_type <> 'OUT_OF_STOCK' OR priority = 1), false) AS `_dq_ok_rt_evt_05`,
         coalesce((`raised_ts` IS NOT NULL), false) AS `_dq_ok_rt_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `store_id` AS _rk FROM silver_mlv.store) ref1
    ON c.`store_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_evt_04` THEN 'RT-EVT-04' END, CASE WHEN NOT `_dq_ok_rt_evt_05` THEN 'RT-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_evt_01` THEN 'RT-EVT-01' END, CASE WHEN NOT `_dq_ok_rt_evt_02` THEN 'RT-EVT-02' END, CASE WHEN NOT `_dq_ok_rt_evt_03` THEN 'RT-EVT-03' END, CASE WHEN NOT `_dq_ok_rt_evt_06` THEN 'RT-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `issue_id`,
  `store_id`,
  `issue_type`,
  `priority`,
  `status`,
  `raised_ts`,
  `resolved_ts`,
  `lost_sales_aud`,
  `units_affected`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_rt_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- RT_POS_STORE_TRADING: bronze.pos_store_trading -> silver_mlv.store_trading
-- RT-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.store_trading
(
  CONSTRAINT rt_rdg_01 CHECK (coalesce((`out_of_stock_pct` IS NULL OR (`out_of_stock_pct` >= 0 AND `out_of_stock_pct` <= 100)), false)) ON MISMATCH DROP,
  CONSTRAINT rt_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Store trading and availability (15-min) every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'RT_POS_STORE_TRADING')
AS
WITH src AS (
  SELECT
    try_cast((`store_id`) AS int) AS `store_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`out_of_stock_pct`) AS double) AS `out_of_stock_pct`,
    try_cast((`sales_aud`) AS double) AS `sales_aud`,
    try_cast((`footfall`) AS double) AS `footfall`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pos_store_trading
),
ranked AS (
  SELECT `store_id`, `reading_ts`, `out_of_stock_pct`, `sales_aud`, `footfall`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `store_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`store_id`, r.`reading_ts`, r.`out_of_stock_pct`, r.`sales_aud`, r.`footfall`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`out_of_stock_pct` IS NULL OR (`out_of_stock_pct` >= 0 AND `out_of_stock_pct` <= 100)), false) AS `_dq_ok_rt_rdg_01`,
         (c.`store_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_rt_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_rt_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `store_id` AS _rk FROM silver_mlv.store) ref1
    ON c.`store_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_rdg_02` THEN 'RT-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_rdg_01` THEN 'RT-RDG-01' END, CASE WHEN NOT `_dq_ok_rt_rdg_03` THEN 'RT-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `store_id`,
  `reading_ts`,
  `out_of_stock_pct`,
  `sales_aud`,
  `footfall`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.store by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.store
COMMENT "Quarantine for RT_RET_STORES - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`store_id`) AS int) AS `store_id`,
    try_cast((`store_code`) AS string) AS `store_code`,
    try_cast((`store_name`) AS string) AS `store_name`,
    try_cast((`store_format`) AS string) AS `store_format`,
    try_cast((`state`) AS string) AS `state`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`store_tier`) AS int) AS `store_tier`,
    try_cast((`weekly_sales_kaud`) AS double) AS `weekly_sales_kaud`,
    try_cast((`open_date`) AS date) AS `open_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ret_stores
),
versions AS (
  SELECT `store_id`, `store_code`, `store_name`, `store_format`, `state`, `status`, `store_tier`, `weekly_sales_kaud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `store_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `store_id`, `store_code`, `store_name`, `store_format`, `state`, `status`, `store_tier`, `weekly_sales_kaud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`store_code` AS STRING), '~'), coalesce(CAST(`store_name` AS STRING), '~'), coalesce(CAST(`store_format` AS STRING), '~'), coalesce(CAST(`state` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`store_tier` AS STRING), '~'), coalesce(CAST(`weekly_sales_kaud` AS STRING), '~'), coalesce(CAST(`open_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `store_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `store_id`, `store_code`, `store_name`, `store_format`, `state`, `status`, `store_tier`, `weekly_sales_kaud`, `open_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `store_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`store_id` IS NOT NULL), false) AS `_dq_ok_rt_ent_01`,
         coalesce((`store_format` IS NULL OR `store_format` IN ('SUPERMARKET', 'EXPRESS', 'MEGASTORE', 'DARK_STORE', 'PHARMACY')), false) AS `_dq_ok_rt_ent_02`,
         coalesce((`store_tier` IS NULL OR (`store_tier` >= 1 AND `store_tier` <= 5)), false) AS `_dq_ok_rt_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('TRADING', 'REFURBISHING', 'TEMP_CLOSED', 'CLOSED')), false) AS `_dq_ok_rt_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_ent_02` THEN 'RT-ENT-02' END, CASE WHEN NOT `_dq_ok_rt_ent_03` THEN 'RT-ENT-03' END, CASE WHEN NOT `_dq_ok_rt_ent_04` THEN 'RT-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_ent_01` THEN 'RT-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `store_id`,
  `store_code`,
  `store_name`,
  `store_format`,
  `state`,
  `status`,
  `store_tier`,
  `weekly_sales_kaud`,
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

-- rows removed from silver_mlv.stock_issue by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.stock_issue
COMMENT "Quarantine for RT_RET_STOCK_ISSUES - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`issue_id`) AS int) AS `issue_id`,
    try_cast((`store_id`) AS int) AS `store_id`,
    try_cast((`issue_type`) AS string) AS `issue_type`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`raised_ts`) AS timestamp) AS `raised_ts`,
    try_cast((`resolved_ts`) AS timestamp) AS `resolved_ts`,
    try_cast((`lost_sales_aud`) AS decimal(12,2)) AS `lost_sales_aud`,
    try_cast((`units_affected`) AS double) AS `units_affected`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.ret_stock_issues
),
ranked AS (
  SELECT `issue_id`, `store_id`, `issue_type`, `priority`, `status`, `raised_ts`, `resolved_ts`, `lost_sales_aud`, `units_affected`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `issue_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`issue_id`, r.`store_id`, r.`issue_type`, r.`priority`, r.`status`, r.`raised_ts`, r.`resolved_ts`, r.`lost_sales_aud`, r.`units_affected`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`issue_id` IS NOT NULL), false) AS `_dq_ok_rt_evt_01`,
         (c.`store_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_rt_evt_02`,
         coalesce((resolved_ts IS NULL OR resolved_ts >= raised_ts), false) AS `_dq_ok_rt_evt_03`,
         coalesce((`lost_sales_aud` IS NULL OR (`lost_sales_aud` >= 0)), false) AS `_dq_ok_rt_evt_04`,
         coalesce((issue_type <> 'OUT_OF_STOCK' OR priority = 1), false) AS `_dq_ok_rt_evt_05`,
         coalesce((`raised_ts` IS NOT NULL), false) AS `_dq_ok_rt_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `store_id` AS _rk FROM silver_mlv.store) ref1
    ON c.`store_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_evt_04` THEN 'RT-EVT-04' END, CASE WHEN NOT `_dq_ok_rt_evt_05` THEN 'RT-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_evt_01` THEN 'RT-EVT-01' END, CASE WHEN NOT `_dq_ok_rt_evt_02` THEN 'RT-EVT-02' END, CASE WHEN NOT `_dq_ok_rt_evt_03` THEN 'RT-EVT-03' END, CASE WHEN NOT `_dq_ok_rt_evt_06` THEN 'RT-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `issue_id`,
  `store_id`,
  `issue_type`,
  `priority`,
  `status`,
  `raised_ts`,
  `resolved_ts`,
  `lost_sales_aud`,
  `units_affected`,
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

-- rows removed from silver_mlv.store_trading by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.store_trading
COMMENT "Quarantine for RT_POS_STORE_TRADING - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`store_id`) AS int) AS `store_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`out_of_stock_pct`) AS double) AS `out_of_stock_pct`,
    try_cast((`sales_aud`) AS double) AS `sales_aud`,
    try_cast((`footfall`) AS double) AS `footfall`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.pos_store_trading
),
ranked AS (
  SELECT `store_id`, `reading_ts`, `out_of_stock_pct`, `sales_aud`, `footfall`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `store_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`store_id`, r.`reading_ts`, r.`out_of_stock_pct`, r.`sales_aud`, r.`footfall`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`out_of_stock_pct` IS NULL OR (`out_of_stock_pct` >= 0 AND `out_of_stock_pct` <= 100)), false) AS `_dq_ok_rt_rdg_01`,
         (c.`store_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_rt_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_rt_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `store_id` AS _rk FROM silver_mlv.store) ref1
    ON c.`store_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_rdg_02` THEN 'RT-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_rdg_01` THEN 'RT-RDG-01' END, CASE WHEN NOT `_dq_ok_rt_rdg_03` THEN 'RT-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `store_id`,
  `reading_ts`,
  `out_of_stock_pct`,
  `sales_aud`,
  `footfall`,
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

-- rows removed from silver_mlv.loyalty_member by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.loyalty_member
COMMENT "Quarantine for RT_LOY_LOYALTY_MEMBERS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`Id`) AS string) AS `member_id`,
    try_cast((`FirstName`) AS string) AS `first_name`,
    try_cast((`LastName`) AS string) AS `last_name`,
    try_cast((`Email`) AS string) AS `email_masked`,
    try_cast((`MobilePhone`) AS string) AS `mobile_masked`,
    `Birthdate` AS `birth_year`,
    try_cast((`MailingPostalCode`) AS string) AS `postcode`,
    try_cast((`State__c`) AS string) AS `state`,
    try_cast((`Tier__c`) AS string) AS `tier`,
    try_cast((`StoreId__c`) AS int) AS `store_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`CardToken__c`) AS string) AS `card_token_hash`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.loy_loyalty_members
),
ranked AS (
  SELECT `member_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `state`, `tier`, `store_id`, `status`, `card_token_hash`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `member_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`member_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`state`, r.`tier`, r.`store_id`, r.`status`, r.`card_token_hash`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.loy_loyalty_members) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`member_id` IS NOT NULL), false) AS `_dq_ok_rt_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_rt_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_rt_pty_03`,
         coalesce((`tier` IS NULL OR `tier` IN ('Gold', 'Silver', 'Bronze', 'Staff')), false) AS `_dq_ok_rt_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_rt_pty_02` THEN 'RT-PTY-02' END, CASE WHEN NOT `_dq_ok_rt_pty_03` THEN 'RT-PTY-03' END, CASE WHEN NOT `_dq_ok_rt_pty_04` THEN 'RT-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_rt_pty_01` THEN 'RT-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `member_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `state`,
  `tier`,
  `store_id`,
  `status`,
  CASE WHEN `card_token_hash` IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST(`card_token_hash` AS STRING)), 256) END AS `card_token_hash`,
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
SELECT 'RT_RET_STORES' AS object_id, 'store' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.store LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'RT_RET_STORES' AS object_id, 'store' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.store LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'RT_RET_STOCK_ISSUES' AS object_id, 'stock_issue' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.stock_issue LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'RT_RET_STOCK_ISSUES' AS object_id, 'stock_issue' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.stock_issue LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'RT_POS_STORE_TRADING' AS object_id, 'store_trading' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.store_trading LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'RT_POS_STORE_TRADING' AS object_id, 'store_trading' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.store_trading LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'RT_LOY_LOYALTY_MEMBERS' AS object_id, 'loyalty_member' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.loyalty_member LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'RT_LOY_LOYALTY_MEMBERS' AS object_id, 'loyalty_member' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.loyalty_member LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- RT-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_store
COMMENT "Current view of the store register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'RT-G02')
AS
SELECT store_id, store_code, store_name, store_format, state, status, store_tier, weekly_sales_kaud, open_date, CAST(floor(months_between(current_date(), open_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.store WHERE _is_current = true;

-- RT-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_stock_issue
COMMENT "Stock issues with duration and store attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'RT-G03')
AS
SELECT v.issue_id, v.store_id, v.issue_type, v.priority, v.status, v.raised_ts, v.resolved_ts, to_date(v.raised_ts) AS opened_date, CAST(date_format(v.raised_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.resolved_ts) - unix_timestamp(v.raised_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('RAISED', 'ACTIONING') THEN 1 ELSE 0 END AS is_open, v.lost_sales_aud, v.units_affected, d.state, d.store_format, d.store_tier FROM silver_mlv.stock_issue v LEFT JOIN silver_mlv.store d ON d.store_id = v.store_id AND d._is_current = true WHERE v._is_deleted = false;

-- RT-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_store_trading_hourly
COMMENT "Hourly store trading and availability (15-min) with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'RT-G04')
AS
SELECT store_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(out_of_stock_pct), 2) AS avg_out_of_stock_pct, ROUND(MAX(out_of_stock_pct), 2) AS max_out_of_stock_pct, ROUND(AVG(sales_aud), 2) AS avg_sales_aud, ROUND(AVG(footfall), 2) AS avg_footfall, SUM(CASE WHEN out_of_stock_pct > 8 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.store_trading GROUP BY store_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- RT-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.loyalty_member_summary
COMMENT "Loyalty members by state and tier"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'RT-G06')
AS
SELECT state, tier, COUNT(*) AS loyalty_members, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_loyalty_members, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT store_id) AS linked_stores FROM silver_mlv.loyalty_member WHERE _is_deleted = false GROUP BY state, tier;

-- RT-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.store_health_scorecard
COMMENT "Store risk score - stock issues, urgent work and out-of-stock lines breaches, plus lost sales at risk in AUD (weights and dollar rates are illustrative: $1,800 per open stock issue + $900 per 15-min window above 8% out-of-stock + weekly sales (k) x store tier x $1)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'RT-G05')
AS
WITH asat AS (SELECT MAX(to_date(raised_ts)) AS as_at FROM silver_mlv.stock_issue), ev AS (SELECT v.store_id, COUNT(*) AS stock_issues_12m, SUM(CASE WHEN v.status IN ('RAISED', 'ACTIONING') THEN 1 ELSE 0 END) AS open_stock_issues, SUM(CASE WHEN v.issue_type = 'OUT_OF_STOCK' THEN 1 ELSE 0 END) AS urgent_stock_issues_12m, ROUND(SUM(v.lost_sales_aud), 2) AS lost_sales_aud_12m FROM silver_mlv.stock_issue v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.raised_ts) > add_months(asat.as_at, -12) GROUP BY v.store_id), rd AS (SELECT store_id, MAX(out_of_stock_pct) AS max_out_of_stock_pct, SUM(CASE WHEN out_of_stock_pct > 8 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.store_trading GROUP BY store_id), s AS (SELECT d.store_id, d.store_code, d.store_name, d.store_format, d.state, d.status, d.store_tier, COALESCE(ev.stock_issues_12m, 0) AS stock_issues_12m, COALESCE(ev.open_stock_issues, 0) AS open_stock_issues, COALESCE(ev.urgent_stock_issues_12m, 0) AS urgent_stock_issues_12m, COALESCE(ev.lost_sales_aud_12m, 0) AS lost_sales_aud_12m, rd.max_out_of_stock_pct, COALESCE(rd.breach_readings, 0) AS breach_readings, d.weekly_sales_kaud FROM silver_mlv.store d LEFT JOIN ev ON ev.store_id = d.store_id LEFT JOIN rd ON rd.store_id = d.store_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.store_tier, 1) * 8 + s.open_stock_issues * 6 + s.urgent_stock_issues_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.store_tier, 1) * 8 + s.open_stock_issues * 6 + s.urgent_stock_issues_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.store_tier, 1) * 8 + s.open_stock_issues * 6 + s.urgent_stock_issues_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_stock_issues * 1800 + LEAST(s.breach_readings, 20) * 900 + COALESCE(s.weekly_sales_kaud, 0) * COALESCE(s.store_tier, 1) * 1, 0) AS lost_sales_at_risk_aud FROM s;
