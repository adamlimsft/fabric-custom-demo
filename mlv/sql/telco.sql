-- Southern Cross Telecom (Telecommunications): materialized lake views generated from config/telco.json.
-- This is a preview. nb_mdf_50_mlv_build generates and runs the same DDL against your real Bronze schema.
-- Order matters: upstream views first. Every view uses CREATE OR REPLACE, so re-running is safe.

CREATE SCHEMA IF NOT EXISTS gold_mlv;
CREATE SCHEMA IF NOT EXISTS quarantine_mlv;
CREATE SCHEMA IF NOT EXISTS silver_mlv;

-- Change Data Feed on every source so optimal refresh can choose incremental
ALTER TABLE bronze.nims_network_elements SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.nims_fault_tickets SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.oss_cell_kpi SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE bronze.dyn_customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE meta.setting SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- TC_NIMS_NETWORK_ELEMENTS: bronze.nims_network_elements -> silver_mlv.network_element
-- SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.network_element
(
  CONSTRAINT tc_ent_01 CHECK (coalesce((`element_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Network element register. SCD2 keeps status, criticality and capacity_users history."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'TC_NIMS_NETWORK_ELEMENTS')
AS
WITH src AS (
  SELECT
    try_cast((`element_id`) AS int) AS `element_id`,
    try_cast((`element_code`) AS string) AS `element_code`,
    try_cast((`element_name`) AS string) AS `element_name`,
    try_cast((`element_type`) AS string) AS `element_type`,
    try_cast((`region`) AS string) AS `region`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`capacity_users`) AS double) AS `capacity_users`,
    try_cast((`install_date`) AS date) AS `install_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.nims_network_elements
),
versions AS (
  SELECT `element_id`, `element_code`, `element_name`, `element_type`, `region`, `status`, `criticality`, `capacity_users`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `element_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `element_id`, `element_code`, `element_name`, `element_type`, `region`, `status`, `criticality`, `capacity_users`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`element_code` AS STRING), '~'), coalesce(CAST(`element_name` AS STRING), '~'), coalesce(CAST(`element_type` AS STRING), '~'), coalesce(CAST(`region` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`capacity_users` AS STRING), '~'), coalesce(CAST(`install_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `element_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `element_id`, `element_code`, `element_name`, `element_type`, `region`, `status`, `criticality`, `capacity_users`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `element_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`element_id` IS NOT NULL), false) AS `_dq_ok_tc_ent_01`,
         coalesce((`element_type` IS NULL OR `element_type` IN ('RAN_4G', 'RAN_5G', 'CORE_ROUTER', 'OLT', 'MICROWAVE')), false) AS `_dq_ok_tc_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_tc_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('ACTIVE', 'MAINTENANCE', 'FAULTY', 'DECOMMISSIONED')), false) AS `_dq_ok_tc_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_ent_02` THEN 'TC-ENT-02' END, CASE WHEN NOT `_dq_ok_tc_ent_03` THEN 'TC-ENT-03' END, CASE WHEN NOT `_dq_ok_tc_ent_04` THEN 'TC-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_ent_01` THEN 'TC-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `element_id`,
  `element_code`,
  `element_name`,
  `element_type`,
  `region`,
  `status`,
  `criticality`,
  `capacity_users`,
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

-- TC_OSS_CELL_KPI: bronze.oss_cell_kpi -> silver_mlv.cell_kpi
-- TC-RDG-03: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.cell_kpi
(
  CONSTRAINT tc_rdg_01 CHECK (coalesce((`prb_util_pct` IS NULL OR (`prb_util_pct` >= 0 AND `prb_util_pct` <= 100)), false)) ON MISMATCH DROP,
  CONSTRAINT tc_rdg_03 CHECK (coalesce((`reading_ts` IS NOT NULL), false)) ON MISMATCH DROP
)
COMMENT "Cell kpi counters every 15 minutes, four micro-batch files a day. Insert-only and idempotent."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'TC_OSS_CELL_KPI')
AS
WITH src AS (
  SELECT
    try_cast((`element_id`) AS int) AS `element_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`prb_util_pct`) AS double) AS `prb_util_pct`,
    try_cast((`active_users`) AS double) AS `active_users`,
    try_cast((`drop_rate_pct`) AS double) AS `drop_rate_pct`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.oss_cell_kpi
),
ranked AS (
  SELECT `element_id`, `reading_ts`, `prb_util_pct`, `active_users`, `drop_rate_pct`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `element_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`element_id`, r.`reading_ts`, r.`prb_util_pct`, r.`active_users`, r.`drop_rate_pct`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`prb_util_pct` IS NULL OR (`prb_util_pct` >= 0 AND `prb_util_pct` <= 100)), false) AS `_dq_ok_tc_rdg_01`,
         (c.`element_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_tc_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_tc_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `element_id` AS _rk FROM silver_mlv.network_element) ref1
    ON c.`element_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_rdg_02` THEN 'TC-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_rdg_01` THEN 'TC-RDG-01' END, CASE WHEN NOT `_dq_ok_tc_rdg_03` THEN 'TC-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `element_id`,
  `reading_ts`,
  `prb_util_pct`,
  `active_users`,
  `drop_rate_pct`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- TC_DYN_CUSTOMERS: bronze.dyn_customers -> silver_mlv.customer
-- full snapshot: keys missing from the latest load are kept with _is_deleted = true
-- PII dropped before Silver: first_name, last_name, id_document_no
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.customer
(
  CONSTRAINT tc_pty_01 CHECK (coalesce((`customer_id` IS NOT NULL), false)) ON MISMATCH FAIL
)
COMMENT "Customers - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'TC_DYN_CUSTOMERS')
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
    try_cast((`Region__c`) AS string) AS `region`,
    try_cast((`Segment__c`) AS string) AS `segment`,
    try_cast((`ElementId__c`) AS int) AS `element_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`IdDocumentNo__c`) AS string) AS `id_document_no`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.dyn_customers
),
ranked AS (
  SELECT `customer_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `region`, `segment`, `element_id`, `status`, `id_document_no`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`customer_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`region`, r.`segment`, r.`element_id`, r.`status`, r.`id_document_no`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.dyn_customers) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`customer_id` IS NOT NULL), false) AS `_dq_ok_tc_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_tc_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_tc_pty_03`,
         coalesce((`segment` IS NULL OR `segment` IN ('Consumer', 'Small Business', 'Enterprise', 'Government')), false) AS `_dq_ok_tc_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_pty_02` THEN 'TC-PTY-02' END, CASE WHEN NOT `_dq_ok_tc_pty_03` THEN 'TC-PTY-03' END, CASE WHEN NOT `_dq_ok_tc_pty_04` THEN 'TC-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_pty_01` THEN 'TC-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `customer_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `region`,
  `segment`,
  `element_id`,
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

-- TC_NIMS_FAULT_TICKETS: bronze.nims_fault_tickets -> silver_mlv.fault_ticket
-- TC-EVT-06: fail threshold 5.0% is not expressible in an MLV -> DROP + quarantine
CREATE OR REPLACE MATERIALIZED LAKE VIEW silver_mlv.fault_ticket
(
  CONSTRAINT tc_evt_01 CHECK (coalesce((`ticket_id` IS NOT NULL), false)) ON MISMATCH FAIL,
  CONSTRAINT tc_evt_03 CHECK (coalesce((resolved_ts IS NULL OR resolved_ts >= opened_ts), false)) ON MISMATCH DROP,
  CONSTRAINT tc_evt_06 CHECK (coalesce((`opened_ts` IS NOT NULL), false)) ON MISMATCH DROP,
  CONSTRAINT tc_evt_02 CHECK (`_dq_ok_tc_evt_02`) ON MISMATCH DROP
)
COMMENT "Fault tickets - incremental by last_modified; can arrive before their network element (late-arriving)."
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = 'TC_NIMS_FAULT_TICKETS')
AS
WITH src AS (
  SELECT
    try_cast((`ticket_id`) AS int) AS `ticket_id`,
    try_cast((`element_id`) AS int) AS `element_id`,
    try_cast((`fault_category`) AS string) AS `fault_category`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`opened_ts`) AS timestamp) AS `opened_ts`,
    try_cast((`resolved_ts`) AS timestamp) AS `resolved_ts`,
    try_cast((`field_cost_aud`) AS decimal(12,2)) AS `field_cost_aud`,
    try_cast((`engineer_hours`) AS double) AS `engineer_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.nims_fault_tickets
),
ranked AS (
  SELECT `ticket_id`, `element_id`, `fault_category`, `priority`, `status`, `opened_ts`, `resolved_ts`, `field_cost_aud`, `engineer_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `ticket_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`ticket_id`, r.`element_id`, r.`fault_category`, r.`priority`, r.`status`, r.`opened_ts`, r.`resolved_ts`, r.`field_cost_aud`, r.`engineer_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`ticket_id` IS NOT NULL), false) AS `_dq_ok_tc_evt_01`,
         (c.`element_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_tc_evt_02`,
         coalesce((resolved_ts IS NULL OR resolved_ts >= opened_ts), false) AS `_dq_ok_tc_evt_03`,
         coalesce((`field_cost_aud` IS NULL OR (`field_cost_aud` >= 0)), false) AS `_dq_ok_tc_evt_04`,
         coalesce((fault_category <> 'OUTAGE' OR priority = 1), false) AS `_dq_ok_tc_evt_05`,
         coalesce((`opened_ts` IS NOT NULL), false) AS `_dq_ok_tc_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `element_id` AS _rk FROM silver_mlv.network_element) ref1
    ON c.`element_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_evt_04` THEN 'TC-EVT-04' END, CASE WHEN NOT `_dq_ok_tc_evt_05` THEN 'TC-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_evt_01` THEN 'TC-EVT-01' END, CASE WHEN NOT `_dq_ok_tc_evt_02` THEN 'TC-EVT-02' END, CASE WHEN NOT `_dq_ok_tc_evt_03` THEN 'TC-EVT-03' END, CASE WHEN NOT `_dq_ok_tc_evt_06` THEN 'TC-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ticket_id`,
  `element_id`,
  `fault_category`,
  `priority`,
  `status`,
  `opened_ts`,
  `resolved_ts`,
  `field_cost_aud`,
  `engineer_hours`,
  `description`,
  `last_modified`,
  `_source_system`,
  `_object_id`,
  `_load_date`,
  `_ingested_at`,
  `_batch_id`,
  `_source_file`,
  `_dq_warnings`,
  `_dq_ok_tc_evt_02`,
  `_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, true AS `_is_current`, `_is_deleted`
FROM chk;

-- rows removed from silver_mlv.cell_kpi by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.cell_kpi
COMMENT "Quarantine for TC_OSS_CELL_KPI - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`element_id`) AS int) AS `element_id`,
    try_cast((to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')) AS timestamp) AS `reading_ts`,
    try_cast((`prb_util_pct`) AS double) AS `prb_util_pct`,
    try_cast((`active_users`) AS double) AS `active_users`,
    try_cast((`drop_rate_pct`) AS double) AS `drop_rate_pct`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.oss_cell_kpi
),
ranked AS (
  SELECT `element_id`, `reading_ts`, `prb_util_pct`, `active_users`, `drop_rate_pct`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `element_id`, `reading_ts` ORDER BY `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`element_id`, r.`reading_ts`, r.`prb_util_pct`, r.`active_users`, r.`drop_rate_pct`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`prb_util_pct` IS NULL OR (`prb_util_pct` >= 0 AND `prb_util_pct` <= 100)), false) AS `_dq_ok_tc_rdg_01`,
         (c.`element_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_tc_rdg_02`,
         coalesce((`reading_ts` IS NOT NULL), false) AS `_dq_ok_tc_rdg_03`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `element_id` AS _rk FROM silver_mlv.network_element) ref1
    ON c.`element_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_rdg_02` THEN 'TC-RDG-02' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_rdg_01` THEN 'TC-RDG-01' END, CASE WHEN NOT `_dq_ok_tc_rdg_03` THEN 'TC-RDG-03' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `element_id`,
  `reading_ts`,
  `prb_util_pct`,
  `active_users`,
  `drop_rate_pct`,
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

-- rows removed from silver_mlv.customer by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.customer
COMMENT "Quarantine for TC_DYN_CUSTOMERS - rows failing quarantine/fail rules"
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
    try_cast((`Region__c`) AS string) AS `region`,
    try_cast((`Segment__c`) AS string) AS `segment`,
    try_cast((`ElementId__c`) AS int) AS `element_id`,
    try_cast((`Status__c`) AS string) AS `status`,
    try_cast((`IdDocumentNo__c`) AS string) AS `id_document_no`,
    try_cast((`LastModifiedDate`) AS timestamp) AS `last_modified_ts`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.dyn_customers
),
ranked AS (
  SELECT `customer_id`, `first_name`, `last_name`, `email_masked`, `mobile_masked`, `birth_year`, `postcode`, `region`, `segment`, `element_id`, `status`, `id_document_no`, `last_modified_ts`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `customer_id` ORDER BY `last_modified_ts` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`customer_id`, r.`first_name`, r.`last_name`, r.`email_masked`, r.`mobile_masked`, r.`birth_year`, r.`postcode`, r.`region`, r.`segment`, r.`element_id`, r.`status`, r.`id_document_no`, r.`last_modified_ts`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, (r.`_load_date` < m._max_load_date) AS `_is_deleted`
  FROM ranked r
  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM bronze.dyn_customers) m
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`customer_id` IS NOT NULL), false) AS `_dq_ok_tc_pty_01`,
         coalesce((`email_masked` IS NULL OR CAST(`email_masked` AS STRING) RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'), false) AS `_dq_ok_tc_pty_02`,
         coalesce((`postcode` IS NULL OR CAST(`postcode` AS STRING) RLIKE '^\\d{4}$'), false) AS `_dq_ok_tc_pty_03`,
         coalesce((`segment` IS NULL OR `segment` IN ('Consumer', 'Small Business', 'Enterprise', 'Government')), false) AS `_dq_ok_tc_pty_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_pty_02` THEN 'TC-PTY-02' END, CASE WHEN NOT `_dq_ok_tc_pty_03` THEN 'TC-PTY-03' END, CASE WHEN NOT `_dq_ok_tc_pty_04` THEN 'TC-PTY-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_pty_01` THEN 'TC-PTY-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `customer_id`,
  regexp_replace(`email_masked`, '^(.)[^@]*(@.*)$', '$1***$2') AS `email_masked`,
  CASE WHEN `mobile_masked` IS NOT NULL THEN concat('*******', substring(`mobile_masked`, -3, 3)) END AS `mobile_masked`,
  CAST(year(to_date(`birth_year`)) AS INT) AS `birth_year`,
  `postcode`,
  `region`,
  `segment`,
  `element_id`,
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

-- rows removed from silver_mlv.network_element by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.network_element
COMMENT "Quarantine for TC_NIMS_NETWORK_ELEMENTS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`element_id`) AS int) AS `element_id`,
    try_cast((`element_code`) AS string) AS `element_code`,
    try_cast((`element_name`) AS string) AS `element_name`,
    try_cast((`element_type`) AS string) AS `element_type`,
    try_cast((`region`) AS string) AS `region`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`criticality`) AS int) AS `criticality`,
    try_cast((`capacity_users`) AS double) AS `capacity_users`,
    try_cast((`install_date`) AS date) AS `install_date`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.nims_network_elements
),
versions AS (
  SELECT `element_id`, `element_code`, `element_name`, `element_type`, `region`, `status`, `criticality`, `capacity_users`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) AS `_valid_from`,
         ROW_NUMBER() OVER (PARTITION BY `element_id`, coalesce(CAST(`last_modified` AS TIMESTAMP), `_ingested_at`) ORDER BY `_ingested_at` DESC) AS _rn
  FROM src
),
hashed AS (
  SELECT `element_id`, `element_code`, `element_name`, `element_type`, `region`, `status`, `criticality`, `capacity_users`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`, sha2(concat_ws('||', coalesce(CAST(`element_code` AS STRING), '~'), coalesce(CAST(`element_name` AS STRING), '~'), coalesce(CAST(`element_type` AS STRING), '~'), coalesce(CAST(`region` AS STRING), '~'), coalesce(CAST(`status` AS STRING), '~'), coalesce(CAST(`criticality` AS STRING), '~'), coalesce(CAST(`capacity_users` AS STRING), '~'), coalesce(CAST(`install_date` AS STRING), '~'), coalesce(CAST(`last_modified` AS STRING), '~')), 256) AS `_record_hash`
  FROM versions WHERE _rn = 1
),
changes AS (
  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY `element_id` ORDER BY `_valid_from`) AS _prev_hash
  FROM hashed
),
cur AS (
  SELECT `element_id`, `element_code`, `element_name`, `element_type`, `region`, `status`, `criticality`, `capacity_users`, `install_date`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`, `_valid_from`,
         LEAD(`_valid_from`) OVER (PARTITION BY `element_id` ORDER BY `_valid_from`) AS `_valid_to`,
         false AS `_is_deleted`
  FROM changes
  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`
),
flags AS (
  SELECT c.*,
         coalesce((`element_id` IS NOT NULL), false) AS `_dq_ok_tc_ent_01`,
         coalesce((`element_type` IS NULL OR `element_type` IN ('RAN_4G', 'RAN_5G', 'CORE_ROUTER', 'OLT', 'MICROWAVE')), false) AS `_dq_ok_tc_ent_02`,
         coalesce((`criticality` IS NULL OR (`criticality` >= 1 AND `criticality` <= 5)), false) AS `_dq_ok_tc_ent_03`,
         coalesce((`status` IS NULL OR `status` IN ('ACTIVE', 'MAINTENANCE', 'FAULTY', 'DECOMMISSIONED')), false) AS `_dq_ok_tc_ent_04`
  FROM cur c
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_ent_02` THEN 'TC-ENT-02' END, CASE WHEN NOT `_dq_ok_tc_ent_03` THEN 'TC-ENT-03' END, CASE WHEN NOT `_dq_ok_tc_ent_04` THEN 'TC-ENT-04' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_ent_01` THEN 'TC-ENT-01' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `element_id`,
  `element_code`,
  `element_name`,
  `element_type`,
  `region`,
  `status`,
  `criticality`,
  `capacity_users`,
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

-- rows removed from silver_mlv.fault_ticket by its DROP constraints (PII already treated)
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.fault_ticket
COMMENT "Quarantine for TC_NIMS_FAULT_TICKETS - rows failing quarantine/fail rules"
AS
WITH src AS (
  SELECT
    try_cast((`ticket_id`) AS int) AS `ticket_id`,
    try_cast((`element_id`) AS int) AS `element_id`,
    try_cast((`fault_category`) AS string) AS `fault_category`,
    try_cast((`priority`) AS int) AS `priority`,
    try_cast((`status`) AS string) AS `status`,
    try_cast((`opened_ts`) AS timestamp) AS `opened_ts`,
    try_cast((`resolved_ts`) AS timestamp) AS `resolved_ts`,
    try_cast((`field_cost_aud`) AS decimal(12,2)) AS `field_cost_aud`,
    try_cast((`engineer_hours`) AS double) AS `engineer_hours`,
    try_cast((`description`) AS string) AS `description`,
    try_cast((`last_modified`) AS timestamp) AS `last_modified`,
    `_source_system`,
    `_object_id`,
    `_load_date`,
    `_ingested_at`,
    `_batch_id`,
    `_source_file`
  FROM bronze.nims_fault_tickets
),
ranked AS (
  SELECT `ticket_id`, `element_id`, `fault_category`, `priority`, `status`, `opened_ts`, `resolved_ts`, `field_cost_aud`, `engineer_hours`, `description`, `last_modified`, `_source_system`, `_object_id`, `_load_date`, `_ingested_at`, `_batch_id`, `_source_file`,
         ROW_NUMBER() OVER (PARTITION BY `ticket_id` ORDER BY `last_modified` DESC NULLS LAST, `_ingested_at` DESC NULLS LAST) AS _rn
  FROM src
),
cur AS (
  SELECT r.`ticket_id`, r.`element_id`, r.`fault_category`, r.`priority`, r.`status`, r.`opened_ts`, r.`resolved_ts`, r.`field_cost_aud`, r.`engineer_hours`, r.`description`, r.`last_modified`, r.`_source_system`, r.`_object_id`, r.`_load_date`, r.`_ingested_at`, r.`_batch_id`, r.`_source_file`, false AS `_is_deleted`
  FROM ranked r
  WHERE r._rn = 1
),
flags AS (
  SELECT c.*,
         coalesce((`ticket_id` IS NOT NULL), false) AS `_dq_ok_tc_evt_01`,
         (c.`element_id` IS NULL OR ref1._rk IS NOT NULL) AS `_dq_ok_tc_evt_02`,
         coalesce((resolved_ts IS NULL OR resolved_ts >= opened_ts), false) AS `_dq_ok_tc_evt_03`,
         coalesce((`field_cost_aud` IS NULL OR (`field_cost_aud` >= 0)), false) AS `_dq_ok_tc_evt_04`,
         coalesce((fault_category <> 'OUTAGE' OR priority = 1), false) AS `_dq_ok_tc_evt_05`,
         coalesce((`opened_ts` IS NOT NULL), false) AS `_dq_ok_tc_evt_06`
  FROM cur c
  LEFT JOIN (SELECT DISTINCT `element_id` AS _rk FROM silver_mlv.network_element) ref1
    ON c.`element_id` = ref1._rk
),
chk AS (
  SELECT *,
         filter(array(CASE WHEN NOT `_dq_ok_tc_evt_04` THEN 'TC-EVT-04' END, CASE WHEN NOT `_dq_ok_tc_evt_05` THEN 'TC-EVT-05' END), x -> x IS NOT NULL) AS `_dq_warnings`,
         filter(array(CASE WHEN NOT `_dq_ok_tc_evt_01` THEN 'TC-EVT-01' END, CASE WHEN NOT `_dq_ok_tc_evt_02` THEN 'TC-EVT-02' END, CASE WHEN NOT `_dq_ok_tc_evt_03` THEN 'TC-EVT-03' END, CASE WHEN NOT `_dq_ok_tc_evt_06` THEN 'TC-EVT-06' END), x -> x IS NOT NULL) AS `_dq_failed`
  FROM flags
)
SELECT
  `ticket_id`,
  `element_id`,
  `fault_category`,
  `priority`,
  `status`,
  `opened_ts`,
  `resolved_ts`,
  `field_cost_aud`,
  `engineer_hours`,
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

-- one row per rule that flagged or removed rows - feeds the operations dashboard
CREATE OR REPLACE MATERIALIZED LAKE VIEW quarantine_mlv.dq_rule_summary
COMMENT "Warnings and quarantined rows per data-quality rule (MLV path)"
AS
SELECT 'TC_NIMS_NETWORK_ELEMENTS' AS object_id, 'network_element' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.network_element LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'TC_NIMS_NETWORK_ELEMENTS' AS object_id, 'network_element' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.network_element LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'TC_NIMS_FAULT_TICKETS' AS object_id, 'fault_ticket' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.fault_ticket LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'TC_NIMS_FAULT_TICKETS' AS object_id, 'fault_ticket' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.fault_ticket LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'TC_OSS_CELL_KPI' AS object_id, 'cell_kpi' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.cell_kpi LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'TC_OSS_CELL_KPI' AS object_id, 'cell_kpi' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.cell_kpi LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id
UNION ALL
SELECT 'TC_DYN_CUSTOMERS' AS object_id, 'customer' AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected
FROM silver_mlv.customer LATERAL VIEW explode(`_dq_warnings`) w AS rule_id
GROUP BY w.rule_id
UNION ALL
SELECT 'TC_DYN_CUSTOMERS' AS object_id, 'customer' AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected
FROM quarantine_mlv.customer LATERAL VIEW explode(`_failed_rules`) q AS rule_id
GROUP BY q.rule_id;

-- TC-G06: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.customer_summary
COMMENT "Customers by region and segment"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'TC-G06')
AS
SELECT region, segment, COUNT(*) AS customers, SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_customers, ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, COUNT(DISTINCT element_id) AS linked_network_elements FROM silver_mlv.customer WHERE _is_deleted = false GROUP BY region, segment;

-- TC-G02: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.dim_network_element
COMMENT "Current view of the network element register (history stays in Silver SCD2)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'TC-G02')
AS
SELECT element_id, element_code, element_name, element_type, region, status, criticality, capacity_users, install_date, CAST(floor(months_between(current_date(), install_date) / 12) AS INT) AS age_years, _valid_from AS version_valid_from FROM silver_mlv.network_element WHERE _is_current = true;

-- TC-G04: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_cell_kpi_hourly
COMMENT "Hourly cell KPI counters with threshold breaches"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'TC-G04')
AS
SELECT element_id, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, ROUND(AVG(prb_util_pct), 2) AS avg_prb_util_pct, ROUND(MAX(prb_util_pct), 2) AS max_prb_util_pct, ROUND(AVG(active_users), 2) AS avg_active_users, ROUND(AVG(drop_rate_pct), 2) AS avg_drop_rate_pct, SUM(CASE WHEN prb_util_pct > 90 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.cell_kpi GROUP BY element_id, date_trunc('HOUR', reading_ts), to_date(reading_ts), CAST(date_format(reading_ts, 'yyyyMMdd') AS INT);

-- TC-G03: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.fact_fault_ticket
COMMENT "Fault tickets with duration and network element attributes"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'TC-G03')
AS
SELECT v.ticket_id, v.element_id, v.fault_category, v.priority, v.status, v.opened_ts, v.resolved_ts, to_date(v.opened_ts) AS opened_date, CAST(date_format(v.opened_ts, 'yyyyMMdd') AS INT) AS opened_date_key, ROUND((unix_timestamp(v.resolved_ts) - unix_timestamp(v.opened_ts)) / 3600.0, 2) AS hours_to_close, CASE WHEN v.status IN ('NEW', 'IN_PROGRESS') THEN 1 ELSE 0 END AS is_open, v.field_cost_aud, v.engineer_hours, d.region, d.element_type, d.criticality FROM silver_mlv.fault_ticket v LEFT JOIN silver_mlv.network_element d ON d.element_id = v.element_id AND d._is_current = true WHERE v._is_deleted = false;

-- TC-G05: same SQL as meta.gold_transform, re-pointed at the MLV schemas
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold_mlv.network_element_health_scorecard
COMMENT "Network element risk score - fault tickets, urgent work and PRB utilisation breaches, plus revenue at risk in AUD (weights and dollar rates are illustrative: $4,000 per open fault + $600 per congested 15-min window + capacity (users) x criticality x $0.50)"
TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = 'TC-G05')
AS
WITH asat AS (SELECT MAX(to_date(opened_ts)) AS as_at FROM silver_mlv.fault_ticket), ev AS (SELECT v.element_id, COUNT(*) AS fault_tickets_12m, SUM(CASE WHEN v.status IN ('NEW', 'IN_PROGRESS') THEN 1 ELSE 0 END) AS open_fault_tickets, SUM(CASE WHEN v.fault_category = 'OUTAGE' THEN 1 ELSE 0 END) AS urgent_fault_tickets_12m, ROUND(SUM(v.field_cost_aud), 2) AS field_cost_aud_12m FROM silver_mlv.fault_ticket v CROSS JOIN asat WHERE v._is_deleted = false AND to_date(v.opened_ts) > add_months(asat.as_at, -12) GROUP BY v.element_id), rd AS (SELECT element_id, MAX(prb_util_pct) AS max_prb_util_pct, SUM(CASE WHEN prb_util_pct > 90 THEN 1 ELSE 0 END) AS breach_readings FROM silver_mlv.cell_kpi GROUP BY element_id), s AS (SELECT d.element_id, d.element_code, d.element_name, d.element_type, d.region, d.status, d.criticality, COALESCE(ev.fault_tickets_12m, 0) AS fault_tickets_12m, COALESCE(ev.open_fault_tickets, 0) AS open_fault_tickets, COALESCE(ev.urgent_fault_tickets_12m, 0) AS urgent_fault_tickets_12m, COALESCE(ev.field_cost_aud_12m, 0) AS field_cost_aud_12m, rd.max_prb_util_pct, COALESCE(rd.breach_readings, 0) AS breach_readings, d.capacity_users FROM silver_mlv.network_element d LEFT JOIN ev ON ev.element_id = d.element_id LEFT JOIN rd ON rd.element_id = d.element_id WHERE d._is_current = true) SELECT s.*, ROUND(LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_fault_tickets * 6 + s.urgent_fault_tickets_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, CASE WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_fault_tickets * 6 + s.urgent_fault_tickets_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' WHEN LEAST(100, COALESCE(s.criticality, 1) * 8 + s.open_fault_tickets * 6 + s.urgent_fault_tickets_12m * 4 + LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, ROUND(s.open_fault_tickets * 4000 + LEAST(s.breach_readings, 20) * 600 + COALESCE(s.capacity_users, 0) * COALESCE(s.criticality, 1) * 0.5, 0) AS revenue_at_risk_aud FROM s;
