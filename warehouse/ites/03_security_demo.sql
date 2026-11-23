/* Bluegum Digital Services (IT & ITeS) - Warehouse in Gold, script 3 of 3: reconcile, demo, platform features, security
   Run AFTER EXEC etl.usp_run_all. Run it one section at a time while you talk; every section is re-runnable.
   Security here applies to T-SQL queries on the warehouse. Workspace Admin, Member and Contributor roles always see
   unmasked data, so test masking and column security as a user who only has Read on the warehouse. */

-- 0. Reset the demo objects this script creates, so it can be run again
IF OBJECT_ID('sec.rls_region') IS NOT NULL DROP SECURITY POLICY sec.rls_region;
DROP FUNCTION IF EXISTS sec.fn_region_filter;
DROP FUNCTION IF EXISTS sec.fn_region_filter_by_key;
DROP TABLE IF EXISTS dw.fact_incident_before_release;
GO

-- 1. Reconciliation: Silver (lakehouse) against the warehouse - parallel-run evidence before a cut-over
SELECT r.check_name, r.silver_value, r.warehouse_value,
       CASE WHEN r.silver_value = r.warehouse_value THEN 'OK' ELSE 'CHECK' END AS result
FROM (
    SELECT 1 AS seq, 'current client services' AS check_name,
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.client_service AS s1 WHERE s1._is_current = 1) AS DECIMAL(18,2)) AS silver_value,
           CAST((SELECT COUNT(*) FROM dw.dim_client_service AS w1 WHERE w1.is_current = 1 AND w1.client_service_key <> -1) AS DECIMAL(18,2)) AS warehouse_value
    UNION ALL
    SELECT 2, 'incidents (not deleted)',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.incident AS s2 WHERE s2._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT COUNT(*) FROM dw.fact_incident AS w2) AS DECIMAL(18,2))
    UNION ALL
    SELECT 3, 'sum of sla_penalty_aud',
           CAST((SELECT SUM(s3.sla_penalty_aud) FROM lh_mdf.silver.incident AS s3 WHERE s3._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w3.sla_penalty_aud) FROM dw.fact_incident AS w3) AS DECIMAL(18,2))
    UNION ALL
    SELECT 4, 'service performance (15-min) readings',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.service_metric AS s4) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w4.readings) FROM dw.fact_service_perf_daily AS w4) AS DECIMAL(18,2))
    UNION ALL
    SELECT 5, 'readings with p95_response_ms over 2500',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.service_metric AS s5 WHERE s5.p95_response_ms > 2500) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w5.breach_readings) FROM dw.fact_service_perf_daily AS w5) AS DECIMAL(18,2))
) AS r
ORDER BY r.seq;
GO

-- 2. Run log, watermark trail and late-arriving keys
SELECT TOP 30 l.run_id, l.object_name, l.status, l.rows_staged, l.rows_inserted, l.rows_updated, l.message, l.ended_at
FROM   etl.load_log AS l ORDER BY l.ended_at DESC;
SELECT w.object_name, MAX(w.wm_value) AS current_watermark, COUNT(*) AS loads FROM etl.watermark AS w GROUP BY w.object_name;
SELECT la.object_name, COUNT(*) AS late_rows, SUM(CASE WHEN la.resolved_at IS NULL THEN 1 ELSE 0 END) AS still_unknown
FROM   etl.late_arriving AS la GROUP BY la.object_name;
GO

-- 3. Demo: incidents and sla_penalty_aud by delivery centre and financial year, using the client service version valid at the time
SELECT d.delivery_centre, dd.fy_label, f.category, COUNT(*) AS incidents, SUM(f.sla_penalty_aud) AS total_sla_penalty_aud,
       CAST(AVG(f.hours_to_close) AS DECIMAL(10,1)) AS avg_hours_to_close
FROM   dw.fact_incident AS f
INNER JOIN dw.dim_client_service AS d ON d.client_service_key = f.client_service_key
INNER JOIN dw.dim_date AS dd ON dd.date_key = f.opened_date_key
GROUP BY d.delivery_centre, dd.fy_label, f.category
ORDER BY total_sla_penalty_aud DESC;
-- Open incidents on client services with sla_tier 4 or 5
SELECT TOP 20 d.contract_ref, d.service_name, d.delivery_centre, d.sla_tier, f.incident_id, f.category, f.status, f.opened_ts
FROM   dw.fact_incident AS f INNER JOIN dw.dim_client_service AS d ON d.client_service_key = f.client_service_key
WHERE  f.is_open = 1 AND d.sla_tier >= 4
ORDER BY d.sla_tier DESC, f.opened_ts;
-- Days with the most p95_response_ms breaches (over 2500)
SELECT TOP 20 d.contract_ref, d.service_name, dd.calendar_date, f.readings, f.max_p95_response_ms, f.breach_readings
FROM   dw.fact_service_perf_daily AS f
INNER JOIN dw.dim_client_service AS d ON d.client_service_key = f.client_service_key
INNER JOIN dw.dim_date AS dd ON dd.date_key = f.reading_date_key
ORDER BY f.breach_readings DESC, f.max_p95_response_ms DESC;
-- SCD2 history: client services with more than one version
SELECT d.service_id, d.client_service_key, d.contract_ref, d.status, d.sla_tier, d.monthly_fee_aud, d.valid_from, d.valid_to, d.is_current
FROM   dw.dim_client_service AS d
WHERE  d.service_id IN (SELECT h.service_id FROM dw.dim_client_service AS h GROUP BY h.service_id HAVING COUNT(*) > 1)
ORDER BY d.service_id, d.valid_from;
GO

-- 4. Zero-copy clone: a metadata-only copy, e.g. a safety net before a release (no data is duplicated)
CREATE TABLE dw.fact_incident_before_release AS CLONE OF dw.fact_incident;
SELECT (SELECT COUNT(*) FROM dw.fact_incident_before_release AS c) AS clone_rows, (SELECT COUNT(*) FROM dw.fact_incident AS f) AS source_rows;
-- Point in time (UTC, inside the retention period):
-- CREATE TABLE dw.fact_incident_asof AS CLONE OF dw.fact_incident AT '2026-11-16T00:00:00.000';
GO

-- 5. Time travel: the fact as it was after the first successful load. The hint needs a literal, so the
--    statement is built as a string. Literal form:
--    SELECT COUNT(*) FROM dw.fact_incident OPTION (FOR TIMESTAMP AS OF '2026-11-16T00:00:00.000');
DECLARE @asof DATETIME2(3) = (SELECT MIN(l.ended_at) FROM etl.load_log AS l
                              WHERE l.object_name = 'fact_incident' AND l.status = 'SUCCESS');
DECLARE @ts VARCHAR(23) = CONVERT(VARCHAR(23), @asof, 126);
DECLARE @sql NVARCHAR(4000) = CONCAT(N'SELECT ''', @ts, N''' AS as_of_utc, COUNT(*) AS rows_then, SUM(CAST(is_open AS INT)) AS open_then ',
                                    N'FROM dw.fact_incident OPTION (FOR TIMESTAMP AS OF ''', @ts, N''');');
IF @asof IS NOT NULL EXEC sp_executesql @sql;
SELECT COUNT(*) AS rows_now, SUM(CAST(f.is_open AS INT)) AS open_now FROM dw.fact_incident AS f;
GO

-- 6. Column-level security: a role that can read the model but not sla_penalty_aud
IF DATABASE_PRINCIPAL_ID('dw_restricted_reader') IS NULL EXEC('CREATE ROLE dw_restricted_reader');
GO
GRANT SELECT ON SCHEMA::dw TO dw_restricted_reader;
DENY SELECT ON dw.fact_incident (sla_penalty_aud) TO dw_restricted_reader;
-- ALTER ROLE dw_restricted_reader ADD MEMBER [contractor@contoso.example];
-- As that user: SELECT sla_penalty_aud FROM dw.fact_incident;   -> permission denied. SELECT COUNT(*) still works.
GO

-- 7. Dynamic data masking on service_name: obfuscation for casual browsing, not a security boundary
ALTER TABLE dw.dim_client_service ALTER COLUMN service_name ADD MASKED WITH (FUNCTION = 'partial(2, "xxxx", 0)');
-- GRANT UNMASK ON dw.dim_client_service TO [analyst@contoso.example];
GO

-- 8. Row-level security on delivery_centre. Map people to delivery centres; '*' sees everything.
--    The loading identity (you now, and the pipeline's connection) must be '*' - etl.usp_run_all checks this.
DELETE FROM sec.user_region WHERE user_principal = USER_NAME();
INSERT INTO sec.user_region (user_principal, region_value) VALUES (USER_NAME(), '*');
-- INSERT INTO sec.user_region (user_principal, region_value) VALUES ('analyst@contoso.example', 'Sydney');
GO
CREATE FUNCTION sec.fn_region_filter(@region VARCHAR(100))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS allowed
       FROM   sec.user_region AS u
       WHERE  u.user_principal = USER_NAME() AND (u.region_value = @region OR u.region_value = '*');
GO
-- Facts carry no delivery_centre: they are filtered through the client service key
CREATE FUNCTION sec.fn_region_filter_by_key(@key INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS allowed
       FROM   dw.dim_client_service AS d
       INNER JOIN sec.user_region AS u ON u.user_principal = USER_NAME() AND (u.region_value = d.delivery_centre OR u.region_value = '*')
       WHERE  d.client_service_key = @key;
GO
CREATE SECURITY POLICY sec.rls_region
    ADD FILTER PREDICATE sec.fn_region_filter(delivery_centre) ON dw.dim_client_service,
    ADD FILTER PREDICATE sec.fn_region_filter_by_key(client_service_key) ON dw.fact_incident,
    ADD FILTER PREDICATE sec.fn_region_filter_by_key(client_service_key) ON dw.fact_service_perf_daily
    WITH (STATE = ON);
GO
-- Check: as '*' you see every delivery centre. Change your row to 'Sydney' and run this again.
SELECT d.delivery_centre, COUNT(*) AS incidents FROM dw.fact_incident AS f INNER JOIN dw.dim_client_service AS d ON d.client_service_key = f.client_service_key GROUP BY d.delivery_centre;
-- UPDATE sec.user_region SET region_value = 'Sydney' WHERE user_principal = USER_NAME();
-- Switch it off for loading or troubleshooting:  ALTER SECURITY POLICY sec.rls_region WITH (STATE = OFF);
GO
