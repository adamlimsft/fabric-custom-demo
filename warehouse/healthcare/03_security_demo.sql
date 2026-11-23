/* Coastal Health Network (Healthcare) - Warehouse in Gold, script 3 of 3: reconcile, demo, platform features, security
   Run AFTER EXEC etl.usp_run_all. Run it one section at a time while you talk; every section is re-runnable.
   Security here applies to T-SQL queries on the warehouse. Workspace Admin, Member and Contributor roles always see
   unmasked data, so test masking and column security as a user who only has Read on the warehouse. */

-- 0. Reset the demo objects this script creates, so it can be run again
IF OBJECT_ID('sec.rls_region') IS NOT NULL DROP SECURITY POLICY sec.rls_region;
DROP FUNCTION IF EXISTS sec.fn_region_filter;
DROP FUNCTION IF EXISTS sec.fn_region_filter_by_key;
DROP TABLE IF EXISTS dw.fact_admission_before_release;
GO

-- 1. Reconciliation: Silver (lakehouse) against the warehouse - parallel-run evidence before a cut-over
SELECT r.check_name, r.silver_value, r.warehouse_value,
       CASE WHEN r.silver_value = r.warehouse_value THEN 'OK' ELSE 'CHECK' END AS result
FROM (
    SELECT 1 AS seq, 'current wards' AS check_name,
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.ward AS s1 WHERE s1._is_current = 1) AS DECIMAL(18,2)) AS silver_value,
           CAST((SELECT COUNT(*) FROM dw.dim_ward AS w1 WHERE w1.is_current = 1 AND w1.ward_key <> -1) AS DECIMAL(18,2)) AS warehouse_value
    UNION ALL
    SELECT 2, 'admissions (not deleted)',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.admission AS s2 WHERE s2._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT COUNT(*) FROM dw.fact_admission AS w2) AS DECIMAL(18,2))
    UNION ALL
    SELECT 3, 'sum of episode_cost_aud',
           CAST((SELECT SUM(s3.episode_cost_aud) FROM lh_mdf.silver.admission AS s3 WHERE s3._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w3.episode_cost_aud) FROM dw.fact_admission AS w3) AS DECIMAL(18,2))
    UNION ALL
    SELECT 4, 'ward occupancy (15-min) readings',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.ward_occupancy AS s4) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w4.readings) FROM dw.fact_ward_occupancy_daily AS w4) AS DECIMAL(18,2))
    UNION ALL
    SELECT 5, 'readings with occupancy_pct over 95',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.ward_occupancy AS s5 WHERE s5.occupancy_pct > 95) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w5.breach_readings) FROM dw.fact_ward_occupancy_daily AS w5) AS DECIMAL(18,2))
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

-- 3. Demo: admissions and episode_cost_aud by hospital and financial year, using the ward version valid at the time
SELECT d.hospital, dd.fy_label, f.admission_type, COUNT(*) AS admissions, SUM(f.episode_cost_aud) AS total_episode_cost_aud,
       CAST(AVG(f.hours_to_close) AS DECIMAL(10,1)) AS avg_hours_to_close
FROM   dw.fact_admission AS f
INNER JOIN dw.dim_ward AS d ON d.ward_key = f.ward_key
INNER JOIN dw.dim_date AS dd ON dd.date_key = f.opened_date_key
GROUP BY d.hospital, dd.fy_label, f.admission_type
ORDER BY total_episode_cost_aud DESC;
-- Open admissions on wards with acuity_level 4 or 5
SELECT TOP 20 d.ward_code, d.ward_name, d.hospital, d.acuity_level, f.admission_id, f.admission_type, f.status, f.admit_ts
FROM   dw.fact_admission AS f INNER JOIN dw.dim_ward AS d ON d.ward_key = f.ward_key
WHERE  f.is_open = 1 AND d.acuity_level >= 4
ORDER BY d.acuity_level DESC, f.admit_ts;
-- Days with the most occupancy_pct breaches (over 95)
SELECT TOP 20 d.ward_code, d.ward_name, dd.calendar_date, f.readings, f.max_occupancy_pct, f.breach_readings
FROM   dw.fact_ward_occupancy_daily AS f
INNER JOIN dw.dim_ward AS d ON d.ward_key = f.ward_key
INNER JOIN dw.dim_date AS dd ON dd.date_key = f.reading_date_key
ORDER BY f.breach_readings DESC, f.max_occupancy_pct DESC;
-- SCD2 history: wards with more than one version
SELECT d.ward_id, d.ward_key, d.ward_code, d.status, d.acuity_level, d.bed_count, d.valid_from, d.valid_to, d.is_current
FROM   dw.dim_ward AS d
WHERE  d.ward_id IN (SELECT h.ward_id FROM dw.dim_ward AS h GROUP BY h.ward_id HAVING COUNT(*) > 1)
ORDER BY d.ward_id, d.valid_from;
GO

-- 4. Zero-copy clone: a metadata-only copy, e.g. a safety net before a release (no data is duplicated)
CREATE TABLE dw.fact_admission_before_release AS CLONE OF dw.fact_admission;
SELECT (SELECT COUNT(*) FROM dw.fact_admission_before_release AS c) AS clone_rows, (SELECT COUNT(*) FROM dw.fact_admission AS f) AS source_rows;
-- Point in time (UTC, inside the retention period):
-- CREATE TABLE dw.fact_admission_asof AS CLONE OF dw.fact_admission AT '2026-11-16T00:00:00.000';
GO

-- 5. Time travel: the fact as it was after the first successful load. The hint needs a literal, so the
--    statement is built as a string. Literal form:
--    SELECT COUNT(*) FROM dw.fact_admission OPTION (FOR TIMESTAMP AS OF '2026-11-16T00:00:00.000');
DECLARE @asof DATETIME2(3) = (SELECT MIN(l.ended_at) FROM etl.load_log AS l
                              WHERE l.object_name = 'fact_admission' AND l.status = 'SUCCESS');
DECLARE @ts VARCHAR(23) = CONVERT(VARCHAR(23), @asof, 126);
DECLARE @sql NVARCHAR(4000) = CONCAT(N'SELECT ''', @ts, N''' AS as_of_utc, COUNT(*) AS rows_then, SUM(CAST(is_open AS INT)) AS open_then ',
                                    N'FROM dw.fact_admission OPTION (FOR TIMESTAMP AS OF ''', @ts, N''');');
IF @asof IS NOT NULL EXEC sp_executesql @sql;
SELECT COUNT(*) AS rows_now, SUM(CAST(f.is_open AS INT)) AS open_now FROM dw.fact_admission AS f;
GO

-- 6. Column-level security: a role that can read the model but not episode_cost_aud
IF DATABASE_PRINCIPAL_ID('dw_restricted_reader') IS NULL EXEC('CREATE ROLE dw_restricted_reader');
GO
GRANT SELECT ON SCHEMA::dw TO dw_restricted_reader;
DENY SELECT ON dw.fact_admission (episode_cost_aud) TO dw_restricted_reader;
-- ALTER ROLE dw_restricted_reader ADD MEMBER [contractor@contoso.example];
-- As that user: SELECT episode_cost_aud FROM dw.fact_admission;   -> permission denied. SELECT COUNT(*) still works.
GO

-- 7. Dynamic data masking on ward_name: obfuscation for casual browsing, not a security boundary
ALTER TABLE dw.dim_ward ALTER COLUMN ward_name ADD MASKED WITH (FUNCTION = 'partial(2, "xxxx", 0)');
-- GRANT UNMASK ON dw.dim_ward TO [analyst@contoso.example];
GO

-- 8. Row-level security on hospital. Map people to hospitals; '*' sees everything.
--    The loading identity (you now, and the pipeline's connection) must be '*' - etl.usp_run_all checks this.
DELETE FROM sec.user_region WHERE user_principal = USER_NAME();
INSERT INTO sec.user_region (user_principal, region_value) VALUES (USER_NAME(), '*');
-- INSERT INTO sec.user_region (user_principal, region_value) VALUES ('analyst@contoso.example', 'Gosford');
GO
CREATE FUNCTION sec.fn_region_filter(@region VARCHAR(100))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS allowed
       FROM   sec.user_region AS u
       WHERE  u.user_principal = USER_NAME() AND (u.region_value = @region OR u.region_value = '*');
GO
-- Facts carry no hospital: they are filtered through the ward key
CREATE FUNCTION sec.fn_region_filter_by_key(@key INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS allowed
       FROM   dw.dim_ward AS d
       INNER JOIN sec.user_region AS u ON u.user_principal = USER_NAME() AND (u.region_value = d.hospital OR u.region_value = '*')
       WHERE  d.ward_key = @key;
GO
CREATE SECURITY POLICY sec.rls_region
    ADD FILTER PREDICATE sec.fn_region_filter(hospital) ON dw.dim_ward,
    ADD FILTER PREDICATE sec.fn_region_filter_by_key(ward_key) ON dw.fact_admission,
    ADD FILTER PREDICATE sec.fn_region_filter_by_key(ward_key) ON dw.fact_ward_occupancy_daily
    WITH (STATE = ON);
GO
-- Check: as '*' you see every hospital. Change your row to 'Gosford' and run this again.
SELECT d.hospital, COUNT(*) AS admissions FROM dw.fact_admission AS f INNER JOIN dw.dim_ward AS d ON d.ward_key = f.ward_key GROUP BY d.hospital;
-- UPDATE sec.user_region SET region_value = 'Gosford' WHERE user_principal = USER_NAME();
-- Switch it off for loading or troubleshooting:  ALTER SECURITY POLICY sec.rls_region WITH (STATE = OFF);
GO
