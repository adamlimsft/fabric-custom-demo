/* Wattle & Co (Retail) - Warehouse in Gold, script 3 of 3: reconcile, demo, platform features, security
   Run AFTER EXEC etl.usp_run_all. Run it one section at a time while you talk; every section is re-runnable.
   Security here applies to T-SQL queries on the warehouse. Workspace Admin, Member and Contributor roles always see
   unmasked data, so test masking and column security as a user who only has Read on the warehouse. */

-- 0. Reset the demo objects this script creates, so it can be run again
IF OBJECT_ID('sec.rls_region') IS NOT NULL DROP SECURITY POLICY sec.rls_region;
DROP FUNCTION IF EXISTS sec.fn_region_filter;
DROP FUNCTION IF EXISTS sec.fn_region_filter_by_key;
DROP TABLE IF EXISTS dw.fact_stock_issue_before_release;
GO

-- 1. Reconciliation: Silver (lakehouse) against the warehouse - parallel-run evidence before a cut-over
SELECT r.check_name, r.silver_value, r.warehouse_value,
       CASE WHEN r.silver_value = r.warehouse_value THEN 'OK' ELSE 'CHECK' END AS result
FROM (
    SELECT 1 AS seq, 'current stores' AS check_name,
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.store AS s1 WHERE s1._is_current = 1) AS DECIMAL(18,2)) AS silver_value,
           CAST((SELECT COUNT(*) FROM dw.dim_store AS w1 WHERE w1.is_current = 1 AND w1.store_key <> -1) AS DECIMAL(18,2)) AS warehouse_value
    UNION ALL
    SELECT 2, 'stock issues (not deleted)',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.stock_issue AS s2 WHERE s2._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT COUNT(*) FROM dw.fact_stock_issue AS w2) AS DECIMAL(18,2))
    UNION ALL
    SELECT 3, 'sum of lost_sales_aud',
           CAST((SELECT SUM(s3.lost_sales_aud) FROM lh_mdf.silver.stock_issue AS s3 WHERE s3._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w3.lost_sales_aud) FROM dw.fact_stock_issue AS w3) AS DECIMAL(18,2))
    UNION ALL
    SELECT 4, 'store trading and availability (15-min) readings',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.store_trading AS s4) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w4.readings) FROM dw.fact_store_trading_daily AS w4) AS DECIMAL(18,2))
    UNION ALL
    SELECT 5, 'readings with out_of_stock_pct over 8',
           CAST((SELECT COUNT(*) FROM lh_mdf.silver.store_trading AS s5 WHERE s5.out_of_stock_pct > 8) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w5.breach_readings) FROM dw.fact_store_trading_daily AS w5) AS DECIMAL(18,2))
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

-- 3. Demo: stock issues and lost_sales_aud by state and financial year, using the store version valid at the time
SELECT d.state, dd.fy_label, f.issue_type, COUNT(*) AS stock_issues, SUM(f.lost_sales_aud) AS total_lost_sales_aud,
       CAST(AVG(f.hours_to_close) AS DECIMAL(10,1)) AS avg_hours_to_close
FROM   dw.fact_stock_issue AS f
INNER JOIN dw.dim_store AS d ON d.store_key = f.store_key
INNER JOIN dw.dim_date AS dd ON dd.date_key = f.opened_date_key
GROUP BY d.state, dd.fy_label, f.issue_type
ORDER BY total_lost_sales_aud DESC;
-- Open stock issues on stores with store_tier 4 or 5
SELECT TOP 20 d.store_code, d.store_name, d.state, d.store_tier, f.issue_id, f.issue_type, f.status, f.raised_ts
FROM   dw.fact_stock_issue AS f INNER JOIN dw.dim_store AS d ON d.store_key = f.store_key
WHERE  f.is_open = 1 AND d.store_tier >= 4
ORDER BY d.store_tier DESC, f.raised_ts;
-- Days with the most out_of_stock_pct breaches (over 8)
SELECT TOP 20 d.store_code, d.store_name, dd.calendar_date, f.readings, f.max_out_of_stock_pct, f.breach_readings
FROM   dw.fact_store_trading_daily AS f
INNER JOIN dw.dim_store AS d ON d.store_key = f.store_key
INNER JOIN dw.dim_date AS dd ON dd.date_key = f.reading_date_key
ORDER BY f.breach_readings DESC, f.max_out_of_stock_pct DESC;
-- SCD2 history: stores with more than one version
SELECT d.store_id, d.store_key, d.store_code, d.status, d.store_tier, d.weekly_sales_kaud, d.valid_from, d.valid_to, d.is_current
FROM   dw.dim_store AS d
WHERE  d.store_id IN (SELECT h.store_id FROM dw.dim_store AS h GROUP BY h.store_id HAVING COUNT(*) > 1)
ORDER BY d.store_id, d.valid_from;
GO

-- 4. Zero-copy clone: a metadata-only copy, e.g. a safety net before a release (no data is duplicated)
CREATE TABLE dw.fact_stock_issue_before_release AS CLONE OF dw.fact_stock_issue;
SELECT (SELECT COUNT(*) FROM dw.fact_stock_issue_before_release AS c) AS clone_rows, (SELECT COUNT(*) FROM dw.fact_stock_issue AS f) AS source_rows;
-- Point in time (UTC, inside the retention period):
-- CREATE TABLE dw.fact_stock_issue_asof AS CLONE OF dw.fact_stock_issue AT '2026-11-16T00:00:00.000';
GO

-- 5. Time travel: the fact as it was after the first successful load. The hint needs a literal, so the
--    statement is built as a string. Literal form:
--    SELECT COUNT(*) FROM dw.fact_stock_issue OPTION (FOR TIMESTAMP AS OF '2026-11-16T00:00:00.000');
DECLARE @asof DATETIME2(3) = (SELECT MIN(l.ended_at) FROM etl.load_log AS l
                              WHERE l.object_name = 'fact_stock_issue' AND l.status = 'SUCCESS');
DECLARE @ts VARCHAR(23) = CONVERT(VARCHAR(23), @asof, 126);
DECLARE @sql NVARCHAR(4000) = CONCAT(N'SELECT ''', @ts, N''' AS as_of_utc, COUNT(*) AS rows_then, SUM(CAST(is_open AS INT)) AS open_then ',
                                    N'FROM dw.fact_stock_issue OPTION (FOR TIMESTAMP AS OF ''', @ts, N''');');
IF @asof IS NOT NULL EXEC sp_executesql @sql;
SELECT COUNT(*) AS rows_now, SUM(CAST(f.is_open AS INT)) AS open_now FROM dw.fact_stock_issue AS f;
GO

-- 6. Column-level security: a role that can read the model but not lost_sales_aud
IF DATABASE_PRINCIPAL_ID('dw_restricted_reader') IS NULL EXEC('CREATE ROLE dw_restricted_reader');
GO
GRANT SELECT ON SCHEMA::dw TO dw_restricted_reader;
DENY SELECT ON dw.fact_stock_issue (lost_sales_aud) TO dw_restricted_reader;
-- ALTER ROLE dw_restricted_reader ADD MEMBER [contractor@contoso.example];
-- As that user: SELECT lost_sales_aud FROM dw.fact_stock_issue;   -> permission denied. SELECT COUNT(*) still works.
GO

-- 7. Dynamic data masking on store_name: obfuscation for casual browsing, not a security boundary
ALTER TABLE dw.dim_store ALTER COLUMN store_name ADD MASKED WITH (FUNCTION = 'partial(2, "xxxx", 0)');
-- GRANT UNMASK ON dw.dim_store TO [analyst@contoso.example];
GO

-- 8. Row-level security on state. Map people to states; '*' sees everything.
--    The loading identity (you now, and the pipeline's connection) must be '*' - etl.usp_run_all checks this.
DELETE FROM sec.user_region WHERE user_principal = USER_NAME();
INSERT INTO sec.user_region (user_principal, region_value) VALUES (USER_NAME(), '*');
-- INSERT INTO sec.user_region (user_principal, region_value) VALUES ('analyst@contoso.example', 'NSW');
GO
CREATE FUNCTION sec.fn_region_filter(@region VARCHAR(100))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS allowed
       FROM   sec.user_region AS u
       WHERE  u.user_principal = USER_NAME() AND (u.region_value = @region OR u.region_value = '*');
GO
-- Facts carry no state: they are filtered through the store key
CREATE FUNCTION sec.fn_region_filter_by_key(@key INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS allowed
       FROM   dw.dim_store AS d
       INNER JOIN sec.user_region AS u ON u.user_principal = USER_NAME() AND (u.region_value = d.state OR u.region_value = '*')
       WHERE  d.store_key = @key;
GO
CREATE SECURITY POLICY sec.rls_region
    ADD FILTER PREDICATE sec.fn_region_filter(state) ON dw.dim_store,
    ADD FILTER PREDICATE sec.fn_region_filter_by_key(store_key) ON dw.fact_stock_issue,
    ADD FILTER PREDICATE sec.fn_region_filter_by_key(store_key) ON dw.fact_store_trading_daily
    WITH (STATE = ON);
GO
-- Check: as '*' you see every state. Change your row to 'NSW' and run this again.
SELECT d.state, COUNT(*) AS stock_issues FROM dw.fact_stock_issue AS f INNER JOIN dw.dim_store AS d ON d.store_key = f.store_key GROUP BY d.state;
-- UPDATE sec.user_region SET region_value = 'NSW' WHERE user_principal = USER_NAME();
-- Switch it off for loading or troubleshooting:  ALTER SECURITY POLICY sec.rls_region WITH (STATE = OFF);
GO
