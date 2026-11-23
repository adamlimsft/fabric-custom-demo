/* Wattle & Co (Retail) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.store ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_store: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_store @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_store';
        -- 1. Stage every Silver version of each store touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_store;
        INSERT INTO stg.dim_store (store_id, store_code, store_name, store_format, state, status, store_tier, weekly_sales_kaud, open_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.store_id, sv.store_code, sv.store_name, sv.store_format, sv.state, sv.status, sv.store_tier, sv.weekly_sales_kaud, sv.open_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.store AS sv
        WHERE  sv.store_id IN (SELECT svn.store_id FROM lh_mdf.silver.store AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_store;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(store_key), 0) FROM dw.dim_store);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, store_tier or weekly_sales_kaud changed).
        --    A new member's first version is valid from 1900-01-01 so older stock issues still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_store (store_key, store_id, store_code, store_name, store_format, state, status, store_tier, weekly_sales_kaud, open_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.store_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.store_id, v.store_code, v.store_name, v.store_format, v.state, v.status, v.store_tier, v.weekly_sales_kaud, v.open_date,
               CASE WHEN c.store_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.store_id, n.store_code, n.store_name, n.store_format, n.state, n.status, n.store_tier, n.weekly_sales_kaud, n.open_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.store_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.store_tier) OVER (PARTITION BY n.store_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_store_tier,
                        LAG(n.weekly_sales_kaud) OVER (PARTITION BY n.store_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_weekly_sales_kaud,
                        ROW_NUMBER() OVER (PARTITION BY n.store_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_store AS n
                LEFT JOIN (SELECT h.store_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_store AS h
                           WHERE h.store_key <> -1 GROUP BY h.store_id) AS l ON l.store_id = n.store_id
                WHERE  l.store_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_store AS c ON c.store_id = v.store_id AND c.is_current = 1 AND c.store_key <> -1
        WHERE (v.rn = 1 AND c.store_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.store_tier <> c.store_tier OR (v.store_tier IS NULL AND c.store_tier IS NOT NULL) OR (v.store_tier IS NOT NULL AND c.store_tier IS NULL)) OR (v.weekly_sales_kaud <> c.weekly_sales_kaud OR (v.weekly_sales_kaud IS NULL AND c.weekly_sales_kaud IS NOT NULL) OR (v.weekly_sales_kaud IS NOT NULL AND c.weekly_sales_kaud IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.store_tier <> v.prev_store_tier OR (v.store_tier IS NULL AND v.prev_store_tier IS NOT NULL) OR (v.store_tier IS NOT NULL AND v.prev_store_tier IS NULL)) OR (v.weekly_sales_kaud <> v.prev_weekly_sales_kaud OR (v.weekly_sales_kaud IS NULL AND v.prev_weekly_sales_kaud IS NOT NULL) OR (v.weekly_sales_kaud IS NOT NULL AND v.prev_weekly_sales_kaud IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_store AS d
        INNER JOIN (
            SELECT h.store_key, LEAD(h.valid_from) OVER (PARTITION BY h.store_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_store AS h
            WHERE  h.store_key <> -1 AND h.store_id IN (SELECT t.store_id FROM stg.dim_store AS t)
        ) AS x ON x.store_key = d.store_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.store_code = s.store_code,
               d.store_name = s.store_name,
               d.store_format = s.store_format,
               d.state = s.state,
               d.open_date = s.open_date
        FROM   dw.dim_store AS d
        INNER JOIN stg.dim_store AS s ON s.store_id = d.store_id AND s.src_is_current = 1
        WHERE  d.store_key <> -1
          AND ((d.store_code <> s.store_code OR (d.store_code IS NULL AND s.store_code IS NOT NULL) OR (d.store_code IS NOT NULL AND s.store_code IS NULL))
            OR (d.store_name <> s.store_name OR (d.store_name IS NULL AND s.store_name IS NOT NULL) OR (d.store_name IS NOT NULL AND s.store_name IS NULL))
            OR (d.store_format <> s.store_format OR (d.store_format IS NULL AND s.store_format IS NOT NULL) OR (d.store_format IS NOT NULL AND s.store_format IS NULL))
            OR (d.state <> s.state OR (d.state IS NULL AND s.state IS NOT NULL) OR (d.state IS NOT NULL AND s.state IS NULL))
            OR (d.open_date <> s.open_date OR (d.open_date IS NULL AND s.open_date IS NOT NULL) OR (d.open_date IS NOT NULL AND s.open_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('stores staged with full history: ', (SELECT COUNT(DISTINCT store_id) FROM stg.dim_store));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_store', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_store', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_store', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_stock_issue: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_stock_issue @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_stock_issue';
        -- 1. Stage stock issues changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_stock_issue;
        INSERT INTO stg.fact_stock_issue (issue_id, store_id, issue_type, priority, status, raised_ts, resolved_ts, lost_sales_aud, units_affected, _is_deleted, _silver_updated_at)
        SELECT sv.issue_id, sv.store_id, sv.issue_type, sv.priority, sv.status, sv.raised_ts, sv.resolved_ts, sv.lost_sales_aud, sv.units_affected, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.stock_issue AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_stock_issue;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = store version valid when the stock issue was raised, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_stock_issue AS t
        USING (
        SELECT s.issue_id,
               COALESCE(ka.store_key, kc.store_key, -1) AS store_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.raised_ts, 112)), -1) AS opened_date_key,
               s.issue_type, s.priority, s.status, s.raised_ts, s.resolved_ts,
               CAST(DATEDIFF(MINUTE, s.raised_ts, s.resolved_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('RAISED', 'ACTIONING') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.lost_sales_aud, s.units_affected, s._is_deleted
        FROM   stg.fact_stock_issue AS s
        LEFT JOIN dw.dim_store AS ka ON ka.store_id = s.store_id AND ka.store_key <> -1 AND s.raised_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.raised_ts < ka.valid_to)
        LEFT JOIN dw.dim_store AS kc ON kc.store_id = s.store_id AND kc.is_current = 1 AND kc.store_key <> -1
        ) AS src
        ON t.issue_id = src.issue_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.store_key = src.store_key, t.opened_date_key = src.opened_date_key, t.issue_type = src.issue_type, t.priority = src.priority, t.status = src.status, t.raised_ts = src.raised_ts, t.resolved_ts = src.resolved_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.lost_sales_aud = src.lost_sales_aud, t.units_affected = src.units_affected
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (issue_id, store_key, opened_date_key, issue_type, priority, status, raised_ts, resolved_ts, hours_to_close, is_open, lost_sales_aud, units_affected)
            VALUES (src.issue_id, src.store_key, src.opened_date_key, src.issue_type, src.priority, src.status, src.raised_ts, src.resolved_ts, src.hours_to_close, src.is_open, src.lost_sales_aud, src.units_affected);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving store: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_stock_issue', f.issue_id, s.store_id, s.raised_ts, @run_id, @now
        FROM   dw.fact_stock_issue AS f
        INNER JOIN stg.fact_stock_issue AS s ON s.issue_id = f.issue_id
        WHERE  f.store_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_stock_issue' AND la.business_key = f.issue_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the store exists in the dimension
        UPDATE f
        SET    f.store_key = COALESCE(ka.store_key, kc.store_key, -1)
        FROM   dw.fact_stock_issue AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_stock_issue' AND l.business_key = f.issue_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_store AS ka ON ka.store_id = l.entity_id AND ka.store_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_store AS kc ON kc.store_id = l.entity_id AND kc.is_current = 1 AND kc.store_key <> -1
        WHERE  f.store_key = -1
          AND  COALESCE(ka.store_key, kc.store_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_stock_issue AS f ON f.issue_id = l.business_key
        WHERE  l.object_name = 'fact_stock_issue' AND l.resolved_at IS NULL AND f.store_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_stock_issue AS u WHERE u.store_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_stock_issue', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_stock_issue', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_stock_issue', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_store_trading_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_store_trading_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_store_trading_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late store are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_store_trading_daily;
        INSERT INTO stg.fact_store_trading_daily (store_id, reading_ts, out_of_stock_pct, sales_aud, footfall, _ingested_at)
        SELECT sv.store_id, sv.reading_ts, sv.out_of_stock_pct, sv.sales_aud, sv.footfall, sv._ingested_at
        FROM   lh_mdf.silver.store_trading AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.store_trading AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_store_trading_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.store_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_store_trading_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_store_trading_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_store_trading_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the store version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_store_trading_daily (store_key, reading_date_key, readings, avg_out_of_stock_pct, max_out_of_stock_pct, breach_readings, avg_sales_aud, avg_footfall)
        SELECT COALESCE(ka.store_key, kc.store_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.out_of_stock_pct) AS DECIMAL(18,2)), CAST(MAX(s.out_of_stock_pct) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.out_of_stock_pct > 8 THEN 1 ELSE 0 END),
               CAST(AVG(s.sales_aud) AS DECIMAL(18,2)), CAST(AVG(s.footfall) AS DECIMAL(18,2))
        FROM   stg.fact_store_trading_daily AS s
        LEFT JOIN dw.dim_store AS ka ON ka.store_id = s.store_id AND ka.store_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_store AS kc ON kc.store_id = s.store_id AND kc.is_current = 1 AND kc.store_key <> -1
        GROUP BY COALESCE(ka.store_key, kc.store_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_store_trading_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_store_trading_daily AS u WHERE u.store_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_store_trading_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_store_trading_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_store_trading_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- Orchestrator: dimension first, then facts. A pipeline Stored procedure activity calls this after the Silver step.
CREATE OR ALTER PROCEDURE etl.usp_run_all
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id VARCHAR(64) = CONCAT('wh_', REPLACE(CONVERT(VARCHAR(19), SYSUTCDATETIME(), 126), ':', ''), '_',
                                         LEFT(CONVERT(VARCHAR(36), NEWID()), 8));
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME();
    -- RLS guard: once sec.user_region has rows, the loading identity must be mapped to '*' or the procedures
    -- would only see some states and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_store @run_id = @run_id;
        EXEC etl.usp_load_fact_stock_issue @run_id = @run_id;
        EXEC etl.usp_load_fact_store_trading_daily @run_id = @run_id;
    END TRY
    BEGIN CATCH
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'usp_run_all', 'FAILED', 0, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
    SELECT object_name, status, rows_staged, rows_inserted, rows_updated,
           DATEDIFF(MILLISECOND, started_at, ended_at) AS duration_ms, message
    FROM   etl.load_log
    WHERE  run_id = @run_id
    ORDER BY started_at;
END;
GO
