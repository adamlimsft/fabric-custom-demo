/* Harbour Mutual Bank (Financial services) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.account ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_account: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_account @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_account';
        -- 1. Stage every Silver version of each account touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_account;
        INSERT INTO stg.dim_account (account_id, account_number, account_name, product_type, state, status, risk_grade, balance_aud, open_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.account_id, sv.account_number, sv.account_name, sv.product_type, sv.state, sv.status, sv.risk_grade, sv.balance_aud, sv.open_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.account AS sv
        WHERE  sv.account_id IN (SELECT svn.account_id FROM lh_mdf.silver.account AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_account;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(account_key), 0) FROM dw.dim_account);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, risk_grade or balance_aud changed).
        --    A new member's first version is valid from 1900-01-01 so older risk cases still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_account (account_key, account_id, account_number, account_name, product_type, state, status, risk_grade, balance_aud, open_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.account_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.account_id, v.account_number, v.account_name, v.product_type, v.state, v.status, v.risk_grade, v.balance_aud, v.open_date,
               CASE WHEN c.account_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.account_id, n.account_number, n.account_name, n.product_type, n.state, n.status, n.risk_grade, n.balance_aud, n.open_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.account_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.risk_grade) OVER (PARTITION BY n.account_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_risk_grade,
                        LAG(n.balance_aud) OVER (PARTITION BY n.account_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_balance_aud,
                        ROW_NUMBER() OVER (PARTITION BY n.account_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_account AS n
                LEFT JOIN (SELECT h.account_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_account AS h
                           WHERE h.account_key <> -1 GROUP BY h.account_id) AS l ON l.account_id = n.account_id
                WHERE  l.account_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_account AS c ON c.account_id = v.account_id AND c.is_current = 1 AND c.account_key <> -1
        WHERE (v.rn = 1 AND c.account_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.risk_grade <> c.risk_grade OR (v.risk_grade IS NULL AND c.risk_grade IS NOT NULL) OR (v.risk_grade IS NOT NULL AND c.risk_grade IS NULL)) OR (v.balance_aud <> c.balance_aud OR (v.balance_aud IS NULL AND c.balance_aud IS NOT NULL) OR (v.balance_aud IS NOT NULL AND c.balance_aud IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.risk_grade <> v.prev_risk_grade OR (v.risk_grade IS NULL AND v.prev_risk_grade IS NOT NULL) OR (v.risk_grade IS NOT NULL AND v.prev_risk_grade IS NULL)) OR (v.balance_aud <> v.prev_balance_aud OR (v.balance_aud IS NULL AND v.prev_balance_aud IS NOT NULL) OR (v.balance_aud IS NOT NULL AND v.prev_balance_aud IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_account AS d
        INNER JOIN (
            SELECT h.account_key, LEAD(h.valid_from) OVER (PARTITION BY h.account_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_account AS h
            WHERE  h.account_key <> -1 AND h.account_id IN (SELECT t.account_id FROM stg.dim_account AS t)
        ) AS x ON x.account_key = d.account_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.account_number = s.account_number,
               d.account_name = s.account_name,
               d.product_type = s.product_type,
               d.state = s.state,
               d.open_date = s.open_date
        FROM   dw.dim_account AS d
        INNER JOIN stg.dim_account AS s ON s.account_id = d.account_id AND s.src_is_current = 1
        WHERE  d.account_key <> -1
          AND ((d.account_number <> s.account_number OR (d.account_number IS NULL AND s.account_number IS NOT NULL) OR (d.account_number IS NOT NULL AND s.account_number IS NULL))
            OR (d.account_name <> s.account_name OR (d.account_name IS NULL AND s.account_name IS NOT NULL) OR (d.account_name IS NOT NULL AND s.account_name IS NULL))
            OR (d.product_type <> s.product_type OR (d.product_type IS NULL AND s.product_type IS NOT NULL) OR (d.product_type IS NOT NULL AND s.product_type IS NULL))
            OR (d.state <> s.state OR (d.state IS NULL AND s.state IS NOT NULL) OR (d.state IS NOT NULL AND s.state IS NULL))
            OR (d.open_date <> s.open_date OR (d.open_date IS NULL AND s.open_date IS NOT NULL) OR (d.open_date IS NOT NULL AND s.open_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('accounts staged with full history: ', (SELECT COUNT(DISTINCT account_id) FROM stg.dim_account));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_account', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_account', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_account', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_risk_case: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_risk_case @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_risk_case';
        -- 1. Stage risk cases changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_risk_case;
        INSERT INTO stg.fact_risk_case (case_id, account_id, case_type, priority, status, raised_ts, closed_ts, case_amount_aud, analyst_hours, _is_deleted, _silver_updated_at)
        SELECT sv.case_id, sv.account_id, sv.case_type, sv.priority, sv.status, sv.raised_ts, sv.closed_ts, sv.case_amount_aud, sv.analyst_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.risk_case AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_risk_case;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = account version valid when the risk case was raised, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_risk_case AS t
        USING (
        SELECT s.case_id,
               COALESCE(ka.account_key, kc.account_key, -1) AS account_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.raised_ts, 112)), -1) AS opened_date_key,
               s.case_type, s.priority, s.status, s.raised_ts, s.closed_ts,
               CAST(DATEDIFF(MINUTE, s.raised_ts, s.closed_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('OPEN', 'INVESTIGATING') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.case_amount_aud, s.analyst_hours, s._is_deleted
        FROM   stg.fact_risk_case AS s
        LEFT JOIN dw.dim_account AS ka ON ka.account_id = s.account_id AND ka.account_key <> -1 AND s.raised_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.raised_ts < ka.valid_to)
        LEFT JOIN dw.dim_account AS kc ON kc.account_id = s.account_id AND kc.is_current = 1 AND kc.account_key <> -1
        ) AS src
        ON t.case_id = src.case_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.account_key = src.account_key, t.opened_date_key = src.opened_date_key, t.case_type = src.case_type, t.priority = src.priority, t.status = src.status, t.raised_ts = src.raised_ts, t.closed_ts = src.closed_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.case_amount_aud = src.case_amount_aud, t.analyst_hours = src.analyst_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (case_id, account_key, opened_date_key, case_type, priority, status, raised_ts, closed_ts, hours_to_close, is_open, case_amount_aud, analyst_hours)
            VALUES (src.case_id, src.account_key, src.opened_date_key, src.case_type, src.priority, src.status, src.raised_ts, src.closed_ts, src.hours_to_close, src.is_open, src.case_amount_aud, src.analyst_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving account: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_risk_case', f.case_id, s.account_id, s.raised_ts, @run_id, @now
        FROM   dw.fact_risk_case AS f
        INNER JOIN stg.fact_risk_case AS s ON s.case_id = f.case_id
        WHERE  f.account_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_risk_case' AND la.business_key = f.case_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the account exists in the dimension
        UPDATE f
        SET    f.account_key = COALESCE(ka.account_key, kc.account_key, -1)
        FROM   dw.fact_risk_case AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_risk_case' AND l.business_key = f.case_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_account AS ka ON ka.account_id = l.entity_id AND ka.account_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_account AS kc ON kc.account_id = l.entity_id AND kc.is_current = 1 AND kc.account_key <> -1
        WHERE  f.account_key = -1
          AND  COALESCE(ka.account_key, kc.account_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_risk_case AS f ON f.case_id = l.business_key
        WHERE  l.object_name = 'fact_risk_case' AND l.resolved_at IS NULL AND f.account_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_risk_case AS u WHERE u.account_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_risk_case', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_risk_case', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_risk_case', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_txn_monitor_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_txn_monitor_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_txn_monitor_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late account are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_txn_monitor_daily;
        INSERT INTO stg.fact_txn_monitor_daily (account_id, reading_ts, decline_pct, txn_value_aud, txn_count, _ingested_at)
        SELECT sv.account_id, sv.reading_ts, sv.decline_pct, sv.txn_value_aud, sv.txn_count, sv._ingested_at
        FROM   lh_mdf.silver.txn_monitor AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.txn_monitor AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_txn_monitor_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.account_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_txn_monitor_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_txn_monitor_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_txn_monitor_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the account version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_txn_monitor_daily (account_key, reading_date_key, readings, avg_decline_pct, max_decline_pct, breach_readings, avg_txn_value_aud, avg_txn_count)
        SELECT COALESCE(ka.account_key, kc.account_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.decline_pct) AS DECIMAL(18,2)), CAST(MAX(s.decline_pct) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.decline_pct > 30 THEN 1 ELSE 0 END),
               CAST(AVG(s.txn_value_aud) AS DECIMAL(18,2)), CAST(AVG(s.txn_count) AS DECIMAL(18,2))
        FROM   stg.fact_txn_monitor_daily AS s
        LEFT JOIN dw.dim_account AS ka ON ka.account_id = s.account_id AND ka.account_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_account AS kc ON kc.account_id = s.account_id AND kc.is_current = 1 AND kc.account_key <> -1
        GROUP BY COALESCE(ka.account_key, kc.account_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_txn_monitor_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_txn_monitor_daily AS u WHERE u.account_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_txn_monitor_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_txn_monitor_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_txn_monitor_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
        EXEC etl.usp_load_dim_account @run_id = @run_id;
        EXEC etl.usp_load_fact_risk_case @run_id = @run_id;
        EXEC etl.usp_load_fact_txn_monitor_daily @run_id = @run_id;
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
