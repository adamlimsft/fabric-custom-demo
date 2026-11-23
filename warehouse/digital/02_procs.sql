/* Lyrebird Apps (Digital natives) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.tenant ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_tenant: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_tenant @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_tenant';
        -- 1. Stage every Silver version of each tenant touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_tenant;
        INSERT INTO stg.dim_tenant (tenant_id, tenant_slug, tenant_name, [plan], cloud_region, status, account_tier, seats, signup_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.tenant_id, sv.tenant_slug, sv.tenant_name, sv.[plan], sv.cloud_region, sv.status, sv.account_tier, sv.seats, sv.signup_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.tenant AS sv
        WHERE  sv.tenant_id IN (SELECT svn.tenant_id FROM lh_mdf.silver.tenant AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_tenant;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(tenant_key), 0) FROM dw.dim_tenant);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, account_tier or seats changed).
        --    A new member's first version is valid from 1900-01-01 so older support tickets still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_tenant (tenant_key, tenant_id, tenant_slug, tenant_name, [plan], cloud_region, status, account_tier, seats, signup_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.tenant_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.tenant_id, v.tenant_slug, v.tenant_name, v.[plan], v.cloud_region, v.status, v.account_tier, v.seats, v.signup_date,
               CASE WHEN c.tenant_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.tenant_id, n.tenant_slug, n.tenant_name, n.[plan], n.cloud_region, n.status, n.account_tier, n.seats, n.signup_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.tenant_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.account_tier) OVER (PARTITION BY n.tenant_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_account_tier,
                        LAG(n.seats) OVER (PARTITION BY n.tenant_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_seats,
                        ROW_NUMBER() OVER (PARTITION BY n.tenant_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_tenant AS n
                LEFT JOIN (SELECT h.tenant_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_tenant AS h
                           WHERE h.tenant_key <> -1 GROUP BY h.tenant_id) AS l ON l.tenant_id = n.tenant_id
                WHERE  l.tenant_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_tenant AS c ON c.tenant_id = v.tenant_id AND c.is_current = 1 AND c.tenant_key <> -1
        WHERE (v.rn = 1 AND c.tenant_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.account_tier <> c.account_tier OR (v.account_tier IS NULL AND c.account_tier IS NOT NULL) OR (v.account_tier IS NOT NULL AND c.account_tier IS NULL)) OR (v.seats <> c.seats OR (v.seats IS NULL AND c.seats IS NOT NULL) OR (v.seats IS NOT NULL AND c.seats IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.account_tier <> v.prev_account_tier OR (v.account_tier IS NULL AND v.prev_account_tier IS NOT NULL) OR (v.account_tier IS NOT NULL AND v.prev_account_tier IS NULL)) OR (v.seats <> v.prev_seats OR (v.seats IS NULL AND v.prev_seats IS NOT NULL) OR (v.seats IS NOT NULL AND v.prev_seats IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_tenant AS d
        INNER JOIN (
            SELECT h.tenant_key, LEAD(h.valid_from) OVER (PARTITION BY h.tenant_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_tenant AS h
            WHERE  h.tenant_key <> -1 AND h.tenant_id IN (SELECT t.tenant_id FROM stg.dim_tenant AS t)
        ) AS x ON x.tenant_key = d.tenant_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.tenant_slug = s.tenant_slug,
               d.tenant_name = s.tenant_name,
               d.[plan] = s.[plan],
               d.cloud_region = s.cloud_region,
               d.signup_date = s.signup_date
        FROM   dw.dim_tenant AS d
        INNER JOIN stg.dim_tenant AS s ON s.tenant_id = d.tenant_id AND s.src_is_current = 1
        WHERE  d.tenant_key <> -1
          AND ((d.tenant_slug <> s.tenant_slug OR (d.tenant_slug IS NULL AND s.tenant_slug IS NOT NULL) OR (d.tenant_slug IS NOT NULL AND s.tenant_slug IS NULL))
            OR (d.tenant_name <> s.tenant_name OR (d.tenant_name IS NULL AND s.tenant_name IS NOT NULL) OR (d.tenant_name IS NOT NULL AND s.tenant_name IS NULL))
            OR (d.[plan] <> s.[plan] OR (d.[plan] IS NULL AND s.[plan] IS NOT NULL) OR (d.[plan] IS NOT NULL AND s.[plan] IS NULL))
            OR (d.cloud_region <> s.cloud_region OR (d.cloud_region IS NULL AND s.cloud_region IS NOT NULL) OR (d.cloud_region IS NOT NULL AND s.cloud_region IS NULL))
            OR (d.signup_date <> s.signup_date OR (d.signup_date IS NULL AND s.signup_date IS NOT NULL) OR (d.signup_date IS NOT NULL AND s.signup_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('tenants staged with full history: ', (SELECT COUNT(DISTINCT tenant_id) FROM stg.dim_tenant));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_tenant', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_tenant', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_tenant', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_support_ticket: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_support_ticket @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_support_ticket';
        -- 1. Stage support tickets changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_support_ticket;
        INSERT INTO stg.fact_support_ticket (ticket_id, tenant_id, ticket_type, priority, status, opened_ts, solved_ts, credit_aud, agent_hours, _is_deleted, _silver_updated_at)
        SELECT sv.ticket_id, sv.tenant_id, sv.ticket_type, sv.priority, sv.status, sv.opened_ts, sv.solved_ts, sv.credit_aud, sv.agent_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.support_ticket AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_support_ticket;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = tenant version valid when the support ticket was opened, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_support_ticket AS t
        USING (
        SELECT s.ticket_id,
               COALESCE(ka.tenant_key, kc.tenant_key, -1) AS tenant_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.opened_ts, 112)), -1) AS opened_date_key,
               s.ticket_type, s.priority, s.status, s.opened_ts, s.solved_ts,
               CAST(DATEDIFF(MINUTE, s.opened_ts, s.solved_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('OPEN', 'PENDING') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.credit_aud, s.agent_hours, s._is_deleted
        FROM   stg.fact_support_ticket AS s
        LEFT JOIN dw.dim_tenant AS ka ON ka.tenant_id = s.tenant_id AND ka.tenant_key <> -1 AND s.opened_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.opened_ts < ka.valid_to)
        LEFT JOIN dw.dim_tenant AS kc ON kc.tenant_id = s.tenant_id AND kc.is_current = 1 AND kc.tenant_key <> -1
        ) AS src
        ON t.ticket_id = src.ticket_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.tenant_key = src.tenant_key, t.opened_date_key = src.opened_date_key, t.ticket_type = src.ticket_type, t.priority = src.priority, t.status = src.status, t.opened_ts = src.opened_ts, t.solved_ts = src.solved_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.credit_aud = src.credit_aud, t.agent_hours = src.agent_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (ticket_id, tenant_key, opened_date_key, ticket_type, priority, status, opened_ts, solved_ts, hours_to_close, is_open, credit_aud, agent_hours)
            VALUES (src.ticket_id, src.tenant_key, src.opened_date_key, src.ticket_type, src.priority, src.status, src.opened_ts, src.solved_ts, src.hours_to_close, src.is_open, src.credit_aud, src.agent_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving tenant: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_support_ticket', f.ticket_id, s.tenant_id, s.opened_ts, @run_id, @now
        FROM   dw.fact_support_ticket AS f
        INNER JOIN stg.fact_support_ticket AS s ON s.ticket_id = f.ticket_id
        WHERE  f.tenant_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_support_ticket' AND la.business_key = f.ticket_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the tenant exists in the dimension
        UPDATE f
        SET    f.tenant_key = COALESCE(ka.tenant_key, kc.tenant_key, -1)
        FROM   dw.fact_support_ticket AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_support_ticket' AND l.business_key = f.ticket_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_tenant AS ka ON ka.tenant_id = l.entity_id AND ka.tenant_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_tenant AS kc ON kc.tenant_id = l.entity_id AND kc.is_current = 1 AND kc.tenant_key <> -1
        WHERE  f.tenant_key = -1
          AND  COALESCE(ka.tenant_key, kc.tenant_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_support_ticket AS f ON f.ticket_id = l.business_key
        WHERE  l.object_name = 'fact_support_ticket' AND l.resolved_at IS NULL AND f.tenant_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_support_ticket AS u WHERE u.tenant_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_support_ticket', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_support_ticket', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_support_ticket', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_usage_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_usage_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_usage_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late tenant are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_usage_daily;
        INSERT INTO stg.fact_usage_daily (tenant_id, reading_ts, p95_latency_ms, active_users, error_rate_pct, _ingested_at)
        SELECT sv.tenant_id, sv.reading_ts, sv.p95_latency_ms, sv.active_users, sv.error_rate_pct, sv._ingested_at
        FROM   lh_mdf.silver.usage_metric AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.usage_metric AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_usage_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.tenant_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_usage_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_usage_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_usage_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the tenant version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_usage_daily (tenant_key, reading_date_key, readings, avg_p95_latency_ms, max_p95_latency_ms, breach_readings, avg_active_users, avg_error_rate_pct)
        SELECT COALESCE(ka.tenant_key, kc.tenant_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.p95_latency_ms) AS DECIMAL(18,2)), CAST(MAX(s.p95_latency_ms) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.p95_latency_ms > 800 THEN 1 ELSE 0 END),
               CAST(AVG(s.active_users) AS DECIMAL(18,2)), CAST(AVG(s.error_rate_pct) AS DECIMAL(18,2))
        FROM   stg.fact_usage_daily AS s
        LEFT JOIN dw.dim_tenant AS ka ON ka.tenant_id = s.tenant_id AND ka.tenant_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_tenant AS kc ON kc.tenant_id = s.tenant_id AND kc.is_current = 1 AND kc.tenant_key <> -1
        GROUP BY COALESCE(ka.tenant_key, kc.tenant_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_usage_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_usage_daily AS u WHERE u.tenant_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_usage_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_usage_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_usage_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
    -- would only see some cloud regions and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_tenant @run_id = @run_id;
        EXEC etl.usp_load_fact_support_ticket @run_id = @run_id;
        EXEC etl.usp_load_fact_usage_daily @run_id = @run_id;
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
