/* Bluegum Digital Services (IT & ITeS) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.client_service ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_client_service: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_client_service @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_client_service';
        -- 1. Stage every Silver version of each client service touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_client_service;
        INSERT INTO stg.dim_client_service (service_id, contract_ref, service_name, service_line, delivery_centre, status, sla_tier, monthly_fee_aud, contract_start, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.service_id, sv.contract_ref, sv.service_name, sv.service_line, sv.delivery_centre, sv.status, sv.sla_tier, sv.monthly_fee_aud, sv.contract_start, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.client_service AS sv
        WHERE  sv.service_id IN (SELECT svn.service_id FROM lh_mdf.silver.client_service AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_client_service;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(client_service_key), 0) FROM dw.dim_client_service);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, sla_tier or monthly_fee_aud changed).
        --    A new member's first version is valid from 1900-01-01 so older incidents still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_client_service (client_service_key, service_id, contract_ref, service_name, service_line, delivery_centre, status, sla_tier, monthly_fee_aud, contract_start, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.service_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.service_id, v.contract_ref, v.service_name, v.service_line, v.delivery_centre, v.status, v.sla_tier, v.monthly_fee_aud, v.contract_start,
               CASE WHEN c.service_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.service_id, n.contract_ref, n.service_name, n.service_line, n.delivery_centre, n.status, n.sla_tier, n.monthly_fee_aud, n.contract_start, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.service_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.sla_tier) OVER (PARTITION BY n.service_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_sla_tier,
                        LAG(n.monthly_fee_aud) OVER (PARTITION BY n.service_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_monthly_fee_aud,
                        ROW_NUMBER() OVER (PARTITION BY n.service_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_client_service AS n
                LEFT JOIN (SELECT h.service_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_client_service AS h
                           WHERE h.client_service_key <> -1 GROUP BY h.service_id) AS l ON l.service_id = n.service_id
                WHERE  l.service_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_client_service AS c ON c.service_id = v.service_id AND c.is_current = 1 AND c.client_service_key <> -1
        WHERE (v.rn = 1 AND c.service_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.sla_tier <> c.sla_tier OR (v.sla_tier IS NULL AND c.sla_tier IS NOT NULL) OR (v.sla_tier IS NOT NULL AND c.sla_tier IS NULL)) OR (v.monthly_fee_aud <> c.monthly_fee_aud OR (v.monthly_fee_aud IS NULL AND c.monthly_fee_aud IS NOT NULL) OR (v.monthly_fee_aud IS NOT NULL AND c.monthly_fee_aud IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.sla_tier <> v.prev_sla_tier OR (v.sla_tier IS NULL AND v.prev_sla_tier IS NOT NULL) OR (v.sla_tier IS NOT NULL AND v.prev_sla_tier IS NULL)) OR (v.monthly_fee_aud <> v.prev_monthly_fee_aud OR (v.monthly_fee_aud IS NULL AND v.prev_monthly_fee_aud IS NOT NULL) OR (v.monthly_fee_aud IS NOT NULL AND v.prev_monthly_fee_aud IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_client_service AS d
        INNER JOIN (
            SELECT h.client_service_key, LEAD(h.valid_from) OVER (PARTITION BY h.service_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_client_service AS h
            WHERE  h.client_service_key <> -1 AND h.service_id IN (SELECT t.service_id FROM stg.dim_client_service AS t)
        ) AS x ON x.client_service_key = d.client_service_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.contract_ref = s.contract_ref,
               d.service_name = s.service_name,
               d.service_line = s.service_line,
               d.delivery_centre = s.delivery_centre,
               d.contract_start = s.contract_start
        FROM   dw.dim_client_service AS d
        INNER JOIN stg.dim_client_service AS s ON s.service_id = d.service_id AND s.src_is_current = 1
        WHERE  d.client_service_key <> -1
          AND ((d.contract_ref <> s.contract_ref OR (d.contract_ref IS NULL AND s.contract_ref IS NOT NULL) OR (d.contract_ref IS NOT NULL AND s.contract_ref IS NULL))
            OR (d.service_name <> s.service_name OR (d.service_name IS NULL AND s.service_name IS NOT NULL) OR (d.service_name IS NOT NULL AND s.service_name IS NULL))
            OR (d.service_line <> s.service_line OR (d.service_line IS NULL AND s.service_line IS NOT NULL) OR (d.service_line IS NOT NULL AND s.service_line IS NULL))
            OR (d.delivery_centre <> s.delivery_centre OR (d.delivery_centre IS NULL AND s.delivery_centre IS NOT NULL) OR (d.delivery_centre IS NOT NULL AND s.delivery_centre IS NULL))
            OR (d.contract_start <> s.contract_start OR (d.contract_start IS NULL AND s.contract_start IS NOT NULL) OR (d.contract_start IS NOT NULL AND s.contract_start IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('client services staged with full history: ', (SELECT COUNT(DISTINCT service_id) FROM stg.dim_client_service));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_client_service', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_client_service', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_client_service', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_incident: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_incident @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_incident';
        -- 1. Stage incidents changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_incident;
        INSERT INTO stg.fact_incident (incident_id, service_id, category, priority, status, opened_ts, resolved_ts, sla_penalty_aud, effort_hours, _is_deleted, _silver_updated_at)
        SELECT sv.incident_id, sv.service_id, sv.category, sv.priority, sv.status, sv.opened_ts, sv.resolved_ts, sv.sla_penalty_aud, sv.effort_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.incident AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_incident;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = client service version valid when the incident was opened, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_incident AS t
        USING (
        SELECT s.incident_id,
               COALESCE(ka.client_service_key, kc.client_service_key, -1) AS client_service_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.opened_ts, 112)), -1) AS opened_date_key,
               s.category, s.priority, s.status, s.opened_ts, s.resolved_ts,
               CAST(DATEDIFF(MINUTE, s.opened_ts, s.resolved_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('NEW', 'IN_PROGRESS') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.sla_penalty_aud, s.effort_hours, s._is_deleted
        FROM   stg.fact_incident AS s
        LEFT JOIN dw.dim_client_service AS ka ON ka.service_id = s.service_id AND ka.client_service_key <> -1 AND s.opened_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.opened_ts < ka.valid_to)
        LEFT JOIN dw.dim_client_service AS kc ON kc.service_id = s.service_id AND kc.is_current = 1 AND kc.client_service_key <> -1
        ) AS src
        ON t.incident_id = src.incident_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.client_service_key = src.client_service_key, t.opened_date_key = src.opened_date_key, t.category = src.category, t.priority = src.priority, t.status = src.status, t.opened_ts = src.opened_ts, t.resolved_ts = src.resolved_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.sla_penalty_aud = src.sla_penalty_aud, t.effort_hours = src.effort_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (incident_id, client_service_key, opened_date_key, category, priority, status, opened_ts, resolved_ts, hours_to_close, is_open, sla_penalty_aud, effort_hours)
            VALUES (src.incident_id, src.client_service_key, src.opened_date_key, src.category, src.priority, src.status, src.opened_ts, src.resolved_ts, src.hours_to_close, src.is_open, src.sla_penalty_aud, src.effort_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving client service: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_incident', f.incident_id, s.service_id, s.opened_ts, @run_id, @now
        FROM   dw.fact_incident AS f
        INNER JOIN stg.fact_incident AS s ON s.incident_id = f.incident_id
        WHERE  f.client_service_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_incident' AND la.business_key = f.incident_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the client service exists in the dimension
        UPDATE f
        SET    f.client_service_key = COALESCE(ka.client_service_key, kc.client_service_key, -1)
        FROM   dw.fact_incident AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_incident' AND l.business_key = f.incident_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_client_service AS ka ON ka.service_id = l.entity_id AND ka.client_service_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_client_service AS kc ON kc.service_id = l.entity_id AND kc.is_current = 1 AND kc.client_service_key <> -1
        WHERE  f.client_service_key = -1
          AND  COALESCE(ka.client_service_key, kc.client_service_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_incident AS f ON f.incident_id = l.business_key
        WHERE  l.object_name = 'fact_incident' AND l.resolved_at IS NULL AND f.client_service_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_incident AS u WHERE u.client_service_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_incident', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_incident', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_incident', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_service_perf_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_service_perf_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_service_perf_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late client service are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_service_perf_daily;
        INSERT INTO stg.fact_service_perf_daily (service_id, reading_ts, p95_response_ms, availability_pct, ticket_backlog, _ingested_at)
        SELECT sv.service_id, sv.reading_ts, sv.p95_response_ms, sv.availability_pct, sv.ticket_backlog, sv._ingested_at
        FROM   lh_mdf.silver.service_metric AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.service_metric AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_service_perf_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.client_service_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_service_perf_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_service_perf_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_service_perf_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the client service version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_service_perf_daily (client_service_key, reading_date_key, readings, avg_p95_response_ms, max_p95_response_ms, breach_readings, avg_availability_pct, avg_ticket_backlog)
        SELECT COALESCE(ka.client_service_key, kc.client_service_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.p95_response_ms) AS DECIMAL(18,2)), CAST(MAX(s.p95_response_ms) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.p95_response_ms > 2500 THEN 1 ELSE 0 END),
               CAST(AVG(s.availability_pct) AS DECIMAL(18,2)), CAST(AVG(s.ticket_backlog) AS DECIMAL(18,2))
        FROM   stg.fact_service_perf_daily AS s
        LEFT JOIN dw.dim_client_service AS ka ON ka.service_id = s.service_id AND ka.client_service_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_client_service AS kc ON kc.service_id = s.service_id AND kc.is_current = 1 AND kc.client_service_key <> -1
        GROUP BY COALESCE(ka.client_service_key, kc.client_service_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_service_perf_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_service_perf_daily AS u WHERE u.client_service_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_service_perf_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_service_perf_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_service_perf_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
    -- would only see some delivery centres and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_client_service @run_id = @run_id;
        EXEC etl.usp_load_fact_incident @run_id = @run_id;
        EXEC etl.usp_load_fact_service_perf_daily @run_id = @run_id;
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
