/* Kestrel Components (Manufacturing) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.machine ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_machine: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_machine @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_machine';
        -- 1. Stage every Silver version of each machine touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_machine;
        INSERT INTO stg.dim_machine (machine_id, machine_code, machine_name, machine_type, plant, status, criticality, rated_units_per_hour, commissioned_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.machine_id, sv.machine_code, sv.machine_name, sv.machine_type, sv.plant, sv.status, sv.criticality, sv.rated_units_per_hour, sv.commissioned_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.machine AS sv
        WHERE  sv.machine_id IN (SELECT svn.machine_id FROM lh_mdf.silver.machine AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_machine;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(machine_key), 0) FROM dw.dim_machine);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, criticality or rated_units_per_hour changed).
        --    A new member's first version is valid from 1900-01-01 so older maintenance orders still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_machine (machine_key, machine_id, machine_code, machine_name, machine_type, plant, status, criticality, rated_units_per_hour, commissioned_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.machine_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.machine_id, v.machine_code, v.machine_name, v.machine_type, v.plant, v.status, v.criticality, v.rated_units_per_hour, v.commissioned_date,
               CASE WHEN c.machine_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.machine_id, n.machine_code, n.machine_name, n.machine_type, n.plant, n.status, n.criticality, n.rated_units_per_hour, n.commissioned_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.machine_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.criticality) OVER (PARTITION BY n.machine_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_criticality,
                        LAG(n.rated_units_per_hour) OVER (PARTITION BY n.machine_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_rated_units_per_hour,
                        ROW_NUMBER() OVER (PARTITION BY n.machine_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_machine AS n
                LEFT JOIN (SELECT h.machine_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_machine AS h
                           WHERE h.machine_key <> -1 GROUP BY h.machine_id) AS l ON l.machine_id = n.machine_id
                WHERE  l.machine_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_machine AS c ON c.machine_id = v.machine_id AND c.is_current = 1 AND c.machine_key <> -1
        WHERE (v.rn = 1 AND c.machine_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.criticality <> c.criticality OR (v.criticality IS NULL AND c.criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND c.criticality IS NULL)) OR (v.rated_units_per_hour <> c.rated_units_per_hour OR (v.rated_units_per_hour IS NULL AND c.rated_units_per_hour IS NOT NULL) OR (v.rated_units_per_hour IS NOT NULL AND c.rated_units_per_hour IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.criticality <> v.prev_criticality OR (v.criticality IS NULL AND v.prev_criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND v.prev_criticality IS NULL)) OR (v.rated_units_per_hour <> v.prev_rated_units_per_hour OR (v.rated_units_per_hour IS NULL AND v.prev_rated_units_per_hour IS NOT NULL) OR (v.rated_units_per_hour IS NOT NULL AND v.prev_rated_units_per_hour IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_machine AS d
        INNER JOIN (
            SELECT h.machine_key, LEAD(h.valid_from) OVER (PARTITION BY h.machine_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_machine AS h
            WHERE  h.machine_key <> -1 AND h.machine_id IN (SELECT t.machine_id FROM stg.dim_machine AS t)
        ) AS x ON x.machine_key = d.machine_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.machine_code = s.machine_code,
               d.machine_name = s.machine_name,
               d.machine_type = s.machine_type,
               d.plant = s.plant,
               d.commissioned_date = s.commissioned_date
        FROM   dw.dim_machine AS d
        INNER JOIN stg.dim_machine AS s ON s.machine_id = d.machine_id AND s.src_is_current = 1
        WHERE  d.machine_key <> -1
          AND ((d.machine_code <> s.machine_code OR (d.machine_code IS NULL AND s.machine_code IS NOT NULL) OR (d.machine_code IS NOT NULL AND s.machine_code IS NULL))
            OR (d.machine_name <> s.machine_name OR (d.machine_name IS NULL AND s.machine_name IS NOT NULL) OR (d.machine_name IS NOT NULL AND s.machine_name IS NULL))
            OR (d.machine_type <> s.machine_type OR (d.machine_type IS NULL AND s.machine_type IS NOT NULL) OR (d.machine_type IS NOT NULL AND s.machine_type IS NULL))
            OR (d.plant <> s.plant OR (d.plant IS NULL AND s.plant IS NOT NULL) OR (d.plant IS NOT NULL AND s.plant IS NULL))
            OR (d.commissioned_date <> s.commissioned_date OR (d.commissioned_date IS NULL AND s.commissioned_date IS NOT NULL) OR (d.commissioned_date IS NOT NULL AND s.commissioned_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('machines staged with full history: ', (SELECT COUNT(DISTINCT machine_id) FROM stg.dim_machine));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_machine', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_machine', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_machine', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_maintenance_order: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_maintenance_order @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_maintenance_order';
        -- 1. Stage maintenance orders changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_maintenance_order;
        INSERT INTO stg.fact_maintenance_order (mo_id, machine_id, mo_type, priority, status, reported_ts, finished_ts, parts_cost_aud, downtime_hours, _is_deleted, _silver_updated_at)
        SELECT sv.mo_id, sv.machine_id, sv.mo_type, sv.priority, sv.status, sv.reported_ts, sv.finished_ts, sv.parts_cost_aud, sv.downtime_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.maintenance_order AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_maintenance_order;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = machine version valid when the maintenance order was reported, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_maintenance_order AS t
        USING (
        SELECT s.mo_id,
               COALESCE(ka.machine_key, kc.machine_key, -1) AS machine_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.reported_ts, 112)), -1) AS opened_date_key,
               s.mo_type, s.priority, s.status, s.reported_ts, s.finished_ts,
               CAST(DATEDIFF(MINUTE, s.reported_ts, s.finished_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.parts_cost_aud, s.downtime_hours, s._is_deleted
        FROM   stg.fact_maintenance_order AS s
        LEFT JOIN dw.dim_machine AS ka ON ka.machine_id = s.machine_id AND ka.machine_key <> -1 AND s.reported_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reported_ts < ka.valid_to)
        LEFT JOIN dw.dim_machine AS kc ON kc.machine_id = s.machine_id AND kc.is_current = 1 AND kc.machine_key <> -1
        ) AS src
        ON t.mo_id = src.mo_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.machine_key = src.machine_key, t.opened_date_key = src.opened_date_key, t.mo_type = src.mo_type, t.priority = src.priority, t.status = src.status, t.reported_ts = src.reported_ts, t.finished_ts = src.finished_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.parts_cost_aud = src.parts_cost_aud, t.downtime_hours = src.downtime_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (mo_id, machine_key, opened_date_key, mo_type, priority, status, reported_ts, finished_ts, hours_to_close, is_open, parts_cost_aud, downtime_hours)
            VALUES (src.mo_id, src.machine_key, src.opened_date_key, src.mo_type, src.priority, src.status, src.reported_ts, src.finished_ts, src.hours_to_close, src.is_open, src.parts_cost_aud, src.downtime_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving machine: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_maintenance_order', f.mo_id, s.machine_id, s.reported_ts, @run_id, @now
        FROM   dw.fact_maintenance_order AS f
        INNER JOIN stg.fact_maintenance_order AS s ON s.mo_id = f.mo_id
        WHERE  f.machine_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_maintenance_order' AND la.business_key = f.mo_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the machine exists in the dimension
        UPDATE f
        SET    f.machine_key = COALESCE(ka.machine_key, kc.machine_key, -1)
        FROM   dw.fact_maintenance_order AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_maintenance_order' AND l.business_key = f.mo_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_machine AS ka ON ka.machine_id = l.entity_id AND ka.machine_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_machine AS kc ON kc.machine_id = l.entity_id AND kc.is_current = 1 AND kc.machine_key <> -1
        WHERE  f.machine_key = -1
          AND  COALESCE(ka.machine_key, kc.machine_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_maintenance_order AS f ON f.mo_id = l.business_key
        WHERE  l.object_name = 'fact_maintenance_order' AND l.resolved_at IS NULL AND f.machine_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_maintenance_order AS u WHERE u.machine_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_maintenance_order', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_maintenance_order', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_maintenance_order', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_machine_telemetry_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_machine_telemetry_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_machine_telemetry_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late machine are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_machine_telemetry_daily;
        INSERT INTO stg.fact_machine_telemetry_daily (machine_id, reading_ts, vibration_mm_s, units_produced, motor_temp_c, _ingested_at)
        SELECT sv.machine_id, sv.reading_ts, sv.vibration_mm_s, sv.units_produced, sv.motor_temp_c, sv._ingested_at
        FROM   lh_mdf.silver.machine_telemetry AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.machine_telemetry AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_machine_telemetry_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.machine_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_machine_telemetry_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_machine_telemetry_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_machine_telemetry_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the machine version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_machine_telemetry_daily (machine_key, reading_date_key, readings, avg_vibration_mm_s, max_vibration_mm_s, breach_readings, avg_units_produced, avg_motor_temp_c)
        SELECT COALESCE(ka.machine_key, kc.machine_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.vibration_mm_s) AS DECIMAL(18,2)), CAST(MAX(s.vibration_mm_s) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.vibration_mm_s > 7.1 THEN 1 ELSE 0 END),
               CAST(AVG(s.units_produced) AS DECIMAL(18,2)), CAST(AVG(s.motor_temp_c) AS DECIMAL(18,2))
        FROM   stg.fact_machine_telemetry_daily AS s
        LEFT JOIN dw.dim_machine AS ka ON ka.machine_id = s.machine_id AND ka.machine_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_machine AS kc ON kc.machine_id = s.machine_id AND kc.is_current = 1 AND kc.machine_key <> -1
        GROUP BY COALESCE(ka.machine_key, kc.machine_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_machine_telemetry_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_machine_telemetry_daily AS u WHERE u.machine_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_machine_telemetry_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_machine_telemetry_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_machine_telemetry_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
    -- would only see some plants and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_machine @run_id = @run_id;
        EXEC etl.usp_load_fact_maintenance_order @run_id = @run_id;
        EXEC etl.usp_load_fact_machine_telemetry_daily @run_id = @run_id;
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
