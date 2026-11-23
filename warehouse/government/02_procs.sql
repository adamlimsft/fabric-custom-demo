/* Riverbend Regional Authority (Government) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.public_asset ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_public_asset: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_public_asset @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_public_asset';
        -- 1. Stage every Silver version of each public asset touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_public_asset;
        INSERT INTO stg.dim_public_asset (asset_id, asset_ref, asset_name, asset_type, district, status, criticality, replacement_value_kaud, constructed_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.asset_id, sv.asset_ref, sv.asset_name, sv.asset_type, sv.district, sv.status, sv.criticality, sv.replacement_value_kaud, sv.constructed_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.public_asset AS sv
        WHERE  sv.asset_id IN (SELECT svn.asset_id FROM lh_mdf.silver.public_asset AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_public_asset;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(public_asset_key), 0) FROM dw.dim_public_asset);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, criticality or replacement_value_kaud changed).
        --    A new member's first version is valid from 1900-01-01 so older citizen cases still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_public_asset (public_asset_key, asset_id, asset_ref, asset_name, asset_type, district, status, criticality, replacement_value_kaud, constructed_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.asset_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.asset_id, v.asset_ref, v.asset_name, v.asset_type, v.district, v.status, v.criticality, v.replacement_value_kaud, v.constructed_date,
               CASE WHEN c.asset_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.asset_id, n.asset_ref, n.asset_name, n.asset_type, n.district, n.status, n.criticality, n.replacement_value_kaud, n.constructed_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.criticality) OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_criticality,
                        LAG(n.replacement_value_kaud) OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_replacement_value_kaud,
                        ROW_NUMBER() OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_public_asset AS n
                LEFT JOIN (SELECT h.asset_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_public_asset AS h
                           WHERE h.public_asset_key <> -1 GROUP BY h.asset_id) AS l ON l.asset_id = n.asset_id
                WHERE  l.asset_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_public_asset AS c ON c.asset_id = v.asset_id AND c.is_current = 1 AND c.public_asset_key <> -1
        WHERE (v.rn = 1 AND c.asset_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.criticality <> c.criticality OR (v.criticality IS NULL AND c.criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND c.criticality IS NULL)) OR (v.replacement_value_kaud <> c.replacement_value_kaud OR (v.replacement_value_kaud IS NULL AND c.replacement_value_kaud IS NOT NULL) OR (v.replacement_value_kaud IS NOT NULL AND c.replacement_value_kaud IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.criticality <> v.prev_criticality OR (v.criticality IS NULL AND v.prev_criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND v.prev_criticality IS NULL)) OR (v.replacement_value_kaud <> v.prev_replacement_value_kaud OR (v.replacement_value_kaud IS NULL AND v.prev_replacement_value_kaud IS NOT NULL) OR (v.replacement_value_kaud IS NOT NULL AND v.prev_replacement_value_kaud IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_public_asset AS d
        INNER JOIN (
            SELECT h.public_asset_key, LEAD(h.valid_from) OVER (PARTITION BY h.asset_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_public_asset AS h
            WHERE  h.public_asset_key <> -1 AND h.asset_id IN (SELECT t.asset_id FROM stg.dim_public_asset AS t)
        ) AS x ON x.public_asset_key = d.public_asset_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.asset_ref = s.asset_ref,
               d.asset_name = s.asset_name,
               d.asset_type = s.asset_type,
               d.district = s.district,
               d.constructed_date = s.constructed_date
        FROM   dw.dim_public_asset AS d
        INNER JOIN stg.dim_public_asset AS s ON s.asset_id = d.asset_id AND s.src_is_current = 1
        WHERE  d.public_asset_key <> -1
          AND ((d.asset_ref <> s.asset_ref OR (d.asset_ref IS NULL AND s.asset_ref IS NOT NULL) OR (d.asset_ref IS NOT NULL AND s.asset_ref IS NULL))
            OR (d.asset_name <> s.asset_name OR (d.asset_name IS NULL AND s.asset_name IS NOT NULL) OR (d.asset_name IS NOT NULL AND s.asset_name IS NULL))
            OR (d.asset_type <> s.asset_type OR (d.asset_type IS NULL AND s.asset_type IS NOT NULL) OR (d.asset_type IS NOT NULL AND s.asset_type IS NULL))
            OR (d.district <> s.district OR (d.district IS NULL AND s.district IS NOT NULL) OR (d.district IS NOT NULL AND s.district IS NULL))
            OR (d.constructed_date <> s.constructed_date OR (d.constructed_date IS NULL AND s.constructed_date IS NOT NULL) OR (d.constructed_date IS NOT NULL AND s.constructed_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('public assets staged with full history: ', (SELECT COUNT(DISTINCT asset_id) FROM stg.dim_public_asset));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_public_asset', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_public_asset', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_public_asset', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_citizen_case: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_citizen_case @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_citizen_case';
        -- 1. Stage citizen cases changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_citizen_case;
        INSERT INTO stg.fact_citizen_case (case_id, asset_id, case_type, priority, status, lodged_ts, completed_ts, cost_aud, crew_hours, _is_deleted, _silver_updated_at)
        SELECT sv.case_id, sv.asset_id, sv.case_type, sv.priority, sv.status, sv.lodged_ts, sv.completed_ts, sv.cost_aud, sv.crew_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.citizen_case AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_citizen_case;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = public asset version valid when the citizen case was lodged, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_citizen_case AS t
        USING (
        SELECT s.case_id,
               COALESCE(ka.public_asset_key, kc.public_asset_key, -1) AS public_asset_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.lodged_ts, 112)), -1) AS opened_date_key,
               s.case_type, s.priority, s.status, s.lodged_ts, s.completed_ts,
               CAST(DATEDIFF(MINUTE, s.lodged_ts, s.completed_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('LODGED', 'ASSIGNED') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.cost_aud, s.crew_hours, s._is_deleted
        FROM   stg.fact_citizen_case AS s
        LEFT JOIN dw.dim_public_asset AS ka ON ka.asset_id = s.asset_id AND ka.public_asset_key <> -1 AND s.lodged_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.lodged_ts < ka.valid_to)
        LEFT JOIN dw.dim_public_asset AS kc ON kc.asset_id = s.asset_id AND kc.is_current = 1 AND kc.public_asset_key <> -1
        ) AS src
        ON t.case_id = src.case_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.public_asset_key = src.public_asset_key, t.opened_date_key = src.opened_date_key, t.case_type = src.case_type, t.priority = src.priority, t.status = src.status, t.lodged_ts = src.lodged_ts, t.completed_ts = src.completed_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.cost_aud = src.cost_aud, t.crew_hours = src.crew_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (case_id, public_asset_key, opened_date_key, case_type, priority, status, lodged_ts, completed_ts, hours_to_close, is_open, cost_aud, crew_hours)
            VALUES (src.case_id, src.public_asset_key, src.opened_date_key, src.case_type, src.priority, src.status, src.lodged_ts, src.completed_ts, src.hours_to_close, src.is_open, src.cost_aud, src.crew_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving public asset: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_citizen_case', f.case_id, s.asset_id, s.lodged_ts, @run_id, @now
        FROM   dw.fact_citizen_case AS f
        INNER JOIN stg.fact_citizen_case AS s ON s.case_id = f.case_id
        WHERE  f.public_asset_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_citizen_case' AND la.business_key = f.case_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the public asset exists in the dimension
        UPDATE f
        SET    f.public_asset_key = COALESCE(ka.public_asset_key, kc.public_asset_key, -1)
        FROM   dw.fact_citizen_case AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_citizen_case' AND l.business_key = f.case_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_public_asset AS ka ON ka.asset_id = l.entity_id AND ka.public_asset_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_public_asset AS kc ON kc.asset_id = l.entity_id AND kc.is_current = 1 AND kc.public_asset_key <> -1
        WHERE  f.public_asset_key = -1
          AND  COALESCE(ka.public_asset_key, kc.public_asset_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_citizen_case AS f ON f.case_id = l.business_key
        WHERE  l.object_name = 'fact_citizen_case' AND l.resolved_at IS NULL AND f.public_asset_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_citizen_case AS u WHERE u.public_asset_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_citizen_case', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_citizen_case', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_citizen_case', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_sensor_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_sensor_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_sensor_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late public asset are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_sensor_daily;
        INSERT INTO stg.fact_sensor_daily (asset_id, reading_ts, water_level_cm, traffic_count, pm25_ugm3, _ingested_at)
        SELECT sv.asset_id, sv.reading_ts, sv.water_level_cm, sv.traffic_count, sv.pm25_ugm3, sv._ingested_at
        FROM   lh_mdf.silver.sensor_reading AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.sensor_reading AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_sensor_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.public_asset_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_sensor_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_sensor_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_sensor_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the public asset version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_sensor_daily (public_asset_key, reading_date_key, readings, avg_water_level_cm, max_water_level_cm, breach_readings, avg_traffic_count, avg_pm25_ugm3)
        SELECT COALESCE(ka.public_asset_key, kc.public_asset_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.water_level_cm) AS DECIMAL(18,2)), CAST(MAX(s.water_level_cm) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.water_level_cm > 150 THEN 1 ELSE 0 END),
               CAST(AVG(s.traffic_count) AS DECIMAL(18,2)), CAST(AVG(s.pm25_ugm3) AS DECIMAL(18,2))
        FROM   stg.fact_sensor_daily AS s
        LEFT JOIN dw.dim_public_asset AS ka ON ka.asset_id = s.asset_id AND ka.public_asset_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_public_asset AS kc ON kc.asset_id = s.asset_id AND kc.is_current = 1 AND kc.public_asset_key <> -1
        GROUP BY COALESCE(ka.public_asset_key, kc.public_asset_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_sensor_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_sensor_daily AS u WHERE u.public_asset_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_sensor_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_sensor_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_sensor_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
    -- would only see some districts and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_public_asset @run_id = @run_id;
        EXEC etl.usp_load_fact_citizen_case @run_id = @run_id;
        EXEC etl.usp_load_fact_sensor_daily @run_id = @run_id;
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
