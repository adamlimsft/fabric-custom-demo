/* Coastal Health Network (Healthcare) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.ward ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_ward: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_ward @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_ward';
        -- 1. Stage every Silver version of each ward touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_ward;
        INSERT INTO stg.dim_ward (ward_id, ward_code, ward_name, ward_type, hospital, status, acuity_level, bed_count, opened_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.ward_id, sv.ward_code, sv.ward_name, sv.ward_type, sv.hospital, sv.status, sv.acuity_level, sv.bed_count, sv.opened_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.ward AS sv
        WHERE  sv.ward_id IN (SELECT svn.ward_id FROM lh_mdf.silver.ward AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_ward;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(ward_key), 0) FROM dw.dim_ward);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, acuity_level or bed_count changed).
        --    A new member's first version is valid from 1900-01-01 so older admissions still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_ward (ward_key, ward_id, ward_code, ward_name, ward_type, hospital, status, acuity_level, bed_count, opened_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.ward_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.ward_id, v.ward_code, v.ward_name, v.ward_type, v.hospital, v.status, v.acuity_level, v.bed_count, v.opened_date,
               CASE WHEN c.ward_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.ward_id, n.ward_code, n.ward_name, n.ward_type, n.hospital, n.status, n.acuity_level, n.bed_count, n.opened_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.ward_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.acuity_level) OVER (PARTITION BY n.ward_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_acuity_level,
                        LAG(n.bed_count) OVER (PARTITION BY n.ward_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_bed_count,
                        ROW_NUMBER() OVER (PARTITION BY n.ward_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_ward AS n
                LEFT JOIN (SELECT h.ward_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_ward AS h
                           WHERE h.ward_key <> -1 GROUP BY h.ward_id) AS l ON l.ward_id = n.ward_id
                WHERE  l.ward_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_ward AS c ON c.ward_id = v.ward_id AND c.is_current = 1 AND c.ward_key <> -1
        WHERE (v.rn = 1 AND c.ward_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.acuity_level <> c.acuity_level OR (v.acuity_level IS NULL AND c.acuity_level IS NOT NULL) OR (v.acuity_level IS NOT NULL AND c.acuity_level IS NULL)) OR (v.bed_count <> c.bed_count OR (v.bed_count IS NULL AND c.bed_count IS NOT NULL) OR (v.bed_count IS NOT NULL AND c.bed_count IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.acuity_level <> v.prev_acuity_level OR (v.acuity_level IS NULL AND v.prev_acuity_level IS NOT NULL) OR (v.acuity_level IS NOT NULL AND v.prev_acuity_level IS NULL)) OR (v.bed_count <> v.prev_bed_count OR (v.bed_count IS NULL AND v.prev_bed_count IS NOT NULL) OR (v.bed_count IS NOT NULL AND v.prev_bed_count IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_ward AS d
        INNER JOIN (
            SELECT h.ward_key, LEAD(h.valid_from) OVER (PARTITION BY h.ward_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_ward AS h
            WHERE  h.ward_key <> -1 AND h.ward_id IN (SELECT t.ward_id FROM stg.dim_ward AS t)
        ) AS x ON x.ward_key = d.ward_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.ward_code = s.ward_code,
               d.ward_name = s.ward_name,
               d.ward_type = s.ward_type,
               d.hospital = s.hospital,
               d.opened_date = s.opened_date
        FROM   dw.dim_ward AS d
        INNER JOIN stg.dim_ward AS s ON s.ward_id = d.ward_id AND s.src_is_current = 1
        WHERE  d.ward_key <> -1
          AND ((d.ward_code <> s.ward_code OR (d.ward_code IS NULL AND s.ward_code IS NOT NULL) OR (d.ward_code IS NOT NULL AND s.ward_code IS NULL))
            OR (d.ward_name <> s.ward_name OR (d.ward_name IS NULL AND s.ward_name IS NOT NULL) OR (d.ward_name IS NOT NULL AND s.ward_name IS NULL))
            OR (d.ward_type <> s.ward_type OR (d.ward_type IS NULL AND s.ward_type IS NOT NULL) OR (d.ward_type IS NOT NULL AND s.ward_type IS NULL))
            OR (d.hospital <> s.hospital OR (d.hospital IS NULL AND s.hospital IS NOT NULL) OR (d.hospital IS NOT NULL AND s.hospital IS NULL))
            OR (d.opened_date <> s.opened_date OR (d.opened_date IS NULL AND s.opened_date IS NOT NULL) OR (d.opened_date IS NOT NULL AND s.opened_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('wards staged with full history: ', (SELECT COUNT(DISTINCT ward_id) FROM stg.dim_ward));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_ward', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_ward', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_ward', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_admission: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_admission @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_admission';
        -- 1. Stage admissions changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_admission;
        INSERT INTO stg.fact_admission (admission_id, ward_id, admission_type, priority, status, admit_ts, discharge_ts, episode_cost_aud, nursing_hours, _is_deleted, _silver_updated_at)
        SELECT sv.admission_id, sv.ward_id, sv.admission_type, sv.priority, sv.status, sv.admit_ts, sv.discharge_ts, sv.episode_cost_aud, sv.nursing_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.admission AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_admission;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = ward version valid when the admission was admitted, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_admission AS t
        USING (
        SELECT s.admission_id,
               COALESCE(ka.ward_key, kc.ward_key, -1) AS ward_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.admit_ts, 112)), -1) AS opened_date_key,
               s.admission_type, s.priority, s.status, s.admit_ts, s.discharge_ts,
               CAST(DATEDIFF(MINUTE, s.admit_ts, s.discharge_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('ADMITTED', 'IN_TREATMENT') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.episode_cost_aud, s.nursing_hours, s._is_deleted
        FROM   stg.fact_admission AS s
        LEFT JOIN dw.dim_ward AS ka ON ka.ward_id = s.ward_id AND ka.ward_key <> -1 AND s.admit_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.admit_ts < ka.valid_to)
        LEFT JOIN dw.dim_ward AS kc ON kc.ward_id = s.ward_id AND kc.is_current = 1 AND kc.ward_key <> -1
        ) AS src
        ON t.admission_id = src.admission_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.ward_key = src.ward_key, t.opened_date_key = src.opened_date_key, t.admission_type = src.admission_type, t.priority = src.priority, t.status = src.status, t.admit_ts = src.admit_ts, t.discharge_ts = src.discharge_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.episode_cost_aud = src.episode_cost_aud, t.nursing_hours = src.nursing_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (admission_id, ward_key, opened_date_key, admission_type, priority, status, admit_ts, discharge_ts, hours_to_close, is_open, episode_cost_aud, nursing_hours)
            VALUES (src.admission_id, src.ward_key, src.opened_date_key, src.admission_type, src.priority, src.status, src.admit_ts, src.discharge_ts, src.hours_to_close, src.is_open, src.episode_cost_aud, src.nursing_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving ward: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_admission', f.admission_id, s.ward_id, s.admit_ts, @run_id, @now
        FROM   dw.fact_admission AS f
        INNER JOIN stg.fact_admission AS s ON s.admission_id = f.admission_id
        WHERE  f.ward_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_admission' AND la.business_key = f.admission_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the ward exists in the dimension
        UPDATE f
        SET    f.ward_key = COALESCE(ka.ward_key, kc.ward_key, -1)
        FROM   dw.fact_admission AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_admission' AND l.business_key = f.admission_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_ward AS ka ON ka.ward_id = l.entity_id AND ka.ward_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_ward AS kc ON kc.ward_id = l.entity_id AND kc.is_current = 1 AND kc.ward_key <> -1
        WHERE  f.ward_key = -1
          AND  COALESCE(ka.ward_key, kc.ward_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_admission AS f ON f.admission_id = l.business_key
        WHERE  l.object_name = 'fact_admission' AND l.resolved_at IS NULL AND f.ward_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_admission AS u WHERE u.ward_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_admission', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_admission', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_admission', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_ward_occupancy_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_ward_occupancy_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_ward_occupancy_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late ward are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_ward_occupancy_daily;
        INSERT INTO stg.fact_ward_occupancy_daily (ward_id, reading_ts, occupancy_pct, patients_waiting, nurses_on_shift, _ingested_at)
        SELECT sv.ward_id, sv.reading_ts, sv.occupancy_pct, sv.patients_waiting, sv.nurses_on_shift, sv._ingested_at
        FROM   lh_mdf.silver.ward_occupancy AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.ward_occupancy AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_ward_occupancy_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.ward_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_ward_occupancy_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_ward_occupancy_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_ward_occupancy_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the ward version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_ward_occupancy_daily (ward_key, reading_date_key, readings, avg_occupancy_pct, max_occupancy_pct, breach_readings, avg_patients_waiting, avg_nurses_on_shift)
        SELECT COALESCE(ka.ward_key, kc.ward_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.occupancy_pct) AS DECIMAL(18,2)), CAST(MAX(s.occupancy_pct) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.occupancy_pct > 95 THEN 1 ELSE 0 END),
               CAST(AVG(s.patients_waiting) AS DECIMAL(18,2)), CAST(AVG(s.nurses_on_shift) AS DECIMAL(18,2))
        FROM   stg.fact_ward_occupancy_daily AS s
        LEFT JOIN dw.dim_ward AS ka ON ka.ward_id = s.ward_id AND ka.ward_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_ward AS kc ON kc.ward_id = s.ward_id AND kc.is_current = 1 AND kc.ward_key <> -1
        GROUP BY COALESCE(ka.ward_key, kc.ward_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_ward_occupancy_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_ward_occupancy_daily AS u WHERE u.ward_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_ward_occupancy_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_ward_occupancy_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_ward_occupancy_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
    -- would only see some hospitals and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_ward @run_id = @run_id;
        EXEC etl.usp_load_fact_admission @run_id = @run_id;
        EXEC etl.usp_load_fact_ward_occupancy_daily @run_id = @run_id;
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
