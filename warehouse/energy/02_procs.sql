/* Southern Grid Transmission (Energy & utilities) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.asset ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_asset: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_asset @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_asset';
        -- 1. Stage every Silver version of each asset touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_asset;
        INSERT INTO stg.dim_asset (asset_id, asset_tag, asset_name, asset_class, region, status, criticality, rating_mva, install_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.asset_id, sv.asset_tag, sv.asset_name, sv.asset_class, sv.region, sv.status, sv.criticality, sv.rating_mva, sv.install_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.asset AS sv
        WHERE  sv.asset_id IN (SELECT svn.asset_id FROM lh_mdf.silver.asset AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_asset;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(asset_key), 0) FROM dw.dim_asset);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, criticality or rating_mva changed).
        --    A new member's first version is valid from 1900-01-01 so older work orders still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_asset (asset_key, asset_id, asset_tag, asset_name, asset_class, region, status, criticality, rating_mva, install_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.asset_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.asset_id, v.asset_tag, v.asset_name, v.asset_class, v.region, v.status, v.criticality, v.rating_mva, v.install_date,
               CASE WHEN c.asset_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.asset_id, n.asset_tag, n.asset_name, n.asset_class, n.region, n.status, n.criticality, n.rating_mva, n.install_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.criticality) OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_criticality,
                        LAG(n.rating_mva) OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_rating_mva,
                        ROW_NUMBER() OVER (PARTITION BY n.asset_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_asset AS n
                LEFT JOIN (SELECT h.asset_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_asset AS h
                           WHERE h.asset_key <> -1 GROUP BY h.asset_id) AS l ON l.asset_id = n.asset_id
                WHERE  l.asset_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_asset AS c ON c.asset_id = v.asset_id AND c.is_current = 1 AND c.asset_key <> -1
        WHERE (v.rn = 1 AND c.asset_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.criticality <> c.criticality OR (v.criticality IS NULL AND c.criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND c.criticality IS NULL)) OR (v.rating_mva <> c.rating_mva OR (v.rating_mva IS NULL AND c.rating_mva IS NOT NULL) OR (v.rating_mva IS NOT NULL AND c.rating_mva IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.criticality <> v.prev_criticality OR (v.criticality IS NULL AND v.prev_criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND v.prev_criticality IS NULL)) OR (v.rating_mva <> v.prev_rating_mva OR (v.rating_mva IS NULL AND v.prev_rating_mva IS NOT NULL) OR (v.rating_mva IS NOT NULL AND v.prev_rating_mva IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_asset AS d
        INNER JOIN (
            SELECT h.asset_key, LEAD(h.valid_from) OVER (PARTITION BY h.asset_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_asset AS h
            WHERE  h.asset_key <> -1 AND h.asset_id IN (SELECT t.asset_id FROM stg.dim_asset AS t)
        ) AS x ON x.asset_key = d.asset_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.asset_tag = s.asset_tag,
               d.asset_name = s.asset_name,
               d.asset_class = s.asset_class,
               d.region = s.region,
               d.install_date = s.install_date
        FROM   dw.dim_asset AS d
        INNER JOIN stg.dim_asset AS s ON s.asset_id = d.asset_id AND s.src_is_current = 1
        WHERE  d.asset_key <> -1
          AND ((d.asset_tag <> s.asset_tag OR (d.asset_tag IS NULL AND s.asset_tag IS NOT NULL) OR (d.asset_tag IS NOT NULL AND s.asset_tag IS NULL))
            OR (d.asset_name <> s.asset_name OR (d.asset_name IS NULL AND s.asset_name IS NOT NULL) OR (d.asset_name IS NOT NULL AND s.asset_name IS NULL))
            OR (d.asset_class <> s.asset_class OR (d.asset_class IS NULL AND s.asset_class IS NOT NULL) OR (d.asset_class IS NOT NULL AND s.asset_class IS NULL))
            OR (d.region <> s.region OR (d.region IS NULL AND s.region IS NOT NULL) OR (d.region IS NOT NULL AND s.region IS NULL))
            OR (d.install_date <> s.install_date OR (d.install_date IS NULL AND s.install_date IS NOT NULL) OR (d.install_date IS NOT NULL AND s.install_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('assets staged with full history: ', (SELECT COUNT(DISTINCT asset_id) FROM stg.dim_asset));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_asset', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_asset', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_asset', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_work_order: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_work_order @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_work_order';
        -- 1. Stage work orders changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_work_order;
        INSERT INTO stg.fact_work_order (wo_id, asset_id, wo_type, priority, status, raised_ts, completed_ts, cost_aud, labour_hours, _is_deleted, _silver_updated_at)
        SELECT sv.wo_id, sv.asset_id, sv.wo_type, sv.priority, sv.status, sv.raised_ts, sv.completed_ts, sv.cost_aud, sv.labour_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.work_order AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_work_order;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = asset version valid when the work order was raised, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_work_order AS t
        USING (
        SELECT s.wo_id,
               COALESCE(ka.asset_key, kc.asset_key, -1) AS asset_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.raised_ts, 112)), -1) AS opened_date_key,
               s.wo_type, s.priority, s.status, s.raised_ts, s.completed_ts,
               CAST(DATEDIFF(MINUTE, s.raised_ts, s.completed_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('OPEN', 'IN_PROGRESS') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.cost_aud, s.labour_hours, s._is_deleted
        FROM   stg.fact_work_order AS s
        LEFT JOIN dw.dim_asset AS ka ON ka.asset_id = s.asset_id AND ka.asset_key <> -1 AND s.raised_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.raised_ts < ka.valid_to)
        LEFT JOIN dw.dim_asset AS kc ON kc.asset_id = s.asset_id AND kc.is_current = 1 AND kc.asset_key <> -1
        ) AS src
        ON t.wo_id = src.wo_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.asset_key = src.asset_key, t.opened_date_key = src.opened_date_key, t.wo_type = src.wo_type, t.priority = src.priority, t.status = src.status, t.raised_ts = src.raised_ts, t.completed_ts = src.completed_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.cost_aud = src.cost_aud, t.labour_hours = src.labour_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (wo_id, asset_key, opened_date_key, wo_type, priority, status, raised_ts, completed_ts, hours_to_close, is_open, cost_aud, labour_hours)
            VALUES (src.wo_id, src.asset_key, src.opened_date_key, src.wo_type, src.priority, src.status, src.raised_ts, src.completed_ts, src.hours_to_close, src.is_open, src.cost_aud, src.labour_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving asset: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_work_order', f.wo_id, s.asset_id, s.raised_ts, @run_id, @now
        FROM   dw.fact_work_order AS f
        INNER JOIN stg.fact_work_order AS s ON s.wo_id = f.wo_id
        WHERE  f.asset_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_work_order' AND la.business_key = f.wo_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the asset exists in the dimension
        UPDATE f
        SET    f.asset_key = COALESCE(ka.asset_key, kc.asset_key, -1)
        FROM   dw.fact_work_order AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_work_order' AND l.business_key = f.wo_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_asset AS ka ON ka.asset_id = l.entity_id AND ka.asset_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_asset AS kc ON kc.asset_id = l.entity_id AND kc.is_current = 1 AND kc.asset_key <> -1
        WHERE  f.asset_key = -1
          AND  COALESCE(ka.asset_key, kc.asset_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_work_order AS f ON f.wo_id = l.business_key
        WHERE  l.object_name = 'fact_work_order' AND l.resolved_at IS NULL AND f.asset_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_work_order AS u WHERE u.asset_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_work_order', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_work_order', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_work_order', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_tx_load_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_tx_load_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_tx_load_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late asset are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_tx_load_daily;
        INSERT INTO stg.fact_tx_load_daily (asset_id, reading_ts, oil_temp_c, load_mw, ambient_temp_c, _ingested_at)
        SELECT sv.asset_id, sv.reading_ts, sv.oil_temp_c, sv.load_mw, sv.ambient_temp_c, sv._ingested_at
        FROM   lh_mdf.silver.tx_telemetry AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.tx_telemetry AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_tx_load_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.asset_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_tx_load_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_tx_load_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_tx_load_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the asset version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_tx_load_daily (asset_key, reading_date_key, readings, avg_oil_temp_c, max_oil_temp_c, breach_readings, avg_load_mw, avg_ambient_temp_c)
        SELECT COALESCE(ka.asset_key, kc.asset_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.oil_temp_c) AS DECIMAL(18,2)), CAST(MAX(s.oil_temp_c) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.oil_temp_c > 95 THEN 1 ELSE 0 END),
               CAST(AVG(s.load_mw) AS DECIMAL(18,2)), CAST(AVG(s.ambient_temp_c) AS DECIMAL(18,2))
        FROM   stg.fact_tx_load_daily AS s
        LEFT JOIN dw.dim_asset AS ka ON ka.asset_id = s.asset_id AND ka.asset_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_asset AS kc ON kc.asset_id = s.asset_id AND kc.is_current = 1 AND kc.asset_key <> -1
        GROUP BY COALESCE(ka.asset_key, kc.asset_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_tx_load_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_tx_load_daily AS u WHERE u.asset_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_tx_load_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_tx_load_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_tx_load_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
    -- would only see some regions and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_dim_asset @run_id = @run_id;
        EXEC etl.usp_load_fact_work_order @run_id = @run_id;
        EXEC etl.usp_load_fact_tx_load_daily @run_id = @run_id;
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
