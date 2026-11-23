/* Southern Cross Telecom (Telecommunications) - Warehouse in Gold, script 2 of 3: stored-procedure ELT
   Reads Silver in lh_mdf through 3-part names (lh_mdf.silver.network_element ...). Each procedure: watermark -> stage ->
   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.
   Run it once to create the procedures, then: EXEC etl.usp_run_all; */

-- dw.dim_network_element: SCD2 replayed from Silver history
CREATE OR ALTER PROCEDURE etl.usp_load_dim_network_element @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'dim_network_element';
        -- 1. Stage every Silver version of each network element touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE stg.dim_network_element;
        INSERT INTO stg.dim_network_element (element_id, element_code, element_name, element_type, region, status, criticality, capacity_users, install_date, src_valid_from, src_is_current, src_updated_at)
        SELECT sv.element_id, sv.element_code, sv.element_name, sv.element_type, sv.region, sv.status, sv.criticality, sv.capacity_users, sv.install_date, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   lh_mdf.silver.network_element AS sv
        WHERE  sv.element_id IN (SELECT svn.element_id FROM lh_mdf.silver.network_element AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM stg.dim_network_element;
        DECLARE @max_key INT = (SELECT COALESCE(MAX(network_element_key), 0) FROM dw.dim_network_element);
        DECLARE @min_from DATETIME2(6) = CAST('1900-01-01' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, criticality or capacity_users changed).
        --    A new member's first version is valid from 1900-01-01 so older fault tickets still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO dw.dim_network_element (network_element_key, element_id, element_code, element_name, element_type, region, status, criticality, capacity_users, install_date, valid_from, valid_to, is_current)
        SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.element_id, v.src_valid_from, v.src_updated_at) AS new_key,
               v.element_id, v.element_code, v.element_name, v.element_type, v.region, v.status, v.criticality, v.capacity_users, v.install_date,
               CASE WHEN c.element_id IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,
               NULL AS valid_to, 1 AS is_current
        FROM  (SELECT n.element_id, n.element_code, n.element_name, n.element_type, n.region, n.status, n.criticality, n.capacity_users, n.install_date, n.src_valid_from, n.src_updated_at,
                        LAG(n.status) OVER (PARTITION BY n.element_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_status,
                        LAG(n.criticality) OVER (PARTITION BY n.element_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_criticality,
                        LAG(n.capacity_users) OVER (PARTITION BY n.element_id ORDER BY n.src_valid_from, n.src_updated_at) AS prev_capacity_users,
                        ROW_NUMBER() OVER (PARTITION BY n.element_id ORDER BY n.src_valid_from, n.src_updated_at) AS rn
                FROM   stg.dim_network_element AS n
                LEFT JOIN (SELECT h.element_id, MAX(h.valid_from) AS max_valid_from FROM dw.dim_network_element AS h
                           WHERE h.network_element_key <> -1 GROUP BY h.element_id) AS l ON l.element_id = n.element_id
                WHERE  l.element_id IS NULL OR n.src_valid_from > l.max_valid_from) AS v
        LEFT JOIN dw.dim_network_element AS c ON c.element_id = v.element_id AND c.is_current = 1 AND c.network_element_key <> -1
        WHERE (v.rn = 1 AND c.element_id IS NULL)
           OR (v.rn = 1 AND ((v.status <> c.status OR (v.status IS NULL AND c.status IS NOT NULL) OR (v.status IS NOT NULL AND c.status IS NULL)) OR (v.criticality <> c.criticality OR (v.criticality IS NULL AND c.criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND c.criticality IS NULL)) OR (v.capacity_users <> c.capacity_users OR (v.capacity_users IS NULL AND c.capacity_users IS NOT NULL) OR (v.capacity_users IS NOT NULL AND c.capacity_users IS NULL))))
           OR (v.rn > 1 AND ((v.status <> v.prev_status OR (v.status IS NULL AND v.prev_status IS NOT NULL) OR (v.status IS NOT NULL AND v.prev_status IS NULL)) OR (v.criticality <> v.prev_criticality OR (v.criticality IS NULL AND v.prev_criticality IS NOT NULL) OR (v.criticality IS NOT NULL AND v.prev_criticality IS NULL)) OR (v.capacity_users <> v.prev_capacity_users OR (v.capacity_users IS NULL AND v.prev_capacity_users IS NOT NULL) OR (v.capacity_users IS NOT NULL AND v.prev_capacity_users IS NULL))));
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   dw.dim_network_element AS d
        INNER JOIN (
            SELECT h.network_element_key, LEAD(h.valid_from) OVER (PARTITION BY h.element_id ORDER BY h.valid_from) AS next_valid_from
            FROM   dw.dim_network_element AS h
            WHERE  h.network_element_key <> -1 AND h.element_id IN (SELECT t.element_id FROM stg.dim_network_element AS t)
        ) AS x ON x.network_element_key = d.network_element_key
        WHERE  (d.valid_to <> x.next_valid_from OR (d.valid_to IS NULL AND x.next_valid_from IS NOT NULL) OR (d.valid_to IS NOT NULL AND x.next_valid_from IS NULL))
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    d.element_code = s.element_code,
               d.element_name = s.element_name,
               d.element_type = s.element_type,
               d.region = s.region,
               d.install_date = s.install_date
        FROM   dw.dim_network_element AS d
        INNER JOIN stg.dim_network_element AS s ON s.element_id = d.element_id AND s.src_is_current = 1
        WHERE  d.network_element_key <> -1
          AND ((d.element_code <> s.element_code OR (d.element_code IS NULL AND s.element_code IS NOT NULL) OR (d.element_code IS NOT NULL AND s.element_code IS NULL))
            OR (d.element_name <> s.element_name OR (d.element_name IS NULL AND s.element_name IS NOT NULL) OR (d.element_name IS NOT NULL AND s.element_name IS NULL))
            OR (d.element_type <> s.element_type OR (d.element_type IS NULL AND s.element_type IS NOT NULL) OR (d.element_type IS NOT NULL AND s.element_type IS NULL))
            OR (d.region <> s.region OR (d.region IS NULL AND s.region IS NOT NULL) OR (d.region IS NOT NULL AND s.region IS NULL))
            OR (d.install_date <> s.install_date OR (d.install_date IS NULL AND s.install_date IS NOT NULL) OR (d.install_date IS NOT NULL AND s.install_date IS NULL)));
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('network elements staged with full history: ', (SELECT COUNT(DISTINCT element_id) FROM stg.dim_network_element));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('dim_network_element', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_network_element', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'dim_network_element', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_fault_ticket: incremental MERGE, as-at key lookup, late-arriving repair
CREATE OR ALTER PROCEDURE etl.usp_load_fact_fault_ticket @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_fault_ticket';
        -- 1. Stage fault tickets changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE stg.fact_fault_ticket;
        INSERT INTO stg.fact_fault_ticket (ticket_id, element_id, fault_category, priority, status, opened_ts, resolved_ts, field_cost_aud, engineer_hours, _is_deleted, _silver_updated_at)
        SELECT sv.ticket_id, sv.element_id, sv.fault_category, sv.priority, sv.status, sv.opened_ts, sv.resolved_ts, sv.field_cost_aud, sv.engineer_hours, sv._is_deleted, sv._silver_updated_at
        FROM   lh_mdf.silver.fault_ticket AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM stg.fact_fault_ticket;

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = network element version valid when the fault ticket was opened, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE dw.fact_fault_ticket AS t
        USING (
        SELECT s.ticket_id,
               COALESCE(ka.network_element_key, kc.network_element_key, -1) AS network_element_key,
               COALESCE(CONVERT(INT, CONVERT(CHAR(8), s.opened_ts, 112)), -1) AS opened_date_key,
               s.fault_category, s.priority, s.status, s.opened_ts, s.resolved_ts,
               CAST(DATEDIFF(MINUTE, s.opened_ts, s.resolved_ts) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ('NEW', 'IN_PROGRESS') THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.field_cost_aud, s.engineer_hours, s._is_deleted
        FROM   stg.fact_fault_ticket AS s
        LEFT JOIN dw.dim_network_element AS ka ON ka.element_id = s.element_id AND ka.network_element_key <> -1 AND s.opened_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.opened_ts < ka.valid_to)
        LEFT JOIN dw.dim_network_element AS kc ON kc.element_id = s.element_id AND kc.is_current = 1 AND kc.network_element_key <> -1
        ) AS src
        ON t.ticket_id = src.ticket_id
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET t.network_element_key = src.network_element_key, t.opened_date_key = src.opened_date_key, t.fault_category = src.fault_category, t.priority = src.priority, t.status = src.status, t.opened_ts = src.opened_ts, t.resolved_ts = src.resolved_ts, t.hours_to_close = src.hours_to_close, t.is_open = src.is_open, t.field_cost_aud = src.field_cost_aud, t.engineer_hours = src.engineer_hours
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT (ticket_id, network_element_key, opened_date_key, fault_category, priority, status, opened_ts, resolved_ts, hours_to_close, is_open, field_cost_aud, engineer_hours)
            VALUES (src.ticket_id, src.network_element_key, src.opened_date_key, src.fault_category, src.priority, src.status, src.opened_ts, src.resolved_ts, src.hours_to_close, src.is_open, src.field_cost_aud, src.engineer_hours);
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving network element: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT 'fact_fault_ticket', f.ticket_id, s.element_id, s.opened_ts, @run_id, @now
        FROM   dw.fact_fault_ticket AS f
        INNER JOIN stg.fact_fault_ticket AS s ON s.ticket_id = f.ticket_id
        WHERE  f.network_element_key = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = 'fact_fault_ticket' AND la.business_key = f.ticket_id AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the network element exists in the dimension
        UPDATE f
        SET    f.network_element_key = COALESCE(ka.network_element_key, kc.network_element_key, -1)
        FROM   dw.fact_fault_ticket AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = 'fact_fault_ticket' AND l.business_key = f.ticket_id AND l.resolved_at IS NULL
        LEFT JOIN dw.dim_network_element AS ka ON ka.element_id = l.entity_id AND ka.network_element_key <> -1 AND l.event_ts >= ka.valid_from AND (ka.valid_to IS NULL OR l.event_ts < ka.valid_to)
        LEFT JOIN dw.dim_network_element AS kc ON kc.element_id = l.entity_id AND kc.is_current = 1 AND kc.network_element_key <> -1
        WHERE  f.network_element_key = -1
          AND  COALESCE(ka.network_element_key, kc.network_element_key, -1) <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN dw.fact_fault_ticket AS f ON f.ticket_id = l.business_key
        WHERE  l.object_name = 'fact_fault_ticket' AND l.resolved_at IS NULL AND f.network_element_key <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM dw.fact_fault_ticket AS u WHERE u.network_element_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_fault_ticket', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_fault_ticket', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_fault_ticket', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO

-- dw.fact_cell_kpi_daily: daily aggregate, affected days recomputed
CREATE OR ALTER PROCEDURE etl.usp_load_fact_cell_kpi_daily @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('1900-01-01' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = 'fact_cell_kpi_daily';
        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late network element are re-keyed on a later run.
        TRUNCATE TABLE stg.fact_cell_kpi_daily;
        INSERT INTO stg.fact_cell_kpi_daily (element_id, reading_ts, prb_util_pct, active_users, drop_rate_pct, _ingested_at)
        SELECT sv.element_id, sv.reading_ts, sv.prb_util_pct, sv.active_users, sv.drop_rate_pct, sv._ingested_at
        FROM   lh_mdf.silver.cell_kpi AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM lh_mdf.silver.cell_kpi AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM dw.fact_cell_kpi_daily AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.network_element_key = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM stg.fact_cell_kpi_daily;

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM dw.fact_cell_kpi_daily
        WHERE  reading_date_key IN (SELECT DISTINCT CONVERT(INT, CONVERT(CHAR(8), x.reading_ts, 112)) FROM stg.fact_cell_kpi_daily AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the network element version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO dw.fact_cell_kpi_daily (network_element_key, reading_date_key, readings, avg_prb_util_pct, max_prb_util_pct, breach_readings, avg_active_users, avg_drop_rate_pct)
        SELECT COALESCE(ka.network_element_key, kc.network_element_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112)), COUNT(*),
               CAST(AVG(s.prb_util_pct) AS DECIMAL(18,2)), CAST(MAX(s.prb_util_pct) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.prb_util_pct > 90 THEN 1 ELSE 0 END),
               CAST(AVG(s.active_users) AS DECIMAL(18,2)), CAST(AVG(s.drop_rate_pct) AS DECIMAL(18,2))
        FROM   stg.fact_cell_kpi_daily AS s
        LEFT JOIN dw.dim_network_element AS ka ON ka.element_id = s.element_id AND ka.network_element_key <> -1 AND s.reading_ts >= ka.valid_from AND (ka.valid_to IS NULL OR s.reading_ts < ka.valid_to)
        LEFT JOIN dw.dim_network_element AS kc ON kc.element_id = s.element_id AND kc.is_current = 1 AND kc.network_element_key <> -1
        GROUP BY COALESCE(ka.network_element_key, kc.network_element_key, -1), CONVERT(INT, CONVERT(CHAR(8), s.reading_ts, 112));
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM stg.fact_cell_kpi_daily AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM dw.fact_cell_kpi_daily AS u WHERE u.network_element_key = -1));
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('fact_cell_kpi_daily', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_cell_kpi_daily', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, 'fact_cell_kpi_daily', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
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
        EXEC etl.usp_load_dim_network_element @run_id = @run_id;
        EXEC etl.usp_load_fact_fault_ticket @run_id = @run_id;
        EXEC etl.usp_load_fact_cell_kpi_daily @run_id = @run_id;
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
