/* =====================================================================================================
   Production variant of the MDF control tables - T-SQL for a Fabric SQL database (or Azure SQL)
   -----------------------------------------------------------------------------------------------------
   The demo keeps metadata in lakehouse Delta tables (zero extra items, works in any workspace).
   Many production frameworks move the CONTROL plane to a Fabric SQL database because it offers:
     * transactional, row-level updates from many parallel pipeline activities (watermarks, run status)
     * constraints / foreign keys that stop bad configuration being deployed
     * stored procedures the pipeline can call directly (Lookup / Script / Stored procedure activities)
     * easy editing (Power Apps / SSMS / VS Code) and it is mirrored into OneLake automatically for reporting
   This mirrors the SSIS/SQL control-table pattern most teams already run - which makes migration easier to explain.
   Column names match config/*.json, so the same JSON can be loaded into either store.
   ===================================================================================================== */

CREATE SCHEMA meta;
GO

CREATE TABLE meta.source_system (
    source_system        VARCHAR(50)   NOT NULL PRIMARY KEY,
    industry             VARCHAR(20)   NOT NULL,
    display_name         NVARCHAR(200) NOT NULL,
    system_type          NVARCHAR(100) NULL,
    connection_pattern   NVARCHAR(400) NULL,
    technical_owner      NVARCHAR(100) NULL,
    fabric_options       NVARCHAR(400) NULL
);

CREATE TABLE meta.ingestion_object (
    object_id            VARCHAR(50)   NOT NULL PRIMARY KEY,
    industry             VARCHAR(20)   NOT NULL,
    source_system        VARCHAR(50)   NOT NULL REFERENCES meta.source_system(source_system),
    source_object        VARCHAR(100)  NOT NULL,
    source_type          VARCHAR(20)   NOT NULL CHECK (source_type IN ('database','file','api','sharepoint')),
    ingest_method        VARCHAR(20)   NOT NULL CHECK (ingest_method IN ('copy_db','landed_file','shortcut','mirror')),
    landing_format       VARCHAR(10)   NOT NULL CHECK (landing_format IN ('csv','txt','json','xlsx','parquet')),
    landing_path         VARCHAR(300)  NOT NULL,
    read_options         NVARCHAR(1000) NULL CHECK (read_options IS NULL OR ISJSON(read_options) = 1),
    source_table         VARCHAR(200)  NULL,
    load_type            VARCHAR(20)   NOT NULL CHECK (load_type IN ('full','incremental')),
    watermark_column     VARCHAR(100)  NULL,
    primary_keys         VARCHAR(300)  NOT NULL,
    dedupe_order_by      VARCHAR(300)  NULL,
    bronze_table         VARCHAR(128)  NOT NULL,
    silver_table         VARCHAR(128)  NOT NULL,
    silver_write_mode    VARCHAR(20)   NOT NULL CHECK (silver_write_mode IN ('merge_scd1','merge_scd2','insert_only')),
    load_group           VARCHAR(30)   NOT NULL,
    priority             INT           NOT NULL DEFAULT 50,
    sla_hours            INT           NOT NULL DEFAULT 26,
    data_owner           NVARCHAR(100) NOT NULL,
    sensitivity          VARCHAR(20)   NOT NULL CHECK (sensitivity IN ('Public','Internal','Confidential','Restricted')),
    active               BIT           NOT NULL DEFAULT 1,
    description          NVARCHAR(1000) NULL,
    fabric_alternative   NVARCHAR(1000) NULL,
    CONSTRAINT ck_incremental_db CHECK (NOT (load_type = 'incremental' AND ingest_method = 'copy_db' AND watermark_column IS NULL))
);

CREATE TABLE meta.column_map (
    object_id            VARCHAR(50)   NOT NULL REFERENCES meta.ingestion_object(object_id),
    column_order         INT           NOT NULL,
    source_column        VARCHAR(128)  NOT NULL,
    target_column        VARCHAR(128)  NOT NULL,
    data_type            VARCHAR(50)   NOT NULL,
    transform_expr       NVARCHAR(2000) NULL,
    pii_treatment        VARCHAR(20)   NOT NULL DEFAULT 'none'
                         CHECK (pii_treatment IN ('none','hash','mask_email','mask_phone','year_only','redact','drop')),
    sensitivity          VARCHAR(20)   NOT NULL DEFAULT 'Internal',
    description          NVARCHAR(500) NULL,
    CONSTRAINT pk_column_map PRIMARY KEY (object_id, target_column)
);

CREATE TABLE meta.dq_rule (
    rule_id              VARCHAR(30)   NOT NULL PRIMARY KEY,
    object_id            VARCHAR(50)   NOT NULL REFERENCES meta.ingestion_object(object_id),
    rule_name            NVARCHAR(200) NOT NULL,
    rule_category        VARCHAR(20)   NOT NULL CHECK (rule_category IN ('technical','business')),
    check_type           VARCHAR(20)   NOT NULL
                         CHECK (check_type IN ('not_null','regex','range','allowed_values','unique','referential','expression')),
    column_name          VARCHAR(300)  NULL,
    params               NVARCHAR(2000) NULL CHECK (params IS NULL OR ISJSON(params) = 1),
    severity             VARCHAR(20)   NOT NULL CHECK (severity IN ('warn','quarantine','fail')),
    fail_threshold_pct   DECIMAL(6,3)  NULL,
    rule_owner           NVARCHAR(100) NOT NULL,     -- business owner for business rules = data steward in Purview
    description          NVARCHAR(1000) NULL,
    active               BIT           NOT NULL DEFAULT 1
);

CREATE TABLE meta.gold_transform (
    transform_id         VARCHAR(30)   NOT NULL PRIMARY KEY,
    industry             VARCHAR(20)   NOT NULL,
    gold_table           VARCHAR(128)  NOT NULL,
    run_order            INT           NOT NULL,
    sql_text             NVARCHAR(MAX) NOT NULL,
    description          NVARCHAR(1000) NULL,
    business_owner       NVARCHAR(100) NULL,
    consumers            NVARCHAR(200) NULL,
    active               BIT           NOT NULL DEFAULT 1
);

-- Operational state ------------------------------------------------------------------------------------------
CREATE TABLE meta.watermark (
    object_id            VARCHAR(50)   NOT NULL,
    layer                VARCHAR(20)   NOT NULL,     -- source | bronze | silver
    watermark_value      NVARCHAR(100) NOT NULL,
    run_id               VARCHAR(100)  NOT NULL,
    updated_at           DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_watermark PRIMARY KEY (object_id, layer)   -- in SQL we can UPDATE in place
);

CREATE TABLE meta.run_log (
    run_id               VARCHAR(100)  NOT NULL,
    parent_run_id        VARCHAR(100)  NULL,         -- pipeline RunId -> joins to Fabric monitoring
    object_id            VARCHAR(50)   NOT NULL,
    layer                VARCHAR(20)   NOT NULL,
    status               VARCHAR(20)   NOT NULL,
    started_at           DATETIME2(3)  NOT NULL,
    ended_at             DATETIME2(3)  NULL,
    duration_sec         DECIMAL(12,3) NULL,
    rows_read            BIGINT        NULL,
    rows_written         BIGINT        NULL,
    rows_quarantined     BIGINT        NULL,
    rows_deduplicated    BIGINT        NULL,
    message              NVARCHAR(4000) NULL
);
CREATE INDEX ix_run_log_object ON meta.run_log (object_id, layer, started_at DESC);

CREATE TABLE meta.dq_result (
    run_id               VARCHAR(100)  NOT NULL,
    object_id            VARCHAR(50)   NOT NULL,
    rule_id              VARCHAR(30)   NOT NULL,
    rule_name            NVARCHAR(200) NULL,
    rule_category        VARCHAR(20)   NULL,
    check_type           VARCHAR(20)   NULL,
    severity             VARCHAR(20)   NULL,
    rows_evaluated       BIGINT        NULL,
    rows_failed          BIGINT        NULL,
    failed_pct           DECIMAL(9,4)  NULL,
    outcome              VARCHAR(20)   NULL,
    evaluated_at         DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Pipeline-facing procedures ----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE meta.usp_get_worklist
    @load_group VARCHAR(30) = 'all', @rerun_failed_only BIT = 0, @run_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @run_date = COALESCE(@run_date, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'AUS Eastern Standard Time' AS DATE));
    WITH last_run AS (
        SELECT object_id, status, ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY started_at DESC) AS rn
        FROM meta.run_log WHERE layer IN ('bronze','silver'))
    SELECT o.object_id, o.ingest_method, o.load_group, o.priority,
           CASE WHEN o.ingest_method = 'copy_db' THEN
                CONCAT('SELECT * FROM ', o.source_table,
                       CASE WHEN o.load_type = 'incremental' AND w.watermark_value IS NOT NULL
                            THEN CONCAT(' WHERE ', o.watermark_column, ' > ''', w.watermark_value, '''') ELSE '' END)
           END AS source_query,
           CONCAT(o.landing_path, '/load_date=', CONVERT(CHAR(10), @run_date, 23)) AS landing_folder,
           CONCAT(o.source_object, '_', CONVERT(CHAR(8), @run_date, 112), '.csv') AS landing_file
    FROM meta.ingestion_object o
    LEFT JOIN meta.watermark w ON w.object_id = o.object_id AND w.layer = 'source'
    LEFT JOIN last_run r ON r.object_id = o.object_id AND r.rn = 1
    WHERE o.active = 1
      AND (@load_group = 'all' OR o.load_group = @load_group)
      AND (@rerun_failed_only = 0 OR r.status = 'FAILED')
    ORDER BY o.priority, o.object_id;
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_set_watermark
    @object_id VARCHAR(50), @layer VARCHAR(20), @value NVARCHAR(100), @run_id VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    MERGE meta.watermark AS t
    USING (SELECT @object_id AS object_id, @layer AS layer) AS s
       ON t.object_id = s.object_id AND t.layer = s.layer
    WHEN MATCHED THEN UPDATE SET watermark_value = @value, run_id = @run_id, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT (object_id, layer, watermark_value, run_id) VALUES (@object_id, @layer, @value, @run_id);
END;
GO
