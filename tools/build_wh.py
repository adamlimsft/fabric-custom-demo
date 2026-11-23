"""Warehouse in Gold: generates the Fabric Data Warehouse (wh_mdf) T-SQL for every pack.

Per pack, in warehouse/<pack>/:
  01_setup.sql          schemas dw, stg, etl, sec; star schema, staging, run log, watermark, late-arriving log, dim_date
  02_procs.sql          stored-procedure ELT: Silver (3-part names) -> stage -> SCD2 dim -> facts, etl.usp_run_all
  03_security_demo.sql  reconciliation, demo queries, clone, time travel, column-level security, masking, RLS
plus warehouse/README.md (rewritten on every build so the pack table stays current).

Usage:  python tools/build_wh.py            build all packs
        python tools/build_wh.py --check    build all packs, then run the offline checks
        python tools/build_wh.py --check energy telco
build(p, kit_dir) is called by tools/build_kit.py for each pack. Standard library only.
"""
import os, re, sqlite3, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kitlib  # noqa: E402

LH = "lh_mdf"
WH = "wh_mdf"
MIN_FROM = "1900-01-01"
DATE_FROM, DATE_TO = "2020-01-01", "2028-12-31"

# T-SQL reserved keywords: pack column names that collide are bracketed
RESERVED = set("""ADD ALL ALTER AND ANY AS ASC AUTHORIZATION BACKUP BEGIN BETWEEN BREAK BROWSE BULK BY CASCADE CASE CHECK
CHECKPOINT CLOSE CLUSTERED COALESCE COLLATE COLUMN COMMIT COMPUTE CONSTRAINT CONTAINS CONTAINSTABLE CONTINUE CONVERT CREATE
CROSS CURRENT CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR DATABASE DBCC DEALLOCATE DECLARE DEFAULT DELETE
DENY DESC DISK DISTINCT DISTRIBUTED DOUBLE DROP DUMP ELSE END ERRLVL ESCAPE EXCEPT EXEC EXECUTE EXISTS EXIT EXTERNAL FETCH FILE
FILLFACTOR FOR FOREIGN FREETEXT FREETEXTTABLE FROM FULL FUNCTION GOTO GRANT GROUP HAVING HOLDLOCK IDENTITY IDENTITY_INSERT
IDENTITYCOL IF IN INDEX INNER INSERT INTERSECT INTO IS JOIN KEY KILL LEFT LIKE LINENO LOAD MERGE NATIONAL NOCHECK NONCLUSTERED
NOT NULL NULLIF OF OFF OFFSETS ON OPEN OPENDATASOURCE OPENQUERY OPENROWSET OPENXML OPTION OR ORDER OUTER OVER PERCENT PIVOT PLAN
PRECISION PRIMARY PRINT PROC PROCEDURE PUBLIC RAISERROR READ READTEXT RECONFIGURE REFERENCES REPLICATION RESTORE RESTRICT RETURN
REVERT REVOKE RIGHT ROLLBACK ROWCOUNT ROWGUIDCOL RULE SAVE SCHEMA SECURITYAUDIT SELECT SEMANTICKEYPHRASETABLE
SEMANTICSIMILARITYDETAILSTABLE SEMANTICSIMILARITYTABLE SESSION_USER SET SETUSER SHUTDOWN SOME STATISTICS SYSTEM_USER TABLE
TABLESAMPLE TEXTSIZE THEN TO TOP TRAN TRANSACTION TRIGGER TRUNCATE TRY_CONVERT TSEQUAL UNION UNIQUE UNPIVOT UPDATE UPDATETEXT USE
USER VALUES VARYING VIEW WAITFOR WHEN WHERE WHILE WITH WITHIN WRITETEXT""".split())

FRAMEWORK_COLS = {
    "merge_scd2": {"_is_current": "boolean", "_valid_from": "timestamp", "_valid_to": "timestamp",
                   "_silver_updated_at": "timestamp", "_record_hash": "string"},
    "merge_scd1": {"_is_deleted": "boolean", "_silver_updated_at": "timestamp", "_record_hash": "string"},
    "insert_only": {"_record_hash": "string"},
}
AUDIT_COLS = {"_ingested_at": "timestamp", "_source_file": "string", "_run_id": "string"}


def q(col):
    return f"[{col}]" if col.upper() in RESERVED else col


def silver_schema(p):
    """{silver table: {column: spark type}} - column_map target columns (minus PII 'drop') + framework columns."""
    cr = kitlib.control_rows(p)
    out = {}
    for o in cr["ingestion_object"]:
        cols = {m["target_column"]: m["data_type"] for m in cr["column_map"]
                if m["object_id"] == o["object_id"] and m["pii_treatment"] != "drop"}
        cols.update(FRAMEWORK_COLS[o["silver_write_mode"]])
        cols.update(AUDIT_COLS)
        out[o["silver_table"]] = cols
    return out


def wh_type(spark_type, col=""):
    t = spark_type.lower()
    if t.startswith("decimal"):
        return t.upper()
    return {"int": "INT", "double": "FLOAT", "date": "DATE", "timestamp": "DATETIME2(6)", "boolean": "BIT",
            "string": "VARCHAR(200)" if col.endswith("name") else "VARCHAR(100)"}[t]


def differs(a, b):
    """Null-safe 'a is different from b' (portable, no IS DISTINCT FROM)."""
    return f"({a} <> {b} OR ({a} IS NULL AND {b} IS NOT NULL) OR ({a} IS NOT NULL AND {b} IS NULL))"


def M(p):
    """Every name the warehouse scripts use, in one place."""
    n, E, V, R = kitlib.N(p), p["entity"], p["event"], p["reading"]
    sch = silver_schema(p)
    reg = n["reg"]
    m = dict(n=n, sch=sch, reg=reg, e=E["table"], v=V["table"], r=R["table"],
             key=f"{E['table']}_key", eid=E["id"], vid=V["id"],
             dim=f"dw.{n['w_dim']}", fact=f"dw.{n['w_fact']}", rfact=f"dw.{n['w_rfact']}",
             s_dim=f"stg.{n['w_dim']}", s_fact=f"stg.{n['w_fact']}", s_rfact=f"stg.{n['w_rfact']}",
             o_dim=n["w_dim"], o_fact=n["w_fact"], o_rfact=n["w_rfact"],
             sv_e=f"{LH}.silver.{E['table']}", sv_v=f"{LH}.silver.{V['table']}", sv_r=f"{LH}.silver.{R['table']}",
             open_ts=V["open_ts"], close_ts=V["close_ts"], vtype=V["type_col"], amt=V["amount_col"], qty=V["qty_col"],
             m1=n["m1"], m2=n["m2"], m3=n["m3"], thr=n["thr"],
             open_status=n["open_status"], cancelled=n["cancelled_status"])
    m["t1"] = [E["code"], E["name_col"], E["type_col"], reg, E["start_col"]]
    m["t2"] = ["status", E["tier_col"], E["cap_col"]]
    m["ecols"] = [E["id"], E["code"], E["name_col"], E["type_col"], reg, "status", E["tier_col"], E["cap_col"], E["start_col"]]
    es, vs, rs = sch[E["table"]], sch[V["table"]], sch[R["table"]]
    m["dim_def"] = ([(m["key"], "INT NOT NULL"), (E["id"], "INT NOT NULL")] +
                    [(c, wh_type(es[c], c)) for c in m["ecols"][1:]] +
                    [("valid_from", "DATETIME2(6) NOT NULL"), ("valid_to", "DATETIME2(6)"), ("is_current", "BIT NOT NULL")])
    m["vcols"] = [V["id"], E["id"], V["type_col"], "priority", "status", V["open_ts"], V["close_ts"], V["amount_col"], V["qty_col"]]
    m["fact_def"] = [(V["id"], "INT NOT NULL"), (m["key"], "INT NOT NULL"), ("opened_date_key", "INT NOT NULL"),
                     (V["type_col"], wh_type(vs[V["type_col"]])), ("priority", wh_type(vs["priority"])),
                     ("status", wh_type(vs["status"])), (V["open_ts"], wh_type(vs[V["open_ts"]])),
                     (V["close_ts"], wh_type(vs[V["close_ts"]])), ("hours_to_close", "DECIMAL(10,2)"), ("is_open", "BIT"),
                     (V["amount_col"], wh_type(vs[V["amount_col"]])), (V["qty_col"], wh_type(vs[V["qty_col"]]))]
    m["rcols"] = [E["id"], "reading_ts", n["m1"], n["m2"], n["m3"]]
    m["rfact_def"] = [(m["key"], "INT NOT NULL"), ("reading_date_key", "INT NOT NULL"), ("readings", "INT"),
                      (f"avg_{n['m1']}", "DECIMAL(18,2)"), (f"max_{n['m1']}", "DECIMAL(18,2)"), ("breach_readings", "INT"),
                      (f"avg_{n['m2']}", "DECIMAL(18,2)"), (f"avg_{n['m3']}", "DECIMAL(18,2)")]
    m["stg_dim_def"] = ([(c, wh_type(es[c], c)) for c in m["ecols"]] +
                        [("src_valid_from", "DATETIME2(6)"), ("src_is_current", "BIT"), ("src_updated_at", "DATETIME2(6)")])
    m["stg_fact_def"] = [(c, wh_type(vs[c], c)) for c in m["vcols"]] + [("_is_deleted", "BIT"), ("_silver_updated_at", "DATETIME2(6)")]
    m["stg_rfact_def"] = [(c, wh_type(rs[c], c)) for c in m["rcols"]] + [("_ingested_at", "DATETIME2(6)")]
    return m


# ---------------------------------------------------------------------------------------------- contract
def contract(p):
    """The column lists other docs depend on (also asserted by --check)."""
    E, V, n = p["entity"], p["event"], kitlib.N(p)
    return {
        "dw.dim_date": ["date_key", "calendar_date", "calendar_year", "calendar_quarter", "month_number", "month_name",
                        "year_month", "day_name", "fy_year", "fy_label", "is_weekend"],
        f"dw.{n['w_dim']}": [f"{E['table']}_key", E["id"], E["code"], E["name_col"], E["type_col"], n["reg"], "status",
                             E["tier_col"], E["cap_col"], E["start_col"], "valid_from", "valid_to", "is_current"],
        f"dw.{n['w_fact']}": [V["id"], f"{E['table']}_key", "opened_date_key", V["type_col"], "priority", "status",
                              V["open_ts"], V["close_ts"], "hours_to_close", "is_open", V["amount_col"], V["qty_col"]],
        f"dw.{n['w_rfact']}": [f"{E['table']}_key", "reading_date_key", "readings", f"avg_{n['m1']}", f"max_{n['m1']}",
                               "breach_readings", f"avg_{n['m2']}", f"avg_{n['m3']}"],
    }


def create_table(name, cols):
    body = ",\n".join(f"    {q(c)} {t}" for c, t in cols)
    return f"CREATE TABLE {name} (\n{body}\n);"


def ddate_key(expr):
    return f"CONVERT(INT, CONVERT(CHAR(8), {expr}, 112))"


# ---------------------------------------------------------------------------------------------- 01_setup.sql
def setup_sql(p):
    m = M(p)
    E = p["entity"]
    L = [f"/* {p['org']} ({p['industry']}) - Warehouse in Gold, script 1 of 3: setup",
         f"   Run in warehouse {WH} (same workspace as lakehouse {LH}) after the notebooks have built Silver.",
         "   Re-runnable: it drops and recreates every warehouse object, so the dw tables start empty again.",
         "   Generated by tools/build_wh.py from config/" + p["id"] + ".json - edit the pack, not this file. */",
         "",
         "-- Security objects are schema-bound to the tables, so they go first (03_security_demo.sql recreates them)",
         "IF OBJECT_ID('sec.rls_region') IS NOT NULL DROP SECURITY POLICY sec.rls_region;",
         "DROP FUNCTION IF EXISTS sec.fn_region_filter;",
         "DROP FUNCTION IF EXISTS sec.fn_region_filter_by_key;",
         "GO",
         "IF SCHEMA_ID('dw') IS NULL EXEC('CREATE SCHEMA dw');",
         "IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');",
         "IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl');",
         "IF SCHEMA_ID('sec') IS NULL EXEC('CREATE SCHEMA sec');",
         "GO",
         "",
         "-- Run control: append-only run log, watermark trail (latest value wins) and the late-arriving key log",
         "DROP TABLE IF EXISTS etl.load_log;",
         "DROP TABLE IF EXISTS etl.watermark;",
         "DROP TABLE IF EXISTS etl.late_arriving;",
         create_table("etl.load_log", [("run_id", "VARCHAR(64)"), ("object_name", "VARCHAR(128)"), ("status", "VARCHAR(20)"),
                                       ("rows_staged", "INT"), ("rows_inserted", "INT"), ("rows_updated", "INT"),
                                       ("started_at", "DATETIME2(6)"), ("ended_at", "DATETIME2(6)"), ("message", "VARCHAR(4000)")]),
         create_table("etl.watermark", [("object_name", "VARCHAR(128)"), ("wm_value", "DATETIME2(6)"), ("run_id", "VARCHAR(64)"),
                                        ("updated_at", "DATETIME2(6)")]),
         create_table("etl.late_arriving", [("object_name", "VARCHAR(128)"), ("business_key", "BIGINT"), ("entity_id", "INT"),
                                            ("event_ts", "DATETIME2(6)"), ("first_seen_run_id", "VARCHAR(64)"),
                                            ("first_seen_at", "DATETIME2(6)"), ("resolved_run_id", "VARCHAR(64)"),
                                            ("resolved_at", "DATETIME2(6)")]),
         "",
         f"-- Row-level security mapping (empty until 03_security_demo.sql). '*' = all {p['region_label']}s.",
         "DROP TABLE IF EXISTS sec.user_region;",
         create_table("sec.user_region", [("user_principal", "VARCHAR(256)"), ("region_value", "VARCHAR(100)")]),
         "GO",
         "",
         f"-- Date dimension {DATE_FROM} to {DATE_TO}, generated (no Silver, no recursion). FY = Australian financial year.",
         "DROP TABLE IF EXISTS dw.dim_date;",
         create_table("dw.dim_date", [("date_key", "INT NOT NULL"), ("calendar_date", "DATE"), ("calendar_year", "INT"),
                                      ("calendar_quarter", "INT"), ("month_number", "INT"), ("month_name", "VARCHAR(10)"),
                                      ("year_month", "CHAR(7)"), ("day_name", "VARCHAR(10)"), ("fy_year", "INT"),
                                      ("fy_label", "VARCHAR(10)"), ("is_weekend", "BIT")]),
         "GO",
         "INSERT INTO dw.dim_date (date_key, calendar_date, calendar_year, calendar_quarter, month_number, month_name, year_month,",
         "                         day_name, fy_year, fy_label, is_weekend)",
         f"SELECT {ddate_key('d.calendar_date')}, d.calendar_date, YEAR(d.calendar_date), DATEPART(QUARTER, d.calendar_date),",
         "       MONTH(d.calendar_date), LEFT(DATENAME(MONTH, d.calendar_date), 3), CONVERT(CHAR(7), d.calendar_date, 120),",
         "       LEFT(DATENAME(WEEKDAY, d.calendar_date), 3),",
         "       CASE WHEN MONTH(d.calendar_date) >= 7 THEN YEAR(d.calendar_date) + 1 ELSE YEAR(d.calendar_date) END,",
         "       CONCAT('FY', RIGHT(CAST(CASE WHEN MONTH(d.calendar_date) >= 7 THEN YEAR(d.calendar_date) + 1 ELSE YEAR(d.calendar_date) END AS VARCHAR(4)), 2)),",
         "       CASE WHEN DATEDIFF(DAY, CAST('1900-01-01' AS DATE), d.calendar_date) % 7 >= 5 THEN 1 ELSE 0 END   -- 1900-01-01 was a Monday",
         f"FROM  (SELECT DATEADD(DAY, n.i, CAST('{DATE_FROM}' AS DATE)) AS calendar_date",
         "       FROM (SELECT a.v + 10 * b.v + 100 * c.v + 1000 * e.v AS i",
         "             FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(v)",
         "             CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(v)",
         "             CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(v)",
         "             CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS e(v)) AS n",
         f"       WHERE n.i <= DATEDIFF(DAY, CAST('{DATE_FROM}' AS DATE), CAST('{DATE_TO}' AS DATE))) AS d;",
         "INSERT INTO dw.dim_date (date_key, calendar_date, calendar_year, calendar_quarter, month_number, month_name, year_month,",
         "                         day_name, fy_year, fy_label, is_weekend)",
         "VALUES (-1, NULL, NULL, NULL, NULL, 'Unknown', 'Unknown', 'Unknown', NULL, 'Unknown', 0);",
         "ALTER TABLE dw.dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED;",
         "GO",
         "",
         f"-- {m['dim']}: SCD2 on status, {E['tier_col']} and {E['cap_col']}; the rest is type 1. Key -1 = unknown member.",
         f"DROP TABLE IF EXISTS {m['s_dim']};",
         f"DROP TABLE IF EXISTS {m['dim']};",
         create_table(m["s_dim"], m["stg_dim_def"]),
         create_table(m["dim"], m["dim_def"]),
         "GO"]
    dcols = [c for c, _ in m["dim_def"]]
    unknown = {m["key"]: "-1", m["eid"]: "-1", E["code"]: "'UNKNOWN'", E["name_col"]: "'Unknown'", E["type_col"]: "'UNKNOWN'",
               m["reg"]: "'Unknown'", "status": "'UNKNOWN'", E["tier_col"]: "NULL", E["cap_col"]: "NULL",
               E["start_col"]: "NULL", "valid_from": f"CAST('{MIN_FROM}' AS DATETIME2(6))", "valid_to": "NULL",
               "is_current": "1"}
    L += [f"INSERT INTO {m['dim']} ({', '.join(q(c) for c in dcols)})",
          f"VALUES ({', '.join(unknown[c] for c in dcols)});   -- unknown member: facts never lose rows when a key is missing",
          f"ALTER TABLE {m['dim']} ADD CONSTRAINT pk_{m['o_dim']} PRIMARY KEY NONCLUSTERED ({m['key']}) NOT ENFORCED;",
          "GO",
          "",
          f"-- {m['fact']}: one row per {p['event']['label']}, SCD1 from Silver, key = {E['label']} version valid when it was {p['event']['open_verb']}",
          f"DROP TABLE IF EXISTS {m['s_fact']};",
          f"DROP TABLE IF EXISTS {m['fact']};",
          create_table(m["s_fact"], m["stg_fact_def"]),
          create_table(m["fact"], m["fact_def"]),
          f"ALTER TABLE {m['fact']} ADD CONSTRAINT pk_{m['o_fact']} PRIMARY KEY NONCLUSTERED ({m['vid']}) NOT ENFORCED;",
          f"ALTER TABLE {m['fact']} ADD CONSTRAINT fk_{m['o_fact']}_{m['e']} FOREIGN KEY ({m['key']}) REFERENCES {m['dim']} ({m['key']}) NOT ENFORCED;",
          f"ALTER TABLE {m['fact']} ADD CONSTRAINT fk_{m['o_fact']}_date FOREIGN KEY (opened_date_key) REFERENCES dw.dim_date (date_key) NOT ENFORCED;",
          "GO",
          "",
          f"-- {m['rfact']}: daily roll-up of the 15-minute {p['reading']['label']}; affected days are recomputed in full",
          f"DROP TABLE IF EXISTS {m['s_rfact']};",
          f"DROP TABLE IF EXISTS {m['rfact']};",
          create_table(m["s_rfact"], m["stg_rfact_def"]),
          create_table(m["rfact"], m["rfact_def"]),
          f"ALTER TABLE {m['rfact']} ADD CONSTRAINT fk_{m['o_rfact']}_{m['e']} FOREIGN KEY ({m['key']}) REFERENCES {m['dim']} ({m['key']}) NOT ENFORCED;",
          f"ALTER TABLE {m['rfact']} ADD CONSTRAINT fk_{m['o_rfact']}_date FOREIGN KEY (reading_date_key) REFERENCES dw.dim_date (date_key) NOT ENFORCED;",
          "GO",
          "",
          "SELECT 'setup complete' AS status, (SELECT COUNT(*) FROM dw.dim_date) AS dim_date_rows;",
          "GO"]
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------------------------- 02_procs.sql
def dim_candidates_select(p):
    """New SCD2 versions to insert: Silver versions newer than what dw holds where a tracked attribute changed.
    Uses @max_key and @min_from. Kept portable so --check can replay it in SQLite."""
    m = M(p)
    k, i = m["key"], m["eid"]
    order = "n.src_valid_from, n.src_updated_at"
    inner = ", ".join(f"n.{q(c)}" for c in m["ecols"]) + ", n.src_valid_from, n.src_updated_at"
    lags = ",\n                ".join(f"LAG(n.{q(c)}) OVER (PARTITION BY n.{i} ORDER BY {order}) AS prev_{c}" for c in m["t2"])
    vs_cur = " OR ".join(differs(f"v.{q(c)}", f"c.{q(c)}") for c in m["t2"])
    vs_prev = " OR ".join(differs(f"v.{q(c)}", f"v.prev_{c}") for c in m["t2"])
    out = ", ".join(f"v.{q(c)}" for c in m["ecols"])
    return (f"SELECT @max_key + ROW_NUMBER() OVER (ORDER BY v.{i}, v.src_valid_from, v.src_updated_at) AS new_key,\n"
            f"       {out},\n"
            f"       CASE WHEN c.{i} IS NULL AND v.rn = 1 THEN @min_from ELSE v.src_valid_from END AS valid_from,\n"
            f"       NULL AS valid_to, 1 AS is_current\n"
            f"FROM  (SELECT {inner},\n"
            f"                {lags},\n"
            f"                ROW_NUMBER() OVER (PARTITION BY n.{i} ORDER BY {order}) AS rn\n"
            f"        FROM   {m['s_dim']} AS n\n"
            f"        LEFT JOIN (SELECT h.{i}, MAX(h.valid_from) AS max_valid_from FROM {m['dim']} AS h\n"
            f"                   WHERE h.{k} <> -1 GROUP BY h.{i}) AS l ON l.{i} = n.{i}\n"
            f"        WHERE  l.{i} IS NULL OR n.src_valid_from > l.max_valid_from) AS v\n"
            f"LEFT JOIN {m['dim']} AS c ON c.{i} = v.{i} AND c.is_current = 1 AND c.{k} <> -1\n"
            f"WHERE (v.rn = 1 AND c.{i} IS NULL)\n"
            f"   OR (v.rn = 1 AND ({vs_cur}))\n"
            f"   OR (v.rn > 1 AND ({vs_prev}))")


def dim_close_select(p):
    """valid_to = next version's valid_from, for every version of the staged keys."""
    m = M(p)
    k, i = m["key"], m["eid"]
    return (f"SELECT h.{k}, LEAD(h.valid_from) OVER (PARTITION BY h.{i} ORDER BY h.valid_from) AS next_valid_from\n"
            f"FROM   {m['dim']} AS h\n"
            f"WHERE  h.{k} <> -1 AND h.{i} IN (SELECT t.{i} FROM {m['s_dim']} AS t)")


def key_joins(p, src_id, ts):
    """As-at SCD2 lookup: the version valid at `ts`, else the current version, else -1."""
    m = M(p)
    k, i = m["key"], m["eid"]
    joins = (f"LEFT JOIN {m['dim']} AS ka ON ka.{i} = {src_id} AND ka.{k} <> -1 AND {ts} >= ka.valid_from "
             f"AND (ka.valid_to IS NULL OR {ts} < ka.valid_to)\n"
             f"        LEFT JOIN {m['dim']} AS kc ON kc.{i} = {src_id} AND kc.is_current = 1 AND kc.{k} <> -1")
    return joins, f"COALESCE(ka.{k}, kc.{k}, -1)"


def proc(name, obj, body):
    return f"""CREATE OR ALTER PROCEDURE etl.{name} @run_id VARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME(), @now DATETIME2(6) = SYSUTCDATETIME(), @rows_staged INT = 0,
            @rows_inserted INT = 0, @rows_updated INT = 0, @msg VARCHAR(4000) = '';
    DECLARE @wm DATETIME2(6), @new_wm DATETIME2(6);
    BEGIN TRY
        SELECT @wm = COALESCE(MAX(wm_value), CAST('{MIN_FROM}' AS DATETIME2(6))) FROM etl.watermark WHERE object_name = '{obj}';
{body}
        IF @new_wm IS NOT NULL AND @new_wm > @wm
            INSERT INTO etl.watermark (object_name, wm_value, run_id, updated_at) VALUES ('{obj}', @new_wm, @run_id, @now);
        COMMIT TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, '{obj}', 'SUCCESS', @rows_staged, @rows_inserted, @rows_updated, @started, SYSUTCDATETIME(), @msg);
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO etl.load_log (run_id, object_name, status, rows_staged, rows_inserted, rows_updated, started_at, ended_at, message)
        VALUES (@run_id, '{obj}', 'FAILED', @rows_staged, 0, 0, @started, SYSUTCDATETIME(), LEFT(ERROR_MESSAGE(), 4000));
        THROW;
    END CATCH
END;
GO"""


def indent(text, n=8):
    return "\n".join((" " * n + ln) if ln else ln for ln in text.split("\n"))


def procs_sql(p):
    m = M(p)
    E, V = p["entity"], p["event"]
    k, i, vid = m["key"], m["eid"], m["vid"]
    sv = lambda cols, a="sv": ", ".join(f"{a}.{q(c)}" for c in cols)
    plain = lambda cols: ", ".join(q(c) for c in cols)

    # ---- dimension
    stg_dim_cols = [c for c, _ in m["stg_dim_def"]]
    dim_cols = [c for c, _ in m["dim_def"]]
    t1_set = ",\n               ".join(f"d.{q(c)} = s.{q(c)}" for c in m["t1"])
    t1_diff = "\n            OR ".join(differs(f"d.{q(c)}", f"s.{q(c)}") for c in m["t1"])
    dim_body = f"""        -- 1. Stage every Silver version of each {E['label']} touched since the last run. Silver keeps full SCD2 history,
        --    so the warehouse can replay it: the first load already has history, not just today's picture.
        TRUNCATE TABLE {m['s_dim']};
        INSERT INTO {m['s_dim']} ({plain(stg_dim_cols)})
        SELECT {sv(m['ecols'])}, sv._valid_from, sv._is_current, sv._silver_updated_at
        FROM   {m['sv_e']} AS sv
        WHERE  sv.{i} IN (SELECT svn.{i} FROM {m['sv_e']} AS svn WHERE svn._silver_updated_at > @wm);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(src_updated_at) FROM {m['s_dim']};
        DECLARE @max_key INT = (SELECT COALESCE(MAX({k}), 0) FROM {m['dim']});
        DECLARE @min_from DATETIME2(6) = CAST('{MIN_FROM}' AS DATETIME2(6));

        BEGIN TRANSACTION;
        -- 2. New members and new versions (only when status, {E['tier_col']} or {E['cap_col']} changed).
        --    A new member's first version is valid from {MIN_FROM} so older {V['plural']} still find it.
        --    Surrogate key = current max + ROW_NUMBER: INT, and -1 stays free for the unknown member.
        INSERT INTO {m['dim']} ({plain(dim_cols)})
{indent(dim_candidates_select(p))};
        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Close versions: valid_to = next version's valid_from, is_current only on the latest
        UPDATE d
        SET    d.valid_to = x.next_valid_from,
               d.is_current = CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END
        FROM   {m['dim']} AS d
        INNER JOIN (
{indent(dim_close_select(p), 12)}
        ) AS x ON x.{k} = d.{k}
        WHERE  {differs('d.valid_to', 'x.next_valid_from')}
            OR d.is_current <> CASE WHEN x.next_valid_from IS NULL THEN 1 ELSE 0 END;
        SET @rows_updated = @@ROWCOUNT;

        -- 4. Type 1 attributes: every version takes the latest value from the current Silver row
        UPDATE d
        SET    {t1_set}
        FROM   {m['dim']} AS d
        INNER JOIN {m['s_dim']} AS s ON s.{i} = d.{i} AND s.src_is_current = 1
        WHERE  d.{k} <> -1
          AND ({t1_diff});
        SET @rows_updated = @rows_updated + @@ROWCOUNT;
        SET @msg = CONCAT('{E['plural']} staged with full history: ', (SELECT COUNT(DISTINCT {i}) FROM {m['s_dim']}));"""

    # ---- event fact
    joins, keyx = key_joins(p, f"s.{i}", f"s.{m['open_ts']}")
    fact_cols = [c for c, _ in m["fact_def"]]
    open_in = ", ".join(f"'{s}'" for s in m["open_status"])
    src_select = f"""SELECT s.{vid},
               {keyx} AS {k},
               COALESCE({ddate_key(f's.{m["open_ts"]}')}, -1) AS opened_date_key,
               s.{q(m['vtype'])}, s.priority, s.status, s.{m['open_ts']}, s.{m['close_ts']},
               CAST(DATEDIFF(MINUTE, s.{m['open_ts']}, s.{m['close_ts']}) / 60.0 AS DECIMAL(10,2)) AS hours_to_close,
               CAST(CASE WHEN s.status IN ({open_in}) THEN 1 ELSE 0 END AS BIT) AS is_open,
               s.{q(m['amt'])}, s.{q(m['qty'])}, s._is_deleted
        FROM   {m['s_fact']} AS s
        {joins}"""
    upd = ", ".join(f"t.{q(c)} = src.{q(c)}" for c in fact_cols if c != vid)
    late_join, late_key = key_joins(p, "l.entity_id", "l.event_ts")
    fact_body = f"""        -- 1. Stage {V['plural']} changed in Silver since the last watermark (_silver_updated_at)
        TRUNCATE TABLE {m['s_fact']};
        INSERT INTO {m['s_fact']} ({plain([c for c, _ in m['stg_fact_def']])})
        SELECT {sv(m['vcols'])}, sv._is_deleted, sv._silver_updated_at
        FROM   {m['sv_v']} AS sv
        WHERE  sv._silver_updated_at > @wm;
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_silver_updated_at) FROM {m['s_fact']};

        BEGIN TRANSACTION;
        -- 2. MERGE: surrogate key = {E['label']} version valid when the {V['label']} was {V['open_verb']}, else current, else -1.
        --    Soft-deleted rows in Silver are removed from the fact.
        MERGE {m['fact']} AS t
        USING (
        {src_select}
        ) AS src
        ON t.{vid} = src.{vid}
        WHEN MATCHED AND src._is_deleted = 1 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET {upd}
        WHEN NOT MATCHED BY TARGET AND src._is_deleted = 0 THEN
            INSERT ({plain(fact_cols)})
            VALUES ({', '.join(f'src.{q(c)}' for c in fact_cols)});
        SET @rows_inserted = @@ROWCOUNT;   -- rows inserted, updated or deleted by the MERGE

        -- 3. Late-arriving {E['label']}: remember every row that landed on -1 ...
        INSERT INTO etl.late_arriving (object_name, business_key, entity_id, event_ts, first_seen_run_id, first_seen_at)
        SELECT '{m['o_fact']}', f.{vid}, s.{i}, s.{m['open_ts']}, @run_id, @now
        FROM   {m['fact']} AS f
        INNER JOIN {m['s_fact']} AS s ON s.{vid} = f.{vid}
        WHERE  f.{k} = -1
          AND  NOT EXISTS (SELECT 1 FROM etl.late_arriving AS la
                           WHERE la.object_name = '{m['o_fact']}' AND la.business_key = f.{vid} AND la.resolved_at IS NULL);

        -- ... and repair them on any later run once the {E['label']} exists in the dimension
        UPDATE f
        SET    f.{k} = {late_key}
        FROM   {m['fact']} AS f
        INNER JOIN etl.late_arriving AS l ON l.object_name = '{m['o_fact']}' AND l.business_key = f.{vid} AND l.resolved_at IS NULL
        {late_join}
        WHERE  f.{k} = -1
          AND  {late_key} <> -1;
        SET @rows_updated = @@ROWCOUNT;
        UPDATE l
        SET    l.resolved_at = @now, l.resolved_run_id = @run_id
        FROM   etl.late_arriving AS l
        INNER JOIN {m['fact']} AS f ON f.{vid} = l.business_key
        WHERE  l.object_name = '{m['o_fact']}' AND l.resolved_at IS NULL AND f.{k} <> -1;
        SET @msg = CONCAT('late-arriving keys repaired: ', @rows_updated, '; still unknown: ',
                          (SELECT COUNT(*) FROM {m['fact']} AS u WHERE u.{k} = -1));"""

    # ---- reading daily fact
    rjoins, rkey = key_joins(p, f"s.{i}", "s.reading_ts")
    rcols = [c for c, _ in m["rfact_def"]]
    m1, m2, m3 = q(m["m1"]), q(m["m2"]), q(m["m3"])
    day = ddate_key("s.reading_ts")
    rfact_body = f"""        -- 1. Stage every reading for the days touched since the last watermark (_ingested_at), plus any day that still
        --    has rows on the unknown member (-1), so readings for a late {E['label']} are re-keyed on a later run.
        TRUNCATE TABLE {m['s_rfact']};
        INSERT INTO {m['s_rfact']} ({plain([c for c, _ in m['stg_rfact_def']])})
        SELECT {sv(m['rcols'])}, sv._ingested_at
        FROM   {m['sv_r']} AS sv
        WHERE  CAST(sv.reading_ts AS DATE) IN (SELECT DISTINCT CAST(svn.reading_ts AS DATE) FROM {m['sv_r']} AS svn
                                               WHERE svn._ingested_at > @wm)
           OR  CAST(sv.reading_ts AS DATE) IN (SELECT dd.calendar_date FROM {m['rfact']} AS u
                                               INNER JOIN dw.dim_date AS dd ON dd.date_key = u.reading_date_key
                                               WHERE u.{k} = -1);
        SET @rows_staged = @@ROWCOUNT;
        SELECT @new_wm = MAX(_ingested_at) FROM {m['s_rfact']};

        BEGIN TRANSACTION;
        -- 2. Recompute the affected days in full (idempotent: a re-run gives the same rows)
        DELETE FROM {m['rfact']}
        WHERE  reading_date_key IN (SELECT DISTINCT {ddate_key('x.reading_ts')} FROM {m['s_rfact']} AS x);
        SET @rows_updated = @@ROWCOUNT;
        -- Each reading takes the {E['label']} version valid at reading time, so a mid-day change gives two rows that day.
        INSERT INTO {m['rfact']} ({plain(rcols)})
        SELECT {rkey}, {day}, COUNT(*),
               CAST(AVG(s.{m1}) AS DECIMAL(18,2)), CAST(MAX(s.{m1}) AS DECIMAL(18,2)),
               SUM(CASE WHEN s.{m1} > {m['thr']} THEN 1 ELSE 0 END),
               CAST(AVG(s.{m2}) AS DECIMAL(18,2)), CAST(AVG(s.{m3}) AS DECIMAL(18,2))
        FROM   {m['s_rfact']} AS s
        {rjoins}
        GROUP BY {rkey}, {day};
        SET @rows_inserted = @@ROWCOUNT;
        SET @msg = CONCAT('days recomputed: ', (SELECT COUNT(DISTINCT CAST(x.reading_ts AS DATE)) FROM {m['s_rfact']} AS x),
                          '; rows on unknown member: ', (SELECT COUNT(*) FROM {m['rfact']} AS u WHERE u.{k} = -1));"""

    run_all = f"""-- Orchestrator: dimension first, then facts. A pipeline Stored procedure activity calls this after the Silver step.
CREATE OR ALTER PROCEDURE etl.usp_run_all
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @run_id VARCHAR(64) = CONCAT('wh_', REPLACE(CONVERT(VARCHAR(19), SYSUTCDATETIME(), 126), ':', ''), '_',
                                         LEFT(CONVERT(VARCHAR(36), NEWID()), 8));
    DECLARE @started DATETIME2(6) = SYSUTCDATETIME();
    -- RLS guard: once sec.user_region has rows, the loading identity must be mapped to '*' or the procedures
    -- would only see some {p['region_label']}s and write wrong history
    DECLARE @mapped INT = (SELECT COUNT(*) FROM sec.user_region);
    DECLARE @full INT = (SELECT COUNT(*) FROM sec.user_region WHERE user_principal = USER_NAME() AND region_value = '*');
    IF @mapped > 0 AND @full = 0
        THROW 50001, 'Row-level security is set up and this identity is not mapped to * in sec.user_region. Add it before loading.', 1;
    BEGIN TRY
        EXEC etl.usp_load_{m['o_dim']} @run_id = @run_id;
        EXEC etl.usp_load_{m['o_fact']} @run_id = @run_id;
        EXEC etl.usp_load_{m['o_rfact']} @run_id = @run_id;
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
GO"""
    head = (f"/* {p['org']} ({p['industry']}) - Warehouse in Gold, script 2 of 3: stored-procedure ELT\n"
            f"   Reads Silver in {LH} through 3-part names ({m['sv_e']} ...). Each procedure: watermark -> stage ->\n"
            "   one transaction -> watermark trail + run log; TRY/CATCH rolls back, logs the error and re-throws it.\n"
            f"   Run it once to create the procedures, then: EXEC etl.usp_run_all; */\n")
    parts = [head,
             f"-- {m['dim']}: SCD2 replayed from Silver history",
             proc(f"usp_load_{m['o_dim']}", m["o_dim"], dim_body), "",
             f"-- {m['fact']}: incremental MERGE, as-at key lookup, late-arriving repair",
             proc(f"usp_load_{m['o_fact']}", m["o_fact"], fact_body), "",
             f"-- {m['rfact']}: daily aggregate, affected days recomputed",
             proc(f"usp_load_{m['o_rfact']}", m["o_rfact"], rfact_body), "",
             run_all]
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------------------------- 03_security_demo.sql
def security_sql(p):
    m = M(p)
    E, V, R = p["entity"], p["event"], p["reading"]
    k, i, vid, reg = m["key"], m["eid"], m["vid"], m["reg"]
    rf = f"{m['o_fact']}_before_release"
    amt, name = q(m["amt"]), q(E["name_col"])
    region1 = p["regions"][0]
    open_in = ", ".join(f"'{s}'" for s in m["open_status"])
    m1 = m["m1"]
    recon = f"""SELECT r.check_name, r.silver_value, r.warehouse_value,
       CASE WHEN r.silver_value = r.warehouse_value THEN 'OK' ELSE 'CHECK' END AS result
FROM (
    SELECT 1 AS seq, 'current {E['plural']}' AS check_name,
           CAST((SELECT COUNT(*) FROM {m['sv_e']} AS s1 WHERE s1._is_current = 1) AS DECIMAL(18,2)) AS silver_value,
           CAST((SELECT COUNT(*) FROM {m['dim']} AS w1 WHERE w1.is_current = 1 AND w1.{k} <> -1) AS DECIMAL(18,2)) AS warehouse_value
    UNION ALL
    SELECT 2, '{V['plural']} (not deleted)',
           CAST((SELECT COUNT(*) FROM {m['sv_v']} AS s2 WHERE s2._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT COUNT(*) FROM {m['fact']} AS w2) AS DECIMAL(18,2))
    UNION ALL
    SELECT 3, 'sum of {m['amt']}',
           CAST((SELECT SUM(s3.{amt}) FROM {m['sv_v']} AS s3 WHERE s3._is_deleted = 0) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w3.{amt}) FROM {m['fact']} AS w3) AS DECIMAL(18,2))
    UNION ALL
    SELECT 4, '{R['label']} readings',
           CAST((SELECT COUNT(*) FROM {m['sv_r']} AS s4) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w4.readings) FROM {m['rfact']} AS w4) AS DECIMAL(18,2))
    UNION ALL
    SELECT 5, 'readings with {m1} over {m['thr']}',
           CAST((SELECT COUNT(*) FROM {m['sv_r']} AS s5 WHERE s5.{q(m1)} > {m['thr']}) AS DECIMAL(18,2)),
           CAST((SELECT SUM(w5.breach_readings) FROM {m['rfact']} AS w5) AS DECIMAL(18,2))
) AS r
ORDER BY r.seq;"""
    L = [f"/* {p['org']} ({p['industry']}) - Warehouse in Gold, script 3 of 3: reconcile, demo, platform features, security",
         "   Run AFTER EXEC etl.usp_run_all. Run it one section at a time while you talk; every section is re-runnable.",
         "   Security here applies to T-SQL queries on the warehouse. Workspace Admin, Member and Contributor roles always see",
         "   unmasked data, so test masking and column security as a user who only has Read on the warehouse. */",
         "",
         "-- 0. Reset the demo objects this script creates, so it can be run again",
         "IF OBJECT_ID('sec.rls_region') IS NOT NULL DROP SECURITY POLICY sec.rls_region;",
         "DROP FUNCTION IF EXISTS sec.fn_region_filter;",
         "DROP FUNCTION IF EXISTS sec.fn_region_filter_by_key;",
         f"DROP TABLE IF EXISTS dw.{rf};",
         "GO",
         "",
         "-- 1. Reconciliation: Silver (lakehouse) against the warehouse - parallel-run evidence before a cut-over",
         recon,
         "GO",
         "",
         "-- 2. Run log, watermark trail and late-arriving keys",
         "SELECT TOP 30 l.run_id, l.object_name, l.status, l.rows_staged, l.rows_inserted, l.rows_updated, l.message, l.ended_at",
         "FROM   etl.load_log AS l ORDER BY l.ended_at DESC;",
         "SELECT w.object_name, MAX(w.wm_value) AS current_watermark, COUNT(*) AS loads FROM etl.watermark AS w GROUP BY w.object_name;",
         "SELECT la.object_name, COUNT(*) AS late_rows, SUM(CASE WHEN la.resolved_at IS NULL THEN 1 ELSE 0 END) AS still_unknown",
         "FROM   etl.late_arriving AS la GROUP BY la.object_name;",
         "GO",
         "",
         f"-- 3. Demo: {V['plural']} and {m['amt']} by {p['region_label']} and financial year, using the {E['label']} version valid at the time",
         f"SELECT d.{reg}, dd.fy_label, f.{q(m['vtype'])}, COUNT(*) AS {V['table']}s, SUM(f.{amt}) AS total_{m['amt']},",
         "       CAST(AVG(f.hours_to_close) AS DECIMAL(10,1)) AS avg_hours_to_close",
         f"FROM   {m['fact']} AS f",
         f"INNER JOIN {m['dim']} AS d ON d.{k} = f.{k}",
         "INNER JOIN dw.dim_date AS dd ON dd.date_key = f.opened_date_key",
         f"GROUP BY d.{reg}, dd.fy_label, f.{q(m['vtype'])}",
         f"ORDER BY total_{m['amt']} DESC;",
         f"-- Open {V['plural']} on {E['plural']} with {E['tier_col']} 4 or 5",
         f"SELECT TOP 20 d.{q(E['code'])}, d.{name}, d.{reg}, d.{q(E['tier_col'])}, f.{vid}, f.{q(m['vtype'])}, f.status, f.{m['open_ts']}",
         f"FROM   {m['fact']} AS f INNER JOIN {m['dim']} AS d ON d.{k} = f.{k}",
         f"WHERE  f.is_open = 1 AND d.{q(E['tier_col'])} >= 4",
         f"ORDER BY d.{q(E['tier_col'])} DESC, f.{m['open_ts']};",
         f"-- Days with the most {m1} breaches (over {m['thr']})",
         f"SELECT TOP 20 d.{q(E['code'])}, d.{name}, dd.calendar_date, f.readings, f.max_{m1}, f.breach_readings",
         f"FROM   {m['rfact']} AS f",
         f"INNER JOIN {m['dim']} AS d ON d.{k} = f.{k}",
         "INNER JOIN dw.dim_date AS dd ON dd.date_key = f.reading_date_key",
         "ORDER BY f.breach_readings DESC, f.max_" + m1 + " DESC;",
         f"-- SCD2 history: {E['plural']} with more than one version",
         f"SELECT d.{i}, d.{k}, d.{q(E['code'])}, d.status, d.{q(E['tier_col'])}, d.{q(E['cap_col'])}, d.valid_from, d.valid_to, d.is_current",
         f"FROM   {m['dim']} AS d",
         f"WHERE  d.{i} IN (SELECT h.{i} FROM {m['dim']} AS h GROUP BY h.{i} HAVING COUNT(*) > 1)",
         f"ORDER BY d.{i}, d.valid_from;",
         "GO",
         "",
         "-- 4. Zero-copy clone: a metadata-only copy, e.g. a safety net before a release (no data is duplicated)",
         f"CREATE TABLE dw.{rf} AS CLONE OF {m['fact']};",
         f"SELECT (SELECT COUNT(*) FROM dw.{rf} AS c) AS clone_rows, (SELECT COUNT(*) FROM {m['fact']} AS f) AS source_rows;",
         "-- Point in time (UTC, inside the retention period):",
         f"-- CREATE TABLE dw.{m['o_fact']}_asof AS CLONE OF {m['fact']} AT '2026-11-16T00:00:00.000';",
         "GO",
         "",
         "-- 5. Time travel: the fact as it was after the first successful load. The hint needs a literal, so the",
         "--    statement is built as a string. Literal form:",
         f"--    SELECT COUNT(*) FROM {m['fact']} OPTION (FOR TIMESTAMP AS OF '2026-11-16T00:00:00.000');",
         "DECLARE @asof DATETIME2(3) = (SELECT MIN(l.ended_at) FROM etl.load_log AS l",
         f"                              WHERE l.object_name = '{m['o_fact']}' AND l.status = 'SUCCESS');",
         "DECLARE @ts VARCHAR(23) = CONVERT(VARCHAR(23), @asof, 126);",
         "DECLARE @sql NVARCHAR(4000) = CONCAT(N'SELECT ''', @ts, N''' AS as_of_utc, COUNT(*) AS rows_then, SUM(CAST(is_open AS INT)) AS open_then ',",
         f"                                    N'FROM {m['fact']} OPTION (FOR TIMESTAMP AS OF ''', @ts, N''');');",
         "IF @asof IS NOT NULL EXEC sp_executesql @sql;",
         f"SELECT COUNT(*) AS rows_now, SUM(CAST(f.is_open AS INT)) AS open_now FROM {m['fact']} AS f;",
         "GO",
         "",
         f"-- 6. Column-level security: a role that can read the model but not {m['amt']}",
         "IF DATABASE_PRINCIPAL_ID('dw_restricted_reader') IS NULL EXEC('CREATE ROLE dw_restricted_reader');",
         "GO",
         "GRANT SELECT ON SCHEMA::dw TO dw_restricted_reader;",
         f"DENY SELECT ON {m['fact']} ({amt}) TO dw_restricted_reader;",
         "-- ALTER ROLE dw_restricted_reader ADD MEMBER [contractor@contoso.example];",
         f"-- As that user: SELECT {amt} FROM {m['fact']};   -> permission denied. SELECT COUNT(*) still works.",
         "GO",
         "",
         f"-- 7. Dynamic data masking on {E['name_col']}: obfuscation for casual browsing, not a security boundary",
         f"ALTER TABLE {m['dim']} ALTER COLUMN {name} ADD MASKED WITH (FUNCTION = 'partial(2, \"xxxx\", 0)');",
         f"-- GRANT UNMASK ON {m['dim']} TO [analyst@contoso.example];",
         "GO",
         "",
         f"-- 8. Row-level security on {reg}. Map people to {p['region_label']}s; '*' sees everything.",
         "--    The loading identity (you now, and the pipeline's connection) must be '*' - etl.usp_run_all checks this.",
         "DELETE FROM sec.user_region WHERE user_principal = USER_NAME();",
         "INSERT INTO sec.user_region (user_principal, region_value) VALUES (USER_NAME(), '*');",
         f"-- INSERT INTO sec.user_region (user_principal, region_value) VALUES ('analyst@contoso.example', '{region1}');",
         "GO",
         "CREATE FUNCTION sec.fn_region_filter(@region VARCHAR(100))",
         "RETURNS TABLE",
         "WITH SCHEMABINDING",
         "AS",
         "RETURN SELECT 1 AS allowed",
         "       FROM   sec.user_region AS u",
         "       WHERE  u.user_principal = USER_NAME() AND (u.region_value = @region OR u.region_value = '*');",
         "GO",
         f"-- Facts carry no {reg}: they are filtered through the {E['label']} key",
         "CREATE FUNCTION sec.fn_region_filter_by_key(@key INT)",
         "RETURNS TABLE",
         "WITH SCHEMABINDING",
         "AS",
         "RETURN SELECT 1 AS allowed",
         f"       FROM   {m['dim']} AS d",
         f"       INNER JOIN sec.user_region AS u ON u.user_principal = USER_NAME() AND (u.region_value = d.{reg} OR u.region_value = '*')",
         f"       WHERE  d.{k} = @key;",
         "GO",
         "CREATE SECURITY POLICY sec.rls_region",
         f"    ADD FILTER PREDICATE sec.fn_region_filter({reg}) ON {m['dim']},",
         f"    ADD FILTER PREDICATE sec.fn_region_filter_by_key({k}) ON {m['fact']},",
         f"    ADD FILTER PREDICATE sec.fn_region_filter_by_key({k}) ON {m['rfact']}",
         "    WITH (STATE = ON);",
         "GO",
         f"-- Check: as '*' you see every {p['region_label']}. Change your row to '{region1}' and run this again.",
         f"SELECT d.{reg}, COUNT(*) AS {V['table']}s FROM {m['fact']} AS f INNER JOIN {m['dim']} AS d ON d.{k} = f.{k} GROUP BY d.{reg};",
         "-- UPDATE sec.user_region SET region_value = '" + region1 + "' WHERE user_principal = USER_NAME();",
         "-- Switch it off for loading or troubleshooting:  ALTER SECURITY POLICY sec.rls_region WITH (STATE = OFF);",
         "GO"]
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------------------------- README
README = """# Warehouse in Gold

The notebooks build Silver in the lakehouse. This module puts a Fabric Data Warehouse on top of it and builds a star
schema with stored procedures. It's the pattern a SQL Server or SSIS team already knows, running on Fabric, reading
the same Silver tables the framework writes. Use it when the conversation is "should Gold be a warehouse?".

```
 lh_mdf (lakehouse)                         wh_mdf (warehouse, same workspace)
 silver.<entity>   (SCD2)  --3-part name-->  stg.*  --procedures-->  dw.dim_<entity>   SCD2, unknown member -1
 silver.<event>    (SCD1)  --3-part name-->  stg.*  --procedures-->  dw.fact_<event>   MERGE, as-at key, late keys
 silver.<reading>  (15min) --3-part name-->  stg.*  --procedures-->  dw.fact_<x>_daily recompute affected days
                                             etl.load_log, etl.watermark, etl.late_arriving   sec.*  RLS / CLS / masking
```

I keep PII out of the warehouse on purpose. There's no party or customer dimension here: that data stays in the
lakehouse, already masked by the framework.

## What's in each pack folder

| Script | What it does |
|---|---|
| `01_setup.sql` | Schemas `dw`, `stg`, `etl`, `sec`. Run log, watermark and late-arriving tables. Staging tables, the star schema, keys (`NOT ENFORCED`), `dw.dim_date` generated for 2020 to 2028 and the `-1` unknown member. Re-runnable: it drops and recreates everything. |
| `02_procs.sql` | One procedure per table plus `etl.usp_run_all`. |
| `03_security_demo.sql` | Reconciliation against Silver, run log, demo queries, zero-copy clone, time travel, column-level security, dynamic data masking and row-level security. |

What the procedures do:

- **Watermarks.** Each table reads Silver incrementally: `_silver_updated_at` for the register and the events,
  `_ingested_at` for readings. Every run appends to `etl.watermark`, so you can show the trail.
- **SCD2 replayed from Silver.** Silver already keeps every version of the register. The dimension procedure stages all
  versions of any changed row and inserts a new warehouse version only when status, the tier column or the capacity
  column changed. Name, type, region and start date are type 1. The first version of a new member is valid from
  1900-01-01, so older events still find it.
- **As-at key lookup.** An event gets the dimension version valid when it was opened, else the current version, else `-1`.
- **Late-arriving keys.** Rows that land on `-1` are logged in `etl.late_arriving` and re-pointed on a later run once
  the member exists. The readings fact recomputes any day that still has `-1` rows.
- **Run control.** Each procedure runs its writes in one transaction inside `TRY/CATCH`, writes to `etl.load_log`, and
  re-throws on error so the pipeline activity fails properly.

## Run it

1. Run the notebook path at least once (`nb_mdf_90_run_all`), so `lh_mdf` has the Silver tables.
2. In the same workspace, select **New item > Warehouse** and name it `wh_mdf`. The scripts use the name `lh_mdf` in
   3-part names, so keep both names or search and replace.
3. In the warehouse, select **New SQL query**. Open `warehouse/<pack>/01_setup.sql`, paste it all and run it.
   Do the same with `02_procs.sql`.
4. Load: `EXEC etl.usp_run_all;` It returns one row per table from the run log.
5. Open `03_security_demo.sql` and run it one section at a time while you talk. The order is: reconcile, run log,
   demo queries, clone, time travel, column-level security, masking, then row-level security last.

The scripts use `GO` between batches. The Fabric SQL query editor runs each `GO` batch in its own session, which is why
nothing relies on variables or transactions across batches.

## What to demo

- **Reconciliation** (section 1 of script 3). Silver and warehouse counts and totals side by side, `OK` or `CHECK`.
  This is the parallel-run evidence people ask for before a cut-over.
- **Incremental and idempotent.** Run `EXEC etl.usp_run_all;` twice. The second run stages nothing for the dimension
  and the event fact, and changes no rows. Then release the next sample day, run the framework and run the procedure
  again: only changed rows move, and the watermark trail grows.
- **SCD2.** The day 2 and day 3 sample data change status and tier on some rows. The "SCD2 history" query in section 3
  shows the versions with `valid_from` and `valid_to`.
- **Late-arriving keys.** In the sample data, day 2 has six events for member 199999, which the source only publishes
  on day 3. The framework holds them in Silver quarantine until then. When they reach the warehouse they still get a
  real key, even though they were opened before the member existed, because a new member's first version starts at
  1900-01-01. To see the `-1` repair itself, run `01_setup.sql` again, run `EXEC etl.usp_load_<event fact>;` on its own
  (every row lands on `-1`), then `EXEC etl.usp_run_all;` and look at section 2 of script 3.
- **Clone and time travel.** A zero-copy clone of the fact, and the fact as it was after the first load.
- **Security.** Column-level security on the amount column, masking on the name column, and row-level security on the
  region column for the dimension and both facts.

Security notes I always say out loud:

- Workspace Admin, Member and Contributor roles see everything unmasked. Test as a user who only has Read on the
  warehouse.
- Row-level security filters everyone, including you. Script 3 maps you to `*` first. The identity the pipeline uses
  must be mapped to `*` too: `etl.usp_run_all` refuses to load if row-level security is set up and the caller isn't
  mapped, because filtered reads would write wrong history.
- These rules apply to T-SQL. Power BI in Direct Lake mode falls back to DirectQuery when the warehouse has
  row-level security. Plan the semantic model with that in mind.

## Call it from the pipeline

In `pl_mdf_orchestrator`, add a **Stored procedure** activity after the Bronze to Silver step:

1. **Settings > Connection**: the warehouse `wh_mdf` (a Fabric Data Warehouse connection).
2. **Stored procedure name**: `etl.usp_run_all`. No parameters.
3. Dependency: **On success** of the Silver step. If you also run the Gold notebook, the two can run in parallel.
4. Add the connection's identity to `sec.user_region` with `*` if you have set up row-level security.

A failure inside any procedure is logged in `etl.load_log` and re-thrown, so the activity goes red with the original
error message.

## Packs

| Pack | Dimension | Event fact | Daily reading fact | Region column | SCD2 columns |
|---|---|---|---|---|---|
{pack_rows}

All packs also get `dw.dim_date` (`date_key`, `calendar_date`, `fy_year`, `fy_label` with FY27 = July 2026 to June 2027).

## Fabric T-SQL notes

These are the limits the scripts are written around. Check the Microsoft Learn page "T-SQL surface area in Fabric
Data Warehouse" if something fails.

- `MERGE` is generally available. It needs the closing semicolon and has no `OUTPUT` clause here.
- `IDENTITY` exists but is `BIGINT` only, with no seed and no `IDENTITY_INSERT`. The scripts use `INT` keys from
  `MAX + ROW_NUMBER`, so `-1` can be the unknown member.
- No sequences, no triggers, no `SET TRANSACTION ISOLATION LEVEL`. Text columns are `VARCHAR` (no `NVARCHAR` columns),
  timestamps are `DATETIME2(6)`, and keys must be `NOT ENFORCED`.
- Only some `ALTER TABLE` options work: add nullable columns, add or drop `NOT ENFORCED` keys, and masking.
- The 3-part names (`lh_mdf.silver.x`) assume the lakehouse and the warehouse sit in the same workspace.

Verify in your tenant:

- The lakehouse SQL analytics endpoint syncs a few seconds to minutes after a notebook writes. If the reconciliation
  shows `CHECK` straight after a load, wait and run it again.
- `sp_executesql` with the time travel hint, `DATABASE_PRINCIPAL_ID`, and `ALTER COLUMN ... ADD MASKED` on an existing
  table. All three are documented, but they're newer than the rest.
- Lakehouse table and column names come from the pack. A pack column that is a T-SQL keyword is written in square
  brackets (for example `[plan]` in the digital pack).

## Rebuild and checks

- `python tools/build_kit.py` rebuilds everything, including these scripts. `python tools/build_wh.py` rebuilds just the
  warehouse folder.
- `python tools/build_wh.py --check` rebuilds the scripts for all nine packs and checks every column, INSERT/MERGE
  and `dw` table against the pack's Silver schema.
"""


def readme(packs):
    rows = []
    for p in packs:
        m = M(p)
        E = p["entity"]
        rows.append(f"| {p['id']} | `{m['dim']}` | `{m['fact']}` | `{m['rfact']}` | `{m['reg']}` | "
                    f"status, {E['tier_col']}, {E['cap_col']} |")
    return README.replace("{pack_rows}", "\n".join(rows))


# ---------------------------------------------------------------------------------------------- build
def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def render(p):
    return {"01_setup.sql": setup_sql(p), "02_procs.sql": procs_sql(p), "03_security_demo.sql": security_sql(p)}


def build(p, kit_dir=kitlib.KIT):
    out = os.path.join(kit_dir, "warehouse", p["id"])
    for name, text in render(p).items():
        write(os.path.join(out, name), text)
    write(os.path.join(kit_dir, "warehouse", "README.md"), readme(kitlib.all_packs()))
    return out


# ---------------------------------------------------------------------------------------------- checks
UNSUPPORTED_TYPES = r"\b(NVARCHAR|NCHAR|NTEXT|TEXT|IMAGE|DATETIME|SMALLDATETIME|DATETIMEOFFSET|MONEY|SMALLMONEY|TINYINT|XML|" \
                    r"GEOGRAPHY|GEOMETRY|SQL_VARIANT|HIERARCHYID|ROWVERSION)\b"
UNSUPPORTED_STMTS = [r"\bCREATE\s+TRIGGER\b", r"\bCREATE\s+SEQUENCE\b", r"\bNEXT\s+VALUE\s+FOR\b", r"\bIDENTITY_INSERT\b",
                     r"\bSET\s+TRANSACTION\s+ISOLATION\b", r"\bCREATE\s+MATERIALIZED\b", r"\bCREATE\s+SYNONYM\b",
                     r"\bSET\s+ROWCOUNT\b", r"\bFOR\s+XML\b", r"\bOUTPUT\s+(inserted|deleted|\$action)", r"\bUSE\s+\w+\s*;",
                     r"\bDATETIME2\(7\)", r"\bIS\s+(NOT\s+)?DISTINCT\s+FROM\b"]


def strip_sql(text):
    """Remove comments and string contents (keeps the quotes so counts still work)."""
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    out, i, n = [], 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if text[j] == "'" and j + 1 < n and text[j + 1] == "'":
                    j += 2
                    continue
                if text[j] == "'":
                    break
                j += 1
            out.append("''")
            i = j + 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def split_top(s, sep=","):
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == sep and depth == 0:
            out.append(cur.strip())
            cur = ""
            continue
        cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def batches(text):
    return re.split(r"^GO\s*$", text, flags=re.M)


def unbr(c):
    return c.strip().strip("[]")


def check(p, files=None):
    errs = []
    files = files or render(p)
    m = M(p)
    sch = silver_schema(p)
    clean = {f: strip_sql(t) for f, t in files.items()}
    allc = "\n".join(clean.values())

    # 1. created tables and their columns
    tables = {}
    for mt in re.finditer(r"CREATE TABLE ((?:dw|stg|etl|sec)\.\w+) \((.*?)\n\);", allc, re.S):
        tables[mt.group(1)] = [unbr(x.split()[0]) for x in split_top(mt.group(2))]
    for mt in re.finditer(r"CREATE TABLE ((?:dw)\.\w+) AS CLONE OF ((?:dw)\.\w+)", allc):
        tables[mt.group(1)] = tables.get(mt.group(2), [])
    procs = set(re.findall(r"CREATE OR ALTER PROCEDURE (etl\.\w+)", allc))
    funcs = set(re.findall(r"CREATE FUNCTION (sec\.\w+)", allc))

    # 2. contract
    for t, cols in contract(p).items():
        if tables.get(t) != cols:
            errs.append(f"contract: {t} has {tables.get(t)} expected {cols}")

    for f, t in clean.items():
        for bi, b in enumerate(batches(t)):
            tag = f"{f} batch {bi}"
            if b.count("(") != b.count(")"):
                errs.append(f"{tag}: parentheses {b.count('(')}/{b.count(')')}")
            if b.count("'") % 2:
                errs.append(f"{tag}: odd number of quotes")
            begins = len(re.findall(r"\bBEGIN\b(?!\s+(TRAN|TRANSACTION|DISTRIBUTED)\b)", b, re.I))
            cases = len(re.findall(r"\bCASE\b", b, re.I))
            ends = len(re.findall(r"\bEND\b", b, re.I))
            if begins + cases != ends:
                errs.append(f"{tag}: BEGIN({begins}) + CASE({cases}) != END({ends})")
            body = b.strip()
            for kw in (r"CREATE OR ALTER PROCEDURE", r"CREATE FUNCTION", r"CREATE SECURITY POLICY"):
                if re.search(r"\b" + kw + r"\b", body) and not body.startswith(kw.replace(r"\b", "")):
                    errs.append(f"{tag}: {kw} must be the first statement in its batch")
            # transactions must open and close in the same batch (each GO batch is its own session in the editor)
            if len(re.findall(r"\bBEGIN TRANSACTION\b", b)) != len(re.findall(r"\bCOMMIT TRANSACTION\b", b)):
                errs.append(f"{tag}: BEGIN/COMMIT TRANSACTION mismatch")
        for pat in UNSUPPORTED_STMTS:
            if re.search(pat, t, re.I):
                errs.append(f"{f}: unsupported statement {pat}")
        for mt in re.finditer(r"CREATE TABLE [\w.]+ \((.*?)\n\);", t, re.S):
            bad = re.findall(UNSUPPORTED_TYPES, mt.group(1), re.I)
            if bad:
                errs.append(f"{f}: unsupported column types {bad}")
        if re.search(r"DATETIME2\((?:[7-9])\)", t):
            errs.append(f"{f}: DATETIME2 precision above 6")

    # 3. per-statement alias checks: Silver columns, warehouse/staging columns
    for f, t in clean.items():
        for b in batches(t):
            for st in split_top(b, ";"):
                if re.search(r"lh_mdf\.silver\.\w+\b(?!\s+AS\s+\w)", st):
                    errs.append(f"{f}: Silver table referenced without an alias: {st[:80]!r}")
                amap = {}
                for mt in re.finditer(r"\b(lh_mdf\.silver\.\w+|(?:dw|stg|etl|sec)\.\w+) AS (\w+)", st):
                    tbl, al = mt.groups()
                    if al in amap and amap[al] != tbl:
                        errs.append(f"{f}: alias {al} used for {amap[al]} and {tbl} in one statement")
                    amap[al] = tbl
                for al, tbl in amap.items():
                    if tbl.startswith("lh_mdf.silver."):
                        cols = set(sch.get(tbl.split(".")[-1], {}))
                        if not cols:
                            errs.append(f"{f}: unknown Silver table {tbl}")
                    else:
                        cols = set(tables.get(tbl, []))
                        if tbl not in tables:
                            errs.append(f"{f}: {tbl} is never created")
                    for c in re.findall(rf"(?<![\w.]){al}\.\[?(\w+)\]?", st):
                        if c not in cols:
                            errs.append(f"{f}: {tbl} ({al}) has no column {c}")
            for mt in re.finditer(r"INSERT INTO ((?:dw|stg|etl|sec)\.\w+) \(([^)]*)\)\s*(VALUES\s*\((.*?)\);|SELECT (.*?)\n\s*FROM)", b, re.S):
                tgt, cl = mt.group(1), split_top(mt.group(2))
                for c in cl:
                    if unbr(c) not in tables.get(tgt, []):
                        errs.append(f"{f}: INSERT into {tgt} names unknown column {c}")
                vals = mt.group(4) if mt.group(4) is not None else mt.group(5)
                if mt.group(5) is not None and "ROW_NUMBER() OVER (ORDER BY v." in vals:
                    vals = vals  # candidate select: counted below from the generator
                if vals is not None and len(split_top(vals)) != len(cl) and "new_key" not in vals:
                    errs.append(f"{f}: INSERT into {tgt}: {len(cl)} columns, {len(split_top(vals))} values")
        for mt in re.finditer(r"MERGE ((?:dw)\.\w+) AS t.*?INSERT \(([^)]*)\)\s*VALUES \((.*?)\);", t, re.S):
            cl, vals = split_top(mt.group(2)), split_top(mt.group(3))
            if len(cl) != len(vals):
                errs.append(f"{f}: MERGE INSERT {len(cl)} columns, {len(vals)} values")
            for c in cl:
                if unbr(c) not in tables.get(mt.group(1), []):
                    errs.append(f"{f}: MERGE INSERT names unknown column {c}")
        for mt in re.finditer(r"UPDATE SET (.*?)\n", t):
            for a in split_top(mt.group(1)):
                c = unbr(a.split("=")[0].split(".")[-1])
                if c not in tables.get(m["fact"], []):
                    errs.append(f"{f}: MERGE UPDATE sets unknown column {c}")
    cand = split_top(dim_candidates_select(p).split("FROM  (")[0].replace("SELECT ", "", 1))
    if len(cand) != len(m["dim_def"]):
        errs.append(f"dim candidate select has {len(cand)} values for {len(m['dim_def'])} columns")

    # 4. EXEC targets, object references, PII, reserved words
    for pr in re.findall(r"\bEXEC (etl\.\w+)", allc):
        if pr not in procs:
            errs.append(f"EXEC of undefined procedure {pr}")
    for ref in set(re.findall(r"\b((?:dw|stg|etl|sec)\.\w+)", allc)):
        if ref not in tables and ref not in procs and ref not in funcs and ref not in ("sec.rls_region",):
            errs.append(f"reference to {ref}, which is never created")
    pii = {"email_masked", "mobile_masked", "birth_year", "first_name", "last_name", p["party"]["table"],
           p["party"]["sensitive_col"], (kitlib.N(p)["p_sens"] or "~")}
    for w in pii:
        if re.search(rf"\b{re.escape(w)}\b", allc):
            errs.append(f"PII or party object in warehouse SQL: {w}")
    for c in set(m["ecols"] + m["vcols"] + m["rcols"]):
        if c.upper() in RESERVED and re.search(rf"(?<![\[\w]){c}(?![\w\]])", allc, re.I):
            errs.append(f"reserved word {c} used without brackets")
    import datetime
    if datetime.date(1900, 1, 1).weekday() != 0:
        errs.append("dim_date weekend rule assumes 1900-01-01 was a Monday")
    if re.search(r"SUM\((?:\w+\.)?is_open\)", allc):
        errs.append("SUM over the BIT column is_open needs a CAST")
    if "GO" not in files["02_procs.sql"]:
        errs.append("02_procs.sql has no GO separators")
    return errs


def sqlite_replay(p):
    """Replays the SCD2 version logic and the as-at key lookup in SQLite on a small scripted history."""
    errs = []
    m = M(p)
    E = p["entity"]
    k, i = m["key"], m["eid"]
    con = sqlite3.connect(":memory:")
    for s in ("dw", "stg"):
        con.execute(f"ATTACH DATABASE ':memory:' AS {s}")
    con.execute(f"CREATE TABLE {m['dim']} ({', '.join(q(c) for c, _ in m['dim_def'])})")
    con.execute(f"CREATE TABLE {m['s_dim']} ({', '.join(q(c) for c, _ in m['stg_dim_def'])})")
    con.execute(f"CREATE TABLE stg.ev (ev_id, {i}, ts)")
    dcols = [c for c, _ in m["dim_def"]]
    con.execute(f"INSERT INTO {m['dim']} VALUES ({', '.join(['?'] * len(dcols))})",
                [-1, -1, "UNKNOWN", "Unknown", "UNKNOWN", "Unknown", "UNKNOWN", None, None, None, MIN_FROM, None, 1])
    tier, cap = E["tier_col"], E["cap_col"]

    def silver(rows):
        """rows: (id, name, status, tier, cap, valid_from, updated). Returns versions with _is_current."""
        out = []
        for r in rows:
            later = [x for x in rows if x[0] == r[0] and x[5] > r[5]]
            out.append(dict(zip(["id", "name", "status", "tier", "cap", "vf", "upd"], r), cur=0 if later else 1))
        return out

    def run(sv_rows, touched):
        con.execute(f"DELETE FROM {m['s_dim']}")
        for r in sv_rows:
            if r["id"] in touched:
                vals = {i: r["id"], E["code"]: f"C{r['id']}", E["name_col"]: r["name"], E["type_col"]: "T", m["reg"]: "R",
                        "status": r["status"], tier: r["tier"], cap: r["cap"], E["start_col"]: "2000-01-01",
                        "src_valid_from": r["vf"], "src_is_current": r["cur"], "src_updated_at": r["upd"]}
                cols = [c for c, _ in m["stg_dim_def"]]
                con.execute(f"INSERT INTO {m['s_dim']} ({', '.join(q(c) for c in cols)}) VALUES ({', '.join(['?'] * len(cols))})",
                            [vals[c] for c in cols])
        max_key = con.execute(f"SELECT COALESCE(MAX({k}), 0) FROM {m['dim']}").fetchone()[0]
        sel = dim_candidates_select(p).replace("@max_key", str(max_key)).replace("@min_from", f"'{MIN_FROM}'")
        con.execute(f"INSERT INTO {m['dim']} ({', '.join(q(c) for c in dcols)}) {sel}")
        for key, nxt in con.execute(dim_close_select(p)).fetchall():
            con.execute(f"UPDATE {m['dim']} SET valid_to = ?, is_current = ? WHERE {k} = ?", (nxt, 1 if nxt is None else 0, key))

    def versions(eid):
        return con.execute(f"SELECT valid_from, valid_to, is_current, status, {q(tier)} FROM {m['dim']} "
                           f"WHERE {i} = ? ORDER BY valid_from", (eid,)).fetchall()

    d1 = [(1, "a", "S1", 3, 10.0, "2026-11-01 06:00:00", "2026-11-02 01:00:00"),
          (1, "a2", "S1", 3, 10.0, "2026-11-02 06:00:00", "2026-11-03 01:00:00"),   # type 1 change only
          (1, "a2", "S2", 3, 10.0, "2026-11-03 06:00:00", "2026-11-03 01:00:00"),
          (2, "b", "S1", 2, None, "2026-11-01 06:00:00", "2026-11-02 01:00:00")]
    run(silver(d1), {1, 2})
    exp1 = [(MIN_FROM, "2026-11-03 06:00:00", 0, "S1", 3), ("2026-11-03 06:00:00", None, 1, "S2", 3)]
    if versions(1) != exp1:
        errs.append(f"sqlite run 1: entity 1 versions {versions(1)} expected {exp1}")
    if versions(2) != [(MIN_FROM, None, 1, "S1", 2)]:
        errs.append(f"sqlite run 1: entity 2 versions {versions(2)}")
    d2 = d1 + [(1, "a2", "S2", 4, 10.0, "2026-11-04 06:00:00", "2026-11-04 01:00:00"),
               (3, "c", "S1", 1, 5.0, "2026-11-04 06:00:00", "2026-11-04 01:00:00")]
    run(silver(d2), {1, 3})
    exp2 = exp1[:1] + [("2026-11-03 06:00:00", "2026-11-04 06:00:00", 0, "S2", 3), ("2026-11-04 06:00:00", None, 1, "S2", 4)]
    if versions(1) != exp2 or versions(3) != [(MIN_FROM, None, 1, "S1", 1)]:
        errs.append(f"sqlite run 2: entity 1 {versions(1)}, entity 3 {versions(3)}")
    before = con.execute(f"SELECT COUNT(*) FROM {m['dim']}").fetchone()[0]
    run(silver(d2), {1, 2, 3})   # full replay with nothing new: idempotent
    if con.execute(f"SELECT COUNT(*) FROM {m['dim']}").fetchone()[0] != before:
        errs.append("sqlite run 3: replay without changes added versions")
    keys = con.execute(f"SELECT COUNT(DISTINCT {k}), COUNT(*) FROM {m['dim']}").fetchone()
    if keys[0] != keys[1]:
        errs.append("sqlite: duplicate surrogate keys")
    # as-at key lookup
    con.executemany("INSERT INTO stg.ev VALUES (?, ?, ?)", [(1, 1, "2025-01-01 00:00:00"), (2, 1, "2026-11-03 12:00:00"),
                                                          (3, 1, None), (4, 9, "2026-11-03 12:00:00")])
    joins, keyx = key_joins(p, f"s.{i}", "s.ts")
    got = dict(con.execute(f"SELECT s.ev_id, {keyx} FROM stg.ev AS s {joins} ORDER BY s.ev_id").fetchall())
    kv = {vf: key for key, vf in con.execute(f"SELECT {k}, valid_from FROM {m['dim']} WHERE {i} = 1").fetchall()}
    exp = {1: kv.get(MIN_FROM), 2: kv.get("2026-11-03 06:00:00"), 3: kv.get("2026-11-04 06:00:00"), 4: -1}
    if got != exp:
        errs.append(f"sqlite key lookup {got} expected {exp}")
    return errs


def main(argv):
    do_check = "--check" in argv
    only = [a for a in argv if not a.startswith("--")]
    packs = [p for p in kitlib.all_packs() if not only or p["id"] in only]
    bad = 0
    for p in packs:
        out = build(p)
        msg = f"{p['id']:14s} -> {os.path.relpath(out, kitlib.KIT)}"
        if do_check:
            files = {f: open(os.path.join(out, f), encoding="utf-8").read() for f in sorted(os.listdir(out))}
            e = check(p, files) + sqlite_replay(p)
            if len(files) > 3:
                e.append(f"{len(files)} files in {out} (max 3)")
            msg += f" | {len(e)} problem(s)"
            for x in e:
                msg += "\n    " + x
            bad += len(e)
        print(msg)
    if do_check:
        print("WAREHOUSE CHECKS PASSED" if not bad else f"WAREHOUSE CHECKS FAILED ({bad})")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
