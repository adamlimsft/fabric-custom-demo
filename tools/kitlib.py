"""Shared helpers for the demo kit build: turns a pack definition (config/<id>.json -> "pack") into
control-table rows, Gold SQL and the names every module uses. Pure Python, no dependencies."""
import json, os

KIT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
PACK_ORDER = ["energy", "telco", "fsi", "manufacturing", "healthcare", "ites", "government", "retail", "digital"]


def load_pack(pid):
    with open(os.path.join(KIT, "config", f"{pid}.json"), encoding="utf-8") as f:
        p = json.load(f)["pack"]
    p.setdefault("impact", dict(col="value_at_risk_aud", label="value at risk", open_cost=1000, breach_cost=500, cap_rate=0,
                                rule="$1,000 per open event + $500 per 15-min threshold breach"))
    return p


def all_packs():
    ids = [f[:-5] for f in os.listdir(os.path.join(KIT, "config")) if f.endswith(".json")]
    ids = [i for i in PACK_ORDER if i in ids] + sorted(i for i in ids if i not in PACK_ORDER)
    return [load_pack(i) for i in ids]


def N(p):
    """Every derived name in one place."""
    E, V, R, Pa, X = p["entity"], p["event"], p["reading"], p["party"], p["extra"]
    S = p["sys"]
    oid = lambda sysk, src: f"{p['prefix']}_{S[sysk]['code'].upper()}_{src.upper()}"
    m = R["metrics"]
    return dict(
        reg=p["region_col"],
        e_oid=oid("ops", E["src"]), v_oid=oid("ops", V["src"]), r_oid=oid("tel", R["src"]),
        p_oid=oid("crm", Pa["src"]), x_oid=oid(X.get("sys", "tel"), X["src"]),
        e_bronze=f"{S['ops']['code']}_{E['src']}", v_bronze=f"{S['ops']['code']}_{V['src']}",
        r_bronze=f"{S['tel']['code']}_{R['src']}", p_bronze=f"{S['crm']['code']}_{Pa['src']}",
        x_bronze=f"{S[X.get('sys', 'tel')]['code']}_{X['src']}",
        e=E["table"], v=V["table"], r=R["table"], pa=Pa["table"], x=X["table"],
        e_land=f"landing/{S['ops']['code']}/{E['src']}", v_land=f"landing/{S['ops']['code']}/{V['src']}",
        r_land=f"landing/{S['tel']['code']}/{R['src']}", p_land=f"landing/{S['crm']['code']}/{Pa['src']}",
        x_land=f"landing/{S[X.get('sys', 'tel')]['code']}/{X['src']}",
        eid=E["id"], vid=V["id"], pid=Pa["id"],
        m1=m[0]["col"], m2=m[1]["col"], m3=m[2]["col"], thr=m[0]["threshold"],
        # gold
        g_date="dim_date", g_dim=f"dim_{E['table']}", g_fact=f"fact_{V['table']}", g_rfact=f"fact_{R['fact']}_hourly",
        g_score=f"{E['table']}_health_scorecard", impact=p.get("impact", {}).get("col"), g_party=f"{Pa['table']}_summary", g_extra=f"{V['table']}_vs_{X['table']}",
        # warehouse
        w_dim=f"dim_{E['table']}", w_fact=f"fact_{V['table']}", w_rfact=f"fact_{R['fact']}_daily",
        open_status=V["status"][:2], closed_status=V["status"][2], cancelled_status=V["status"][3],
        p_email="email_masked", p_mobile="mobile_masked", p_birth="birth_year",
        p_sens=(Pa["sensitive_col"] + "_hash") if Pa["sensitive_treatment"] == "hash" else None,
    )


def sf_name(col):
    """Salesforce-style API name for a CRM custom field."""
    return "".join(w.capitalize() for w in col.split("_")) + "__c"


def party_source_columns(p):
    """(source JSON field, silver column, type, pii treatment, sensitivity)"""
    n, Pa = N(p), p["party"]
    cols = [("Id", Pa["id"], "string", "none", "Internal"),
            ("FirstName", "first_name", "string", "drop", "Confidential"),
            ("LastName", "last_name", "string", "drop", "Confidential"),
            ("Email", "email_masked", "string", "mask_email", "Confidential"),
            ("MobilePhone", "mobile_masked", "string", "mask_phone", "Confidential"),
            ("Birthdate", "birth_year", "int", "year_only", "Confidential"),
            ("MailingPostalCode", "postcode", "string", "none", "Internal"),
            (sf_name(n["reg"]), n["reg"], "string", "none", "Internal"),
            (sf_name(Pa["segment_col"]), Pa["segment_col"], "string", "none", "Internal"),
            (sf_name(p["entity"]["id"]), p["entity"]["id"], "int", "none", "Internal"),
            ("Status__c", "status", "string", "none", "Internal"),
            (sf_name(Pa["sensitive_col"]), n["p_sens"] or Pa["sensitive_col"],
             "string", Pa["sensitive_treatment"], "Restricted"),
            ("LastModifiedDate", "last_modified_ts", "timestamp", "none", "Internal")]
    return cols


def control_rows(p):
    n, E, V, R, Pa, X, S = N(p), p["entity"], p["event"], p["reading"], p["party"], p["extra"], p["sys"]
    ind = p["id"]
    reg = n["reg"]
    csv_opts = json.dumps({"header": True, "delimiter": ","})
    systems = [dict(source_system=S[k]["code"], industry=ind, display_name=S[k]["name"], system_type=S[k]["type"],
                    connection_pattern={"ops": "On-premises data gateway -> pipeline Copy with watermark query",
                                        "tel": "Files dropped to OneLake (Files/landing) every 15 minutes",
                                        "crm": "REST/OData API -> JSON landed per day"}[k],
                    technical_owner=S[k]["owner"], fabric_options=S[k]["fabric"]) for k in ("ops", "tel", "crm")]

    def obj(object_id, syskey, src, stype, method, fmt, path, opts, table, load, wm, keys, order, bronze, silver,
            mode, group, prio, sla, owner, sens, desc, alt, active=True):
        return dict(object_id=object_id, industry=ind, source_system=S[syskey]["code"], source_object=src,
                    source_type=stype, ingest_method=method, landing_format=fmt, landing_path=path, read_options=opts,
                    source_table=table, load_type=load, watermark_column=wm, primary_keys=keys, dedupe_order_by=order,
                    bronze_table=bronze, silver_table=silver, silver_write_mode=mode, load_group=group, priority=prio,
                    sla_hours=sla, data_owner=owner, sensitivity=sens, active=active, description=desc,
                    fabric_alternative=alt)

    objects = [
        obj(n["e_oid"], "ops", E["src"], "database", "copy_db", "csv", n["e_land"], csv_opts, f"src.{E['src']}",
            "incremental", "last_modified", E["id"], "last_modified DESC, _ingested_at DESC", n["e_bronze"], E["table"],
            "merge_scd2", "reference", 10, 26, E["owner"], "Internal",
            f"{E['label'].capitalize()} register. SCD2 keeps status, {E['tier_col']} and {E['cap_col']} history.",
            "Strong mirroring candidate - small, slowly changing, needed in near real time by operations"),
        obj(n["v_oid"], "ops", V["src"], "database", "copy_db", "csv", n["v_land"], csv_opts, f"src.{V['src']}",
            "incremental", "last_modified", V["id"], "last_modified DESC, _ingested_at DESC", n["v_bronze"], V["table"],
            "merge_scd1", "transactional", 20, 26, V["owner"], "Internal",
            f"{V['plural'].capitalize()} - incremental by last_modified; can arrive before their {E['label']} (late-arriving).",
            "Copy job incremental or mirroring; keep the watermark query if the source is on-prem"),
        obj(n["r_oid"], "tel", R["src"], "file", "landed_file", "csv", n["r_land"], csv_opts, None,
            "incremental", None, f"{E['id']},reading_ts", "_ingested_at DESC", n["r_bronze"], R["table"],
            "insert_only", "telemetry", 30, 2, R["owner"], "Internal",
            f"{R['label'].capitalize()} every 15 minutes, four micro-batch files a day. Insert-only and idempotent.",
            "Eventstream into Eventhouse for real time; keep this batch path for history and reconciliation"),
        obj(n["p_oid"], "crm", Pa["src"], "api", "landed_file", "json", n["p_land"],
            json.dumps({"multiline": True, "json_root": "records"}), None,
            "full", None, Pa["id"], "LastModifiedDate DESC, _ingested_at DESC".replace("LastModifiedDate", "last_modified_ts"),
            n["p_bronze"], Pa["table"], "merge_scd1", "reference", 15, 26, Pa["owner"], "Confidential",
            f"{Pa['plural'].capitalize()} - full daily snapshot from the CRM API (contains PII). Missing rows are soft-deleted.",
            "Copy job with the CRM connector; Dataverse-based CRMs can use Link to Fabric instead of copying"),
    ]
    cmap = []
    def add(oid, rows):
        for i, r in enumerate(rows):
            s, t, d, e, pii, sens = (list(r) + [None, "none", "Internal"])[:6] if len(r) < 6 else r
            cmap.append(dict(object_id=oid, column_order=i, source_column=s, target_column=t, data_type=d,
                             transform_expr=e, pii_treatment=pii or "none", sensitivity=sens or "Internal", description=""))
    add(n["e_oid"], [(E["id"], E["id"], "int"), (E["code"], E["code"], "string"), (E["name_col"], E["name_col"], "string"),
                     (E["type_col"], E["type_col"], "string"), (reg, reg, "string"), ("status", "status", "string"),
                     (E["tier_col"], E["tier_col"], "int"), (E["cap_col"], E["cap_col"], "double"),
                     (E["start_col"], E["start_col"], "date"), ("last_modified", "last_modified", "timestamp")])
    add(n["v_oid"], [(V["id"], V["id"], "int"), (E["id"], E["id"], "int"), (V["type_col"], V["type_col"], "string"),
                     ("priority", "priority", "int"), ("status", "status", "string"),
                     (V["open_ts"], V["open_ts"], "timestamp"), (V["close_ts"], V["close_ts"], "timestamp"),
                     (V["amount_col"], V["amount_col"], "decimal(12,2)"), (V["qty_col"], V["qty_col"], "double"),
                     ("description", "description", "string"), ("last_modified", "last_modified", "timestamp")])
    add(n["r_oid"], [(E["id"], E["id"], "int"),
                     ("reading_time", "reading_ts", "timestamp", "to_timestamp(reading_time, 'dd/MM/yyyy HH:mm')")] +
        [(m["col"], m["col"], "double") for m in R["metrics"]])
    add(n["p_oid"], [(s, t, d, None, pii, sens) for s, t, d, pii, sens in party_source_columns(p)])

    pre = p["prefix"]
    def rule(rid, oid, name, cat, check, col, params, sev, owner, desc, thr=None):
        return dict(rule_id=f"{pre}-{rid}", object_id=oid, rule_name=name, rule_category=cat, check_type=check,
                    column_name=col, params=json.dumps(params), severity=sev, fail_threshold_pct=thr,
                    rule_owner=owner, description=desc, active=True)
    de = "Data engineering"
    m1 = R["metrics"][0]
    rules = [
        rule("ENT-01", n["e_oid"], f"{E['label'].capitalize()} id present", "technical", "not_null", E["id"], {}, "fail", de,
             "Primary key must never be null", 0),
        rule("ENT-02", n["e_oid"], f"Known {E['type_col']}", "technical", "allowed_values", E["type_col"], {"values": E["types"]},
             "warn", de, "Unknown classes are kept but flagged for the data owner"),
        rule("ENT-03", n["e_oid"], f"{E['tier_col']} between 1 and 5", "technical", "range", E["tier_col"], {"min": 1, "max": 5},
             "warn", de, "Out-of-range ratings usually mean a keying error in the source"),
        rule("ENT-04", n["e_oid"], "Known status", "business", "allowed_values", "status", {"values": E["status"]},
             "warn", E["owner"], f"Status list agreed with {E['owner']}"),
        rule("EVT-01", n["v_oid"], f"{V['label'].capitalize()} id present", "technical", "not_null", V["id"], {}, "fail", de,
             "Primary key must never be null", 0),
        rule("EVT-02", n["v_oid"], f"{E['label'].capitalize()} exists", "technical", "referential", E["id"],
             {"ref_table": f"silver.{E['table']}", "ref_column": E["id"]}, "quarantine", de,
             f"Held until the {E['label']} arrives (late-arriving reference data), then merged automatically"),
        rule("EVT-03", n["v_oid"], f"{V['close_verb'].capitalize()} after {V['open_verb']}", "business", "expression", None,
             {"expr": f"{V['close_ts']} IS NULL OR {V['close_ts']} >= {V['open_ts']}"}, "quarantine", V["owner"],
             f"A {V['label']} can't be {V['close_verb']} before it was {V['open_verb']}"),
        rule("EVT-04", n["v_oid"], f"{V['amount_col']} not negative", "technical", "range", V["amount_col"], {"min": 0},
             "warn", de, "Negative values are sign errors in the source extract"),
        rule("EVT-05", n["v_oid"], f"{V['urgent_type']} is priority 1", "business", "expression", None,
             {"expr": f"{V['type_col']} <> '{V['urgent_type']}' OR priority = 1"}, "warn", V["owner"],
             f"{V['type_names'][V['types'].index(V['urgent_type'])]} {V['plural']} must be raised as priority 1"),
        rule("EVT-06", n["v_oid"], f"{V['open_ts']} parses", "technical", "not_null", V["open_ts"], {}, "fail", de,
             "If more than 5% of timestamps fail to parse, the extract format has changed - stop the load", 5),
        rule("RDG-01", n["r_oid"], f"{m1['label'].capitalize()} plausible", "technical", "range", m1["col"],
             {"min": m1["min"], "max": m1["max"]}, "quarantine", de,
             f"Sensor fault values (e.g. {R['sentinel']}) are quarantined, never averaged into Gold"),
        rule("RDG-02", n["r_oid"], f"{E['label'].capitalize()} known", "technical", "referential", E["id"],
             {"ref_table": f"silver.{E['table']}", "ref_column": E["id"]}, "warn", de, "Readings for unknown tags are flagged"),
        rule("RDG-03", n["r_oid"], "Reading time parses", "technical", "not_null", "reading_ts", {}, "fail", de,
             "Timestamp format change stops the load", 5),
        rule("PTY-01", n["p_oid"], f"{Pa['label'].capitalize()} id present", "technical", "not_null", Pa["id"], {}, "fail", de,
             "Primary key must never be null", 0),
        rule("PTY-02", n["p_oid"], "Email looks valid", "technical", "regex", "email_masked",
             {"pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}, "warn", de, "Checked on the raw value before masking"),
        rule("PTY-03", n["p_oid"], "Australian postcode", "technical", "regex", "postcode", {"pattern": r"^\d{4}$"}, "warn", de,
             "Four digits"),
        rule("PTY-04", n["p_oid"], f"Known {Pa['segment_col']}", "business", "allowed_values", Pa["segment_col"],
             {"values": Pa["segments"]}, "warn", Pa["owner"], f"Segments agreed with {Pa['owner']}"),
    ]
    golds = gold_rows(p)
    return dict(source_system=systems, ingestion_object=objects, column_map=cmap, dq_rule=rules, gold_transform=golds,
                onboard=onboard_spec(p))


def _in(vals):
    return ", ".join(f"'{v}'" for v in vals)


def gold_rows(p):
    n, E, V, R, Pa = N(p), p["entity"], p["event"], p["reading"], p["party"]
    I = p["impact"]
    reg, ind = n["reg"], p["id"]
    open_in = _in(n["open_status"])
    mets = R["metrics"]
    m1 = mets[0]
    dim_date = ("SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date, year(d) AS calendar_year, "
                "quarter(d) AS calendar_quarter, month(d) AS month_number, date_format(d, 'MMM') AS month_name, "
                "date_format(d, 'yyyy-MM') AS year_month, dayofmonth(d) AS day_of_month, date_format(d, 'EEE') AS day_name, "
                "CASE WHEN month(d) >= 7 THEN year(d) + 1 ELSE year(d) END AS fy_year, "
                "concat('FY', substring(CAST(CASE WHEN month(d) >= 7 THEN year(d) + 1 ELSE year(d) END AS STRING), 3, 2)) AS fy_label, "
                "CASE WHEN dayofweek(d) IN (1, 7) THEN true ELSE false END AS is_weekend "
                "FROM (SELECT explode(sequence(to_date('2020-01-01'), to_date('2028-12-31'), interval 1 day)) AS d) dates")
    dim = (f"SELECT {E['id']}, {E['code']}, {E['name_col']}, {E['type_col']}, {reg}, status, {E['tier_col']}, {E['cap_col']}, "
           f"{E['start_col']}, CAST(floor(months_between(current_date(), {E['start_col']}) / 12) AS INT) AS age_years, "
           f"_valid_from AS version_valid_from FROM silver.{E['table']} WHERE _is_current = true")
    fact = (f"SELECT v.{V['id']}, v.{E['id']}, v.{V['type_col']}, v.priority, v.status, v.{V['open_ts']}, v.{V['close_ts']}, "
            f"to_date(v.{V['open_ts']}) AS opened_date, CAST(date_format(v.{V['open_ts']}, 'yyyyMMdd') AS INT) AS opened_date_key, "
            f"ROUND((unix_timestamp(v.{V['close_ts']}) - unix_timestamp(v.{V['open_ts']})) / 3600.0, 2) AS hours_to_close, "
            f"CASE WHEN v.status IN ({open_in}) THEN 1 ELSE 0 END AS is_open, "
            f"v.{V['amount_col']}, v.{V['qty_col']}, d.{reg}, d.{E['type_col']}, d.{E['tier_col']} "
            f"FROM silver.{V['table']} v LEFT JOIN silver.{E['table']} d ON d.{E['id']} = v.{E['id']} AND d._is_current = true "
            f"WHERE v._is_deleted = false")
    agg = ", ".join([f"ROUND(AVG({m1['col']}), 2) AS avg_{m1['col']}", f"ROUND(MAX({m1['col']}), 2) AS max_{m1['col']}"] +
                    [f"ROUND(AVG({m['col']}), 2) AS avg_{m['col']}" for m in mets[1:]])
    rfact = (f"SELECT {E['id']}, date_trunc('HOUR', reading_ts) AS reading_hour, to_date(reading_ts) AS reading_date, "
             f"CAST(date_format(reading_ts, 'yyyyMMdd') AS INT) AS reading_date_key, COUNT(*) AS readings, {agg}, "
             f"SUM(CASE WHEN {m1['col']} > {m1['threshold']} THEN 1 ELSE 0 END) AS breach_readings "
             f"FROM silver.{R['table']} GROUP BY {E['id']}, date_trunc('HOUR', reading_ts), to_date(reading_ts), "
             f"CAST(date_format(reading_ts, 'yyyyMMdd') AS INT)")
    score = (f"WITH asat AS (SELECT MAX(to_date({V['open_ts']})) AS as_at FROM silver.{V['table']}), "
             f"ev AS (SELECT v.{E['id']}, COUNT(*) AS {V['table']}s_12m, "
             f"SUM(CASE WHEN v.status IN ({open_in}) THEN 1 ELSE 0 END) AS open_{V['table']}s, "
             f"SUM(CASE WHEN v.{V['type_col']} = '{V['urgent_type']}' THEN 1 ELSE 0 END) AS urgent_{V['table']}s_12m, "
             f"ROUND(SUM(v.{V['amount_col']}), 2) AS {V['amount_col']}_12m "
             f"FROM silver.{V['table']} v CROSS JOIN asat WHERE v._is_deleted = false "
             f"AND to_date(v.{V['open_ts']}) > add_months(asat.as_at, -12) GROUP BY v.{E['id']}), "
             f"rd AS (SELECT {E['id']}, MAX({m1['col']}) AS max_{m1['col']}, "
             f"SUM(CASE WHEN {m1['col']} > {m1['threshold']} THEN 1 ELSE 0 END) AS breach_readings "
             f"FROM silver.{R['table']} GROUP BY {E['id']}), "
             f"s AS (SELECT d.{E['id']}, d.{E['code']}, d.{E['name_col']}, d.{E['type_col']}, d.{reg}, d.status, d.{E['tier_col']}, "
             f"COALESCE(ev.{V['table']}s_12m, 0) AS {V['table']}s_12m, COALESCE(ev.open_{V['table']}s, 0) AS open_{V['table']}s, "
             f"COALESCE(ev.urgent_{V['table']}s_12m, 0) AS urgent_{V['table']}s_12m, "
             f"COALESCE(ev.{V['amount_col']}_12m, 0) AS {V['amount_col']}_12m, rd.max_{m1['col']}, "
             f"COALESCE(rd.breach_readings, 0) AS breach_readings, d.{E['cap_col']} "
             f"FROM silver.{E['table']} d LEFT JOIN ev ON ev.{E['id']} = d.{E['id']} LEFT JOIN rd ON rd.{E['id']} = d.{E['id']} "
             f"WHERE d._is_current = true) "
             f"SELECT s.*, ROUND(LEAST(100, COALESCE(s.{E['tier_col']}, 1) * 8 + s.open_{V['table']}s * 6 "
             f"+ s.urgent_{V['table']}s_12m * 4 + LEAST(s.breach_readings, 20) * 1.5), 1) AS risk_score, "
             f"CASE WHEN LEAST(100, COALESCE(s.{E['tier_col']}, 1) * 8 + s.open_{V['table']}s * 6 + s.urgent_{V['table']}s_12m * 4 "
             f"+ LEAST(s.breach_readings, 20) * 1.5) >= 60 THEN 'High' "
             f"WHEN LEAST(100, COALESCE(s.{E['tier_col']}, 1) * 8 + s.open_{V['table']}s * 6 + s.urgent_{V['table']}s_12m * 4 "
             f"+ LEAST(s.breach_readings, 20) * 1.5) >= 35 THEN 'Medium' ELSE 'Low' END AS risk_band, "
             f"ROUND(s.open_{V['table']}s * {I['open_cost']} + LEAST(s.breach_readings, 20) * {I['breach_cost']} "
             f"+ COALESCE(s.{E['cap_col']}, 0) * COALESCE(s.{E['tier_col']}, 1) * {I['cap_rate']}, 0) AS {I['col']} FROM s")
    party = (f"SELECT {reg}, {Pa['segment_col']}, COUNT(*) AS {Pa['table']}s, "
             f"SUM(CASE WHEN status = '{Pa['status'][0]}' THEN 1 ELSE 0 END) AS active_{Pa['table']}s, "
             f"ROUND(AVG(year(current_date()) - birth_year), 1) AS avg_age, "
             f"COUNT(DISTINCT {E['id']}) AS linked_{E['table']}s "
             f"FROM silver.{Pa['table']} WHERE _is_deleted = false GROUP BY {reg}, {Pa['segment_col']}")
    mk = lambda tid, table, order, sql, desc, owner: dict(transform_id=f"{p['prefix']}-{tid}", industry=ind, gold_table=table,
                                                         run_order=order, sql_text=sql, description=desc, business_owner=owner,
                                                         consumers="Power BI / Direct Lake, data agent", active=True)
    return [mk("G01", "dim_date", 100, dim_date, "Calendar with Australian financial year", "Data engineering"),
            mk("G02", n["g_dim"], 110, dim, f"Current view of the {E['label']} register (history stays in Silver SCD2)", E["owner"]),
            mk("G03", n["g_fact"], 120, fact, f"{V['plural'].capitalize()} with duration and {E['label']} attributes", V["owner"]),
            mk("G04", n["g_rfact"], 130, rfact, f"Hourly {R['label']} with threshold breaches", R["owner"]),
            mk("G05", n["g_score"], 200, score, f"{E['label'].capitalize()} risk score - {V['plural']}, urgent work and "
               f"{m1['label']} breaches, plus {I['label']} in AUD (weights and dollar rates are illustrative: {I['rule']})", E["owner"]),
            mk("G06", n["g_party"], 210, party, f"{Pa['plural'].capitalize()} by {p['region_label']} and {Pa['segment_col']}",
               Pa["owner"])]


def onboard_spec(p):
    n, E, V, X = N(p), p["entity"], p["event"], p["extra"]
    reg = n["reg"]
    obj = dict(object_id=n["x_oid"], industry=p["id"], source_system=p["sys"][X.get("sys", "tel")]["code"], source_object=X["src"],
               source_type="file", ingest_method="landed_file", landing_format="txt", landing_path=n["x_land"],
               read_options=json.dumps({"header": True, "delimiter": "|"}), source_table=None, load_type="incremental",
               watermark_column=None, primary_keys=f"{reg},obs_date", dedupe_order_by="_ingested_at DESC",
               bronze_table=n["x_bronze"], silver_table=X["table"], silver_write_mode="merge_scd1", load_group="reference",
               priority=25, sla_hours=26, data_owner=X["owner"], sensitivity="Public", active=True,
               description=f"Daily {X['label']} by {p['region_label']} (pipe-delimited, dd/MM/yyyy) - onboarded live in the demo",
               fabric_alternative="Public or partner feed - could be a REST copy, or a shortcut if the provider shares a folder")
    cols = [(reg, reg, "string", None), ("obs_date", "obs_date", "date", "to_date(obs_date, 'dd/MM/yyyy')")] + \
           [(c["col"], c["col"], "double", None) for c in X["cols"]] + [(X["cat_col"], X["cat_col"], "string", None)]
    c1 = X["cols"][0]
    rules = [(f"{p['prefix']}-EXT-01", f"{c1['col']} plausible", "technical", "range", c1["col"],
              json.dumps({"min": c1["min"], "max": c1["max"]}), "warn"),
             (f"{p['prefix']}-EXT-02", f"Known {X['cat_col']}", "business", "allowed_values", X["cat_col"],
              json.dumps({"values": X["cat_values"]}), "warn")]
    xcols = ", ".join(f"x.{c['col']}" for c in X["cols"])
    sql = (f"WITH ev AS (SELECT d.{reg}, to_date(v.{V['open_ts']}) AS obs_date, COUNT(*) AS {V['table']}s, "
           f"SUM(CASE WHEN v.{V['type_col']} = '{V['urgent_type']}' THEN 1 ELSE 0 END) AS urgent_{V['table']}s "
           f"FROM silver.{V['table']} v JOIN silver.{E['table']} d ON d.{E['id']} = v.{E['id']} AND d._is_current = true "
           f"WHERE v._is_deleted = false GROUP BY d.{reg}, to_date(v.{V['open_ts']})) "
           f"SELECT x.{reg}, x.obs_date, {xcols}, x.{X['cat_col']}, COALESCE(ev.{V['table']}s, 0) AS {V['table']}s, "
           f"COALESCE(ev.urgent_{V['table']}s, 0) AS urgent_{V['table']}s "
           f"FROM silver.{X['table']} x LEFT JOIN ev ON ev.{reg} = x.{reg} AND ev.obs_date = x.obs_date "
           f"WHERE x._is_deleted = false")
    gold = dict(transform_id=f"{p['prefix']}-G07", industry=p["id"], gold_table=n["g_extra"], run_order=220, sql_text=sql,
                description=f"{X['story'].capitalize()}", business_owner=X["owner"], consumers="Power BI / Direct Lake", active=True)
    return dict(object=obj, columns=cols, rules=rules, gold=gold)
