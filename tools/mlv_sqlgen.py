# %% [markdown]
# # nb_mdf_mlv_sqlgen - generate materialized lake view (MLV) DDL from the MDF control tables
#
# Pure Python (no Spark calls) so the same code runs in Fabric (`%run nb_mdf_mlv_sqlgen`) and offline
# (`mlv/tools/generate_mlv_sql.py`). It turns the SAME metadata the notebook path uses into Spark SQL:
#
# | Metadata | Becomes in the MLV path |
# |---|---|
# | `meta.ingestion_object` (keys, `dedupe_order_by`, `load_type`, `silver_write_mode`) | De-duplication window, full-snapshot soft delete, SCD2 history built from Bronze with `LAG`/`LEAD` |
# | `meta.column_map` (rename, type, `transform_expr`) | `try_cast(expr AS type) AS target` projection |
# | `meta.column_map.pii_treatment` | hash / mask / year-only / redact / drop in the final SELECT (salt read from `meta.setting`) |
# | `meta.dq_rule` severity `quarantine` | `CONSTRAINT ... CHECK (...) ON MISMATCH DROP` + a `quarantine_mlv.<entity>` view of the dropped rows |
# | `meta.dq_rule` severity `fail` (threshold 0) | `CONSTRAINT ... ON MISMATCH FAIL` (threshold > 0 cannot be expressed -> DROP + quarantine) |
# | `meta.dq_rule` severity `warn` | `_dq_warnings` array column (row kept, flagged) + `quarantine_mlv.dq_rule_summary` |
# | `meta.gold_transform.sql_text` | `gold_mlv.<table>` - the SAME SQL, schema names re-pointed |
#
# Rules are evaluated on the raw (pre-PII) values, exactly like the notebook path. A rule whose column is
# PII-treated, a referential rule (needs a join) or a unique rule (needs a window) is computed as a
# `_dq_ok_<rule>` flag column and the constraint checks that flag.

# %%
import json, re

MLV_SCHEMAS = {"bronze": "bronze", "silver": "silver_mlv", "gold": "gold_mlv",
               "quarantine": "quarantine_mlv", "bronze_files": "bronze_mlv", "meta": "meta"}
MLV_AUDIT_COLS = ["_source_system", "_object_id", "_load_date", "_ingested_at", "_batch_id", "_source_file"]
MLV_PII_SALT_DEFAULT = "demo-salt-change-me"
DIRECT_RULE_TYPES = {"not_null", "regex", "range", "allowed_values", "expression"}


def _bq(name):
    return f"`{name}`"


def _sql_str(v):
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_lit(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    return _sql_str(v)


def _comment(text):
    return '"' + str(text or "").replace("\\", " ").replace('"', "'").replace("\n", " ")[:900] + '"'


def _rid(rule):
    return re.sub(r"\W", "_", rule["rule_id"]).lower()


def _keys(obj):
    return [k.strip() for k in (obj.get("primary_keys") or "").split(",") if k.strip()]


def _params(rule):
    p = rule.get("params") or "{}"
    return json.loads(p) if isinstance(p, str) else dict(p)


def _rule_columns(rule):
    if rule["check_type"] == "expression":
        return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", _params(rule).get("expr", "")))
    return {c.strip() for c in (rule.get("column_name") or "").split(",") if c.strip()}


def rule_condition(rule):
    """Boolean Spark SQL for one row-level rule (not for unique / referential). NULL-safe like the notebook."""
    t, p, c = rule["check_type"], _params(rule), rule.get("column_name")
    col = _bq(c) if c else None
    if t == "not_null":
        cond = f"{col} IS NOT NULL"
    elif t == "regex":
        cond = f"{col} IS NULL OR CAST({col} AS STRING) RLIKE {_sql_str(p['pattern'])}"
    elif t == "range":
        parts = []
        if p.get("min") is not None:
            parts.append(f"{col} >= {_sql_lit(p['min'])}")
        if p.get("max") is not None:
            parts.append(f"{col} <= {_sql_lit(p['max'])}")
        cond = f"{col} IS NULL OR ({' AND '.join(parts) or 'true'})"
    elif t == "allowed_values":
        cond = f"{col} IS NULL OR {col} IN ({', '.join(_sql_lit(v) for v in p['values'])})"
    elif t == "expression":
        cond = p["expr"]
    else:
        raise ValueError(f"rule_condition does not handle {t}")
    return f"coalesce(({cond}), false)"


def _pii_expr(m, src_col):
    t = (m.get("pii_treatment") or "none")
    c = _bq(src_col)
    if t == "none":
        return c
    if t == "hash":
        return f"CASE WHEN {c} IS NOT NULL THEN sha2(concat(_salt._pii_salt, CAST({c} AS STRING)), 256) END"
    if t == "mask_email":
        return f"regexp_replace({c}, '^(.)[^@]*(@.*)$', '$1***$2')"
    if t == "mask_phone":
        return f"CASE WHEN {c} IS NOT NULL THEN concat('*******', substring({c}, -3, 3)) END"
    if t == "year_only":
        return f"CAST(year(to_date({c})) AS INT)"
    if t == "redact":
        return "CAST(NULL AS STRING)"
    if t == "drop":
        return None
    raise ValueError(f"Unknown pii_treatment {t} on {m['object_id']}.{m['target_column']}")


def _order_by(obj, available):
    out = []
    for part in (obj.get("dedupe_order_by") or "_ingested_at DESC").split(","):
        bits = part.strip().split()
        if not bits or bits[0] not in available:
            continue
        direction = "ASC" if (len(bits) > 1 and bits[1].upper() == "ASC") else "DESC"
        out.append(f"{_bq(bits[0])} {direction} NULLS LAST")
    return ", ".join(out) or "`_ingested_at` DESC NULLS LAST"


def _resolve_ref(ref_table, silver_tables):
    """'silver.asset' -> 'silver_mlv.asset' when that entity is generated too, otherwise leave as-is."""
    schema, _, name = ref_table.partition(".")
    if schema == "silver" and name in silver_tables:
        return f"{MLV_SCHEMAS['silver']}.{name}", f"silver:{name}"
    return ref_table, None


def silver_sql(obj, cmap, rules, silver_tables, bronze_columns=None):
    """Returns dict(silver=ddl, quarantine=ddl|None, deps=[...], notes=[...], warnings=[...])."""
    ent = obj["silver_table"]
    keys = _keys(obj)
    bronze = f"{MLV_SCHEMAS['bronze']}.{obj['bronze_table']}"
    notes, warnings, deps = [], [], []
    bcols = None if bronze_columns is None else {c.lower() for c in bronze_columns}

    # 1. projection from the column map (raw values; PII applied at the very end)
    proj, targets, pii_cols = [], [], {}
    for m in cmap:
        src, tgt, typ = m["source_column"], m["target_column"], (m.get("data_type") or "string")
        base = m.get("transform_expr") or _bq(src)
        if not m.get("transform_expr") and bcols is not None and src.lower() not in bcols:
            warnings.append(f"missing source column {src} -> NULL")
            proj.append(f"CAST(NULL AS {typ}) AS {_bq(tgt)}")
        elif m.get("pii_treatment") == "year_only":
            proj.append(f"{base} AS {_bq(tgt)}")
        else:
            proj.append(f"try_cast(({base}) AS {typ}) AS {_bq(tgt)}")
        targets.append(tgt)
        if (m.get("pii_treatment") or "none") != "none":
            pii_cols[tgt] = m
    audit = [c for c in MLV_AUDIT_COLS if bcols is None or c in bcols]
    if "_ingested_at" not in audit:
        raise ValueError(f"{bronze} has no _ingested_at column")
    cols = targets + audit
    col_list = ", ".join(_bq(c) for c in cols)
    key_list = ", ".join(_bq(k) for k in keys)
    order = _order_by(obj, set(cols))
    is_full = obj.get("load_type") == "full"
    scd2 = obj.get("silver_write_mode") == "merge_scd2"

    ctes = [f"src AS (\n  SELECT\n    " + ",\n    ".join(proj + [_bq(a) for a in audit]) + f"\n  FROM {bronze}\n)"]
    if scd2:
        eff = (obj.get("dedupe_order_by") or "").split(",")[0].strip().split(" ")[0]
        eff_expr = f"coalesce(CAST({_bq(eff)} AS TIMESTAMP), `_ingested_at`)" if eff in targets else "`_ingested_at`"
        hash_cols = [c for c in targets if c not in keys]
        hash_expr = "sha2(concat_ws('||', " + ", ".join(f"coalesce(CAST({_bq(c)} AS STRING), '~')" for c in hash_cols) + "), 256)"
        ctes += [
            f"versions AS (\n  SELECT {col_list}, {eff_expr} AS `_valid_from`,\n"
            f"         ROW_NUMBER() OVER (PARTITION BY {key_list}, {eff_expr} ORDER BY `_ingested_at` DESC) AS _rn\n  FROM src\n)",
            f"hashed AS (\n  SELECT {col_list}, `_valid_from`, {hash_expr} AS `_record_hash`\n  FROM versions WHERE _rn = 1\n)",
            f"changes AS (\n  SELECT *, LAG(`_record_hash`) OVER (PARTITION BY {key_list} ORDER BY `_valid_from`) AS _prev_hash\n  FROM hashed\n)",
            f"cur AS (\n  SELECT {col_list}, `_valid_from`,\n"
            f"         LEAD(`_valid_from`) OVER (PARTITION BY {key_list} ORDER BY `_valid_from`) AS `_valid_to`,\n"
            f"         false AS `_is_deleted`\n  FROM changes\n  WHERE _prev_hash IS NULL OR _prev_hash <> `_record_hash`\n)"]
        lifecycle = "`_valid_from`, `_valid_to`, (`_valid_to` IS NULL) AS `_is_current`, `_is_deleted`"
        notes.append("SCD2 rebuilt declaratively from Bronze history (LAG/LEAD) - full refresh by design")
    else:
        deleted = "(r.`_load_date` < m._max_load_date)" if is_full else "false"
        join_m = f"\n  CROSS JOIN (SELECT max(`_load_date`) AS _max_load_date FROM {bronze}) m" if is_full else ""
        ctes += [
            f"ranked AS (\n  SELECT {col_list},\n         ROW_NUMBER() OVER (PARTITION BY {key_list} ORDER BY {order}) AS _rn\n  FROM src\n)",
            f"cur AS (\n  SELECT " + ", ".join(f"r.{_bq(c)}" for c in cols) + f", {deleted} AS `_is_deleted`\n  FROM ranked r{join_m}\n  WHERE r._rn = 1\n)"]
        lifecycle = ("`_ingested_at` AS `_valid_from`, CAST(NULL AS TIMESTAMP) AS `_valid_to`, "
                     "true AS `_is_current`, `_is_deleted`")
        if is_full:
            notes.append("full snapshot: keys missing from the latest load are kept with _is_deleted = true")

    # 2. data-quality flags (computed on raw values, before PII)
    flag_exprs, joins, direct, flagged = [], [], [], []
    for i, r in enumerate(rules):
        rid, t = _rid(r), r["check_type"]
        if t == "referential":
            p = _params(r)
            ref, dep = _resolve_ref(p["ref_table"], silver_tables)
            if dep:
                deps.append(dep)
            alias = f"ref{i}"
            joins.append(f"  LEFT JOIN (SELECT DISTINCT {_bq(p['ref_column'])} AS _rk FROM {ref}) {alias}\n"
                         f"    ON c.{_bq(r['column_name'])} = {alias}._rk")
            cond = f"(c.{_bq(r['column_name'])} IS NULL OR {alias}._rk IS NOT NULL)"
        elif t == "unique":
            ucols = ", ".join(f"c.{_bq(x.strip())}" for x in r["column_name"].split(","))
            cond = f"(COUNT(1) OVER (PARTITION BY {ucols}) = 1)"
        elif t in DIRECT_RULE_TYPES:
            cond = rule_condition(r)
        else:
            raise ValueError(f"Unknown check_type {t} in rule {r['rule_id']}")
        flag_exprs.append(f"{cond} AS `_dq_ok_{rid}`")
        if r["severity"] in ("quarantine", "fail"):
            if t in DIRECT_RULE_TYPES and not (_rule_columns(r) & set(pii_cols)):
                direct.append(r)
            else:
                flagged.append(r)
    ctes.append("flags AS (\n  SELECT c.*" + "".join(f",\n         {f}" for f in flag_exprs)
                + "\n  FROM cur c" + ("\n" + "\n".join(joins) if joins else "") + "\n)")

    def rule_array(rs):
        if not rs:
            return "CAST(array() AS ARRAY<STRING>)"
        items = ", ".join(f"CASE WHEN NOT `_dq_ok_{_rid(r)}` THEN {_sql_str(r['rule_id'])} END" for r in rs)
        return f"filter(array({items}), x -> x IS NOT NULL)"
    warn_rules = [r for r in rules if r["severity"] == "warn"]
    stop_rules = [r for r in rules if r["severity"] in ("quarantine", "fail")]
    ctes.append(f"chk AS (\n  SELECT *,\n         {rule_array(warn_rules)} AS `_dq_warnings`,\n"
                f"         {rule_array(stop_rules)} AS `_dq_failed`\n  FROM flags\n)")

    # 3. final projection with PII treatment
    out_cols = []
    for c in targets:
        if c in pii_cols:
            e = _pii_expr(pii_cols[c], c)
            if e is not None:
                out_cols.append(f"{e} AS {_bq(c)}")
        else:
            out_cols.append(_bq(c))
    out_cols += [_bq(a) for a in audit]
    out_cols.append("`_dq_warnings`")
    out_cols += [f"`_dq_ok_{_rid(r)}`" for r in flagged]
    out_cols.append(lifecycle)
    if scd2:
        out_cols.append("`_record_hash`")
    salt_join = ""
    if any((m.get("pii_treatment") == "hash") for m in pii_cols.values()):
        salt_join = (f"\nCROSS JOIN (SELECT coalesce(max(`value`), {_sql_str(MLV_PII_SALT_DEFAULT)}) AS _pii_salt\n"
                     f"            FROM {MLV_SCHEMAS['meta']}.setting WHERE `key` = 'pii_salt') _salt")
        notes.append("hash salt read from meta.setting (never written into the view definition)")
    dropped = [c for c, m in pii_cols.items() if m.get("pii_treatment") == "drop"]
    if dropped:
        notes.append("PII dropped before Silver: " + ", ".join(dropped))

    constraints = []
    for r in direct + flagged:
        cond = rule_condition(r) if r in direct else f"`_dq_ok_{_rid(r)}`"
        thr = float(r.get("fail_threshold_pct") or 0)
        action = "FAIL" if (r["severity"] == "fail" and thr == 0) else "DROP"
        if r["severity"] == "fail" and thr > 0:
            notes.append(f"{r['rule_id']}: fail threshold {thr}% is not expressible in an MLV -> DROP + quarantine")
        constraints.append(f"  CONSTRAINT {_rid(r)} CHECK ({cond}) ON MISMATCH {action}")

    with_clause = "WITH " + ",\n".join(ctes)
    body = (f"{with_clause}\nSELECT\n  " + ",\n  ".join(out_cols) + f"\nFROM chk{salt_join}")
    name = f"{MLV_SCHEMAS['silver']}.{ent}"
    header = (f"-- {obj['object_id']}: {bronze} -> {name}\n-- " + "\n-- ".join(notes or ["SCD1 current state"]) + "\n")
    cons = ("(\n" + ",\n".join(constraints) + "\n)\n") if constraints else ""
    silver = (f"{header}CREATE OR REPLACE MATERIALIZED LAKE VIEW {name}\n{cons}"
              f"COMMENT {_comment(obj.get('description'))}\n"
              f"TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.object_id' = '{obj['object_id']}')\n"
              f"AS\n{body}")

    quarantine = None
    if stop_rules:
        qname = f"{MLV_SCHEMAS['quarantine']}.{ent}"
        qcols = [x for x in out_cols if not x.startswith("`_dq_ok_")] + ["`_dq_failed` AS `_failed_rules`"]
        quarantine = (f"-- rows removed from {name} by its DROP constraints (PII already treated)\n"
                      f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {qname}\n"
                      f"COMMENT \"Quarantine for {obj['object_id']} - rows failing quarantine/fail rules\"\n"
                      f"AS\n{with_clause}\nSELECT\n  " + ",\n  ".join(qcols)
                      + f"\nFROM chk{salt_join}\nWHERE size(`_dq_failed`) > 0")
    return {"silver": silver, "quarantine": quarantine, "deps": sorted(set(deps)), "notes": notes,
            "warnings": warnings, "direct": [r["rule_id"] for r in direct], "flagged": [r["rule_id"] for r in flagged]}


def gold_sql(t, silver_tables, gold_tables):
    refs = set(re.findall(r"\b(silver|gold)\.(\w+)", t["sql_text"]))
    if not refs:
        return None, [], "no source table (keep as a notebook-built table)"
    missing = [f"{s}.{n}" for s, n in refs if (s == "silver" and n not in silver_tables) or (s == "gold" and n not in gold_tables)]
    if missing:
        return None, [], "source not generated: " + ", ".join(sorted(missing))
    sql = re.sub(r"\bsilver\.(\w+)", lambda m: f"{MLV_SCHEMAS['silver']}.{m.group(1)}", t["sql_text"])
    sql = re.sub(r"\bgold\.(\w+)", lambda m: f"{MLV_SCHEMAS['gold']}.{m.group(1)}", sql)
    deps = sorted(f"{s}:{n}" for s, n in refs)
    name = f"{MLV_SCHEMAS['gold']}.{t['gold_table']}"
    ddl = (f"-- {t['transform_id']}: same SQL as meta.gold_transform, re-pointed at the MLV schemas\n"
           f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {name}\n"
           f"COMMENT {_comment(t.get('description'))}\n"
           f"TBLPROPERTIES (delta.enableChangeDataFeed = true, 'mdf.transform_id' = '{t['transform_id']}')\n"
           f"AS\n{sql}")
    return ddl, deps, None


def dq_summary_sql(objs_with_rules):
    """objs_with_rules: list of (object_id, entity, has_warn, has_stop)."""
    parts = []
    for oid, ent, has_warn, has_stop in objs_with_rules:
        if has_warn:
            parts.append(f"SELECT {_sql_str(oid)} AS object_id, {_sql_str(ent)} AS entity, 'WARN' AS outcome, w.rule_id, count(1) AS rows_affected\n"
                         f"FROM {MLV_SCHEMAS['silver']}.{ent} LATERAL VIEW explode(`_dq_warnings`) w AS rule_id\nGROUP BY w.rule_id")
        if has_stop:
            parts.append(f"SELECT {_sql_str(oid)} AS object_id, {_sql_str(ent)} AS entity, 'QUARANTINED' AS outcome, q.rule_id, count(1) AS rows_affected\n"
                         f"FROM {MLV_SCHEMAS['quarantine']}.{ent} LATERAL VIEW explode(`_failed_rules`) q AS rule_id\nGROUP BY q.rule_id")
    if not parts:
        return None
    return (f"-- one row per rule that flagged or removed rows - feeds the operations dashboard\n"
            f"CREATE OR REPLACE MATERIALIZED LAKE VIEW {MLV_SCHEMAS['quarantine']}.dq_rule_summary\n"
            f"COMMENT \"Warnings and quarantined rows per data-quality rule (MLV path)\"\nAS\n"
            + "\nUNION ALL\n".join(parts))


def file_bronze_sql(obj, workspace_id="<WORKSPACE_ID>", lakehouse_id="<LAKEHOUSE_ID>"):
    """Optional showcase: ingest the landing folder straight into a Bronze MLV (CSV / delimited text only)."""
    if obj.get("landing_format") not in ("csv", "txt"):
        return None
    ro = obj.get("read_options") or "{}"
    ro = json.loads(ro) if isinstance(ro, str) else dict(ro)
    opts = {"format": "csv",
            "path": f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{obj['landing_path'].strip('/')}/",
            "header": "true" if str(ro.get("header", True)).lower() in ("true", "1") else "false"}
    for k in ("delimiter", "sep", "quote", "escape", "encoding", "dateFormat", "timestampFormat"):
        if k in ro:
            opts["delimiter" if k == "sep" else k] = str(ro[k])
    opt_sql = ",\n  ".join(f"{_sql_str(k)} = {_sql_str(v)}" for k, v in opts.items())
    name = f"{MLV_SCHEMAS['bronze_files']}.{obj['bronze_table']}"
    return (f"-- {obj['object_id']}: landing folder -> {name} with no copy step (APPEND_ONLY picks up new files)\n"
            f"CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {name}\nUSING OneLake_Files\nOPTIONS (\n  {opt_sql}\n)\n"
            f"COMMENT {_comment('File-ingested Bronze for ' + obj['object_id'])}\n"
            f"TBLPROPERTIES ('schema_mode' = 'DYNAMIC', 'refresh_mode' = 'APPEND_ONLY')")


def build_mlv_plan(objects, column_map, dq_rules, gold_transforms, bronze_columns=None,
                   include_file_bronze=False, workspace_id="<WORKSPACE_ID>", lakehouse_id="<LAKEHOUSE_ID>"):
    """Returns an ordered list of dicts: {key, layer, name, ddl, deps, notes, warnings}. Upstream first."""
    objects = [o for o in objects if o.get("active", True)]
    if bronze_columns is not None:
        skipped = [o["object_id"] for o in objects if o["object_id"] not in bronze_columns]
        objects = [o for o in objects if o["object_id"] in bronze_columns]
    else:
        skipped = []
    silver_tables = {o["silver_table"] for o in objects}
    items, summary = {}, []
    for o in objects:
        cm = [m for m in column_map if m["object_id"] == o["object_id"]]
        cm = sorted(cm, key=lambda m: m.get("column_order") or 0) if any(m.get("column_order") for m in cm) else cm
        rs = sorted([r for r in dq_rules if r["object_id"] == o["object_id"] and r.get("active", True)],
                    key=lambda r: r["rule_id"])
        res = silver_sql(o, cm, rs, silver_tables, None if bronze_columns is None else bronze_columns[o["object_id"]])
        items[f"silver:{o['silver_table']}"] = {"layer": "silver", "name": f"{MLV_SCHEMAS['silver']}.{o['silver_table']}",
                                                "object_id": o["object_id"], "ddl": res["silver"], "deps": res["deps"],
                                                "notes": res["notes"], "warnings": res["warnings"],
                                                "direct_rules": res["direct"], "flag_rules": res["flagged"]}
        if res["quarantine"]:
            items[f"quarantine:{o['silver_table']}"] = {"layer": "quarantine", "name": f"{MLV_SCHEMAS['quarantine']}.{o['silver_table']}",
                                                        "object_id": o["object_id"], "ddl": res["quarantine"],
                                                        "deps": res["deps"], "notes": [], "warnings": []}
        summary.append((o["object_id"], o["silver_table"], any(r["severity"] == "warn" for r in rs),
                        bool(res["quarantine"])))
        if include_file_bronze:
            fb = file_bronze_sql(o, workspace_id, lakehouse_id)
            if fb:
                items[f"bronze_files:{o['bronze_table']}"] = {"layer": "bronze_files", "name": f"{MLV_SCHEMAS['bronze_files']}.{o['bronze_table']}",
                                                              "object_id": o["object_id"], "ddl": fb, "deps": [], "notes": [], "warnings": []}
    gold_tables = set()
    gold_skipped = []
    pending = sorted([g for g in gold_transforms if g.get("active", True)], key=lambda g: (g.get("run_order") or 0, g["transform_id"]))
    progress = True
    while pending and progress:
        progress = False
        for g in list(pending):
            ddl, deps, why = gold_sql(g, silver_tables, gold_tables | {x["gold_table"] for x in pending if x is not g})
            if ddl is None:
                gold_skipped.append((g["transform_id"], g["gold_table"], why))
                pending.remove(g)
                progress = True
                continue
            if all((d.split(":")[0] == "silver") or d.split(":")[1] in gold_tables for d in deps):
                items[f"gold:{g['gold_table']}"] = {"layer": "gold", "name": f"{MLV_SCHEMAS['gold']}.{g['gold_table']}",
                                                    "object_id": g["transform_id"], "ddl": ddl, "deps": deps,
                                                    "notes": [], "warnings": []}
                gold_tables.add(g["gold_table"])
                pending.remove(g)
                progress = True
    for g in pending:
        gold_skipped.append((g["transform_id"], g["gold_table"], "unresolved gold dependency"))
    s = dq_summary_sql(summary)
    if s:
        items["quarantine:dq_rule_summary"] = {"layer": "quarantine", "name": f"{MLV_SCHEMAS['quarantine']}.dq_rule_summary",
                                               "object_id": "DQ_SUMMARY", "ddl": s,
                                               "deps": [f"silver:{e}" for _, e, w, q in summary if w] + [f"quarantine:{e}" for _, e, w, q in summary if q],
                                               "notes": [], "warnings": []}
    # topological order (upstream first); layer order breaks ties so the listing reads Bronze -> Gold
    rank = {"bronze_files": 0, "silver": 1, "quarantine": 2, "gold": 3}
    ordered, done = [], set()
    def visit(k, stack=()):
        if k in done:
            return
        if k in stack:
            raise ValueError("dependency cycle: " + " -> ".join(stack + (k,)))
        for d in items[k]["deps"]:
            if d in items:
                visit(d, stack + (k,))
        done.add(k)
        ordered.append(dict(items[k], key=k))
    for k in sorted(items, key=lambda k: (rank[items[k]["layer"]], k)):
        visit(k)
    return {"items": ordered, "skipped_objects": skipped, "skipped_gold": gold_skipped}


def enable_cdf_sql(objects):
    return [f"ALTER TABLE {MLV_SCHEMAS['bronze']}.{o['bronze_table']} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            for o in objects if o.get("active", True)]
