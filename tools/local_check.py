"""Offline smoke test for a pack - no Fabric, no Spark. Standard library only.

Generates the pack's data, replays Landing -> Bronze -> Silver for days 1..3 with a small Python emulation of the
framework (column map, DQ rules, quarantine, late-arriving retry, SCD1/SCD2/insert-only, soft delete), then runs every
Gold SQL statement (and the onboarding Gold) in SQLite with a few Spark-function shims. It proves the metadata, the
generated files and the Gold SQL agree with each other before you take a new pack to Fabric.

Usage:  python tools/local_check.py            (all packs)
        python tools/local_check.py retail
"""
import csv, json, math, os, re, sqlite3, sys, tempfile
from datetime import datetime, date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kitlib, generate_data  # noqa: E402

TODAY = "2026-11-16"


def parse_ts(v, fmt=None):
    if v in (None, ""):
        return None
    s = str(v)
    fmts = [fmt] if fmt else ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            pass
    return None


JFMT = {"dd/MM/yyyy HH:mm": "%d/%m/%Y %H:%M", "dd/MM/yyyy": "%d/%m/%Y"}


def cast(v, typ):
    if v in (None, ""):
        return None
    try:
        if typ == "int":
            f = float(v)
            return int(f) if f == int(f) else None
        if typ == "double" or typ.startswith("decimal"):
            return float(v)
        if typ == "timestamp":
            t = parse_ts(v)
            return t.strftime("%Y-%m-%d %H:%M:%S") if t else None
        if typ == "date":
            t = parse_ts(v)
            return t.date().isoformat() if t else None
        return str(v)
    except (ValueError, TypeError):
        return None


def apply_expr(expr, row):
    m = re.match(r"to_(timestamp|date)\((\w+), '([^']+)'\)$", expr)
    if not m:
        raise ValueError(f"local_check can't emulate transform_expr: {expr}")
    kind, col, fmt = m.groups()
    t = parse_ts(row.get(col), JFMT[fmt])
    if t is None:
        return None
    return t.strftime("%Y-%m-%d %H:%M:%S") if kind == "timestamp" else t.date().isoformat()


def eval_expr(expr, r):
    """Tiny evaluator for the expression rules the packs use."""
    m = re.match(r"(\w+) IS NULL OR (\w+) >= (\w+)$", expr)
    if m:
        a, b, c = m.groups()
        return r.get(a) is None or (r.get(b) is not None and r.get(c) is not None and r[b] >= r[c])
    m = re.match(r"(\w+) <> '([^']+)' OR (\w+) = (\d+)$", expr)
    if m:
        a, v, b, n = m.groups()
        return r.get(a) != v or r.get(b) == int(n)
    raise ValueError(f"can't evaluate {expr}")


def check(rule, row, silver):
    p = json.loads(rule["params"] or "{}")
    c, t = rule["column_name"], rule["check_type"]
    v = row.get(c) if c else None
    if t == "not_null":
        return v is not None
    if t == "regex":
        return v is None or re.search(p["pattern"], str(v)) is not None
    if t == "range":
        return v is None or ((p.get("min") is None or v >= p["min"]) and (p.get("max") is None or v <= p["max"]))
    if t == "allowed_values":
        return v is None or v in p["values"]
    if t == "expression":
        return eval_expr(p["expr"], row)
    if t == "referential":
        tbl = p["ref_table"].split(".")[1]
        keys = {r[p["ref_column"]] for r in silver.get(tbl, [])}
        return v is None or v in keys
    raise ValueError(t)


def read_landing(obj, folder):
    fmt = obj["landing_format"]
    opts = json.loads(obj["read_options"] or "{}")
    rows = []
    if not os.path.isdir(folder):
        return rows
    for f in sorted(os.listdir(folder)):
        path = os.path.join(folder, f)
        if fmt in ("csv", "txt"):
            with open(path, encoding="utf-8") as fh:
                rows += list(csv.DictReader(fh, delimiter=opts.get("delimiter", ",")))
        elif fmt == "json":
            doc = json.load(open(path, encoding="utf-8"))
            rows += doc[opts["json_root"]] if opts.get("json_root") else doc
    return rows


def run_pack(p, verbose=True):
    doc = kitlib.control_rows(p)
    tmp = tempfile.mkdtemp(prefix=f"chk_{p['id']}_")
    data = generate_data.build(p, tmp)
    objs = sorted(doc["ingestion_object"], key=lambda o: o["priority"])
    onboard = doc["onboard"]
    xo = onboard["object"]
    xcmap = [dict(object_id=xo["object_id"], source_column=s, target_column=t, data_type=d, transform_expr=e, pii_treatment="none")
             for s, t, d, e in onboard["columns"]]
    xrules = [dict(rule_id=r[0], rule_name=r[1], rule_category=r[2], check_type=r[3], column_name=r[4], params=r[5], severity=r[6],
                   fail_threshold_pct=None) for r in onboard["rules"]]
    objs.append(xo)
    cmaps = {o["object_id"]: [m for m in doc["column_map"] if m["object_id"] == o["object_id"]] for o in objs}
    cmaps[xo["object_id"]] = xcmap
    rules = {o["object_id"]: [r for r in doc["dq_rule"] if r["object_id"] == o["object_id"]] for o in objs}
    rules[xo["object_id"]] = xrules
    silver, quarantine, stats = {}, {}, []
    problems = []
    for day, d in enumerate(data["dates"], start=1):
        for o in objs:
            oid, tbl = o["object_id"], o["silver_table"]
            keys = [k.strip() for k in o["primary_keys"].split(",")]
            bronze = read_landing(o, os.path.join(tmp, o["landing_path"], f"load_date={d}"))
            if not bronze:
                problems.append(f"day {day}: no landing rows for {oid}")
                continue
            srccols = set(bronze[0].keys())
            for m in cmaps[oid]:
                if not m["transform_expr"] and m["source_column"] not in srccols:
                    problems.append(f"{oid}: mapped source column {m['source_column']} missing in landed file")
            unmapped = srccols - {m["source_column"] for m in cmaps[oid]} - {"attributes"}
            if unmapped and not (day >= 3 and unmapped == {p["entity"]["drift_col"]}):
                problems.append(f"day {day} {oid}: unmapped columns {sorted(unmapped)}")
            # retry late arrivals first
            resolved = 0
            ref_rules = [r for r in rules[oid] if r["check_type"] == "referential" and r["severity"] == "quarantine"]
            q = quarantine.setdefault(tbl, [])
            for qr in q:
                if qr["_status"] == "OPEN" and qr["_retry"] and all(check(r, qr, silver) for r in ref_rules):
                    qr["_status"] = "RESOLVED"
                    resolved += 1
                    merge(silver, tbl, keys, [qr], o, full=False)
            mapped = []
            for b in bronze:
                row = {}
                for m in cmaps[oid]:
                    if m["transform_expr"]:
                        v = apply_expr(m["transform_expr"], b)
                        v = cast(v, m["data_type"])
                    elif m["pii_treatment"] == "year_only":
                        t = parse_ts(b.get(m["source_column"]))
                        v = t.year if t else None
                    else:
                        v = cast(b.get(m["source_column"]), m["data_type"])
                    row[m["target_column"]] = v
                row["_lm"] = row.get("last_modified") or row.get("last_modified_ts") or ""
                mapped.append(row)
            # dedupe newest wins
            dd = {}
            for row in mapped:
                k = tuple(row[x] for x in keys)
                if k not in dd or row["_lm"] >= dd[k]["_lm"]:
                    dd[k] = row
            dups = len(mapped) - len(dd)
            rows = list(dd.values())
            n = len(rows)
            outcome = {}
            good, held = [], []
            for row in rows:
                failed_q, ok_nonref = [], True
                for r in rules[oid]:
                    ok = check(r, row, silver)
                    if not ok:
                        outcome[r["rule_id"]] = outcome.get(r["rule_id"], 0) + 1
                        if r["severity"] == "quarantine":
                            failed_q.append(r["rule_id"])
                            if r["check_type"] != "referential":
                                ok_nonref = False
                if failed_q:
                    row.update(_status="OPEN", _retry=ok_nonref, _rules=failed_q)
                    held.append(row)
                else:
                    good.append(row)
            for r in rules[oid]:
                if r["severity"] == "fail" and outcome.get(r["rule_id"], 0) * 100.0 / max(n, 1) > float(r["fail_threshold_pct"] or 0):
                    problems.append(f"day {day} {oid}: fail rule {r['rule_id']} would stop the load ({outcome[r['rule_id']]} rows)")
            q += held
            for m in cmaps[oid]:
                if m["pii_treatment"] == "drop":
                    for row in good:
                        row.pop(m["target_column"], None)
            merge(silver, tbl, keys, good, o, full=(o["load_type"] == "full"), snapshot=rows)
            stats.append((day, oid, len(bronze), dups, len(good), len(held), resolved, outcome))
    # ---------------------------------------------------------------- gold in sqlite
    con = sqlite3.connect(":memory:")
    shims(con)
    for tbl, rows in silver.items():
        cols = sorted({c for r in rows for c in r if not c.startswith("_") or c in ("_is_current", "_is_deleted", "_valid_from")})
        con.execute(f"CREATE TABLE silver_{tbl} ({', '.join(cols)})")
        con.executemany(f"INSERT INTO silver_{tbl} VALUES ({', '.join('?' * len(cols))})",
                        [[r.get(c) for c in cols] for r in rows])
    gold_out = {}
    for g in doc["gold_transform"] + [onboard["gold"]]:
        if g["gold_table"] == "dim_date":
            continue
        sql = to_sqlite(g["sql_text"])
        try:
            cur = con.execute(sql)
            names = [c[0] for c in cur.description]
            res = cur.fetchall()
            gold_out[g["gold_table"]] = (names, res)
            if not res:
                problems.append(f"gold {g['gold_table']}: 0 rows")
        except Exception as e:
            problems.append(f"gold {g['gold_table']}: {e}")
    if verbose:
        for s in stats:
            print(f"  day {s[0]} {s[1]:34s} landed={s[2]:5d} dups={s[3]:3d} silver_in={s[4]:5d} quarantined={s[5]:3d} "
                  f"resolved={s[6]:2d} {s[7]}")
        for t, (names, res) in gold_out.items():
            print(f"  gold {t:40s} rows={len(res)}")
        sc = gold_out.get(kitlib.N(p)["g_score"])
        if sc:
            i = sc[0].index("risk_band")
            bands = {}
            for row in sc[1]:
                bands[row[i]] = bands.get(row[i], 0) + 1
            print("  risk bands", bands)
    return problems, gold_out, silver


def merge(silver, tbl, keys, rows, o, full=False, snapshot=None):
    tab = silver.setdefault(tbl, [])
    mode = o["silver_write_mode"]
    idx = {}
    for i, r in enumerate(tab):
        if mode != "merge_scd2" or r.get("_is_current"):
            idx[tuple(r[k] for k in keys)] = i
    for r in rows:
        r = {k: v for k, v in r.items() if k not in ("_status", "_retry", "_rules", "_lm")}
        k = tuple(r[x] for x in keys)
        if mode == "insert_only":
            if k not in idx:
                tab.append(r); idx[k] = len(tab) - 1
        elif mode == "merge_scd1":
            r["_is_deleted"] = False
            if k in idx:
                tab[idx[k]] = r
            else:
                tab.append(r); idx[k] = len(tab) - 1
        else:
            r.update(_is_current=True, _valid_from=r.get("last_modified"))
            if k in idx:
                old = tab[idx[k]]
                if {c: v for c, v in old.items() if not c.startswith("_")} == {c: v for c, v in r.items() if not c.startswith("_")}:
                    continue
                old["_is_current"] = False
            tab.append(r); idx[k] = len(tab) - 1
    if full and snapshot is not None:
        present = {tuple(r[x] for x in keys) for r in snapshot}
        for r in tab:
            if tuple(r[x] for x in keys) not in present:
                r["_is_deleted"] = True


def to_sqlite(sql):
    s = sql.replace("silver.", "silver_").replace("current_date()", f"'{TODAY}'")
    s = re.sub(r"\bAS STRING\b", "AS TEXT", s)
    return s


def shims(con):
    def ts(v):
        if v is None:
            return None
        return parse_ts(v) or datetime.strptime(str(v)[:10], "%Y-%m-%d")
    J = {"yyyyMMdd": "%Y%m%d", "yyyy-MM": "%Y-%m", "MMM": "%b", "EEE": "%a"}
    con.create_function("to_date", 1, lambda v: ts(v).date().isoformat() if v else None)
    con.create_function("date_format", 2, lambda v, f: ts(v).strftime(J[f]) if v else None)
    con.create_function("unix_timestamp", 1, lambda v: ts(v).timestamp() if v else None)
    con.create_function("date_trunc", 2, lambda u, v: ts(v).strftime("%Y-%m-%d %H:00:00") if v else None)
    con.create_function("year", 1, lambda v: (v if isinstance(v, int) else ts(v).year) if v else None)
    con.create_function("floor", 1, lambda v: math.floor(v) if v is not None else None)
    con.create_function("months_between", 2, lambda a, b: ((ts(a) - ts(b)).days / 30.4375) if a and b else None)

    def add_months(d, n):
        t = ts(d)
        mth = t.month - 1 + int(n)
        y, mo = t.year + mth // 12, mth % 12 + 1
        return date(y, mo, min(t.day, 28)).isoformat()
    con.create_function("add_months", 2, add_months)
    con.create_function("LEAST", -1, lambda *a: min(x for x in a if x is not None))


def main(ids):
    packs = [p for p in kitlib.all_packs() if not ids or p["id"] in ids]
    bad = 0
    for p in packs:
        print(f"=== {p['id']} - {p['org']}")
        probs, _, _ = run_pack(p)
        for x in probs:
            print("  PROBLEM:", x)
        bad += len(probs)
    print("ALL OK" if not bad else f"{bad} problem(s)")
    return bad


if __name__ == "__main__":
    sys.exit(1 if main(sys.argv[1:]) else 0)
