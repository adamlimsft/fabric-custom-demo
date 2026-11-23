"""Builds the Real-Time Intelligence KQL for each industry pack: rti/<pack>.kql.

One file per pack, in run order:
  1. raw table, typed tables, parse functions and update policies (reading + alert event types)
  2. materialized views (latest reading per entity, 5-minute aggregates, alert counts)
  3. detection functions (threshold breach, frozen sensor, alert burst, anomaly, fault values, silence)
  4. Real-Time Dashboard tile queries
  5. Activator rule suggestions (comments)
It also refreshes the pack table in rti/README.md (between the <!-- packs --> markers).

The table definitions and the parse functions come from the same column list, so they can't drift apart.
Column names match the simulator (rti/rti_simulator.py) and the Gold layer, so streaming joins to lh_mdf.

Usage:  python tools/build_rti.py            (all packs)
        python tools/build_rti.py retail
Standard library only. build_kit.py calls build(p, kit_dir) for every pack.
"""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kitlib  # noqa: E402

DB = "eh_mdf_rt"
RAW = "RawEvents"
CONV = {"datetime": "todatetime", "int": "toint", "real": "toreal", "string": "tostring"}


def cap(s):
    return s[:1].upper() + s[1:]


def pascal(s):
    return "".join(w.capitalize() for w in s.split("_"))


def num(v):
    return repr(float(v))


def names(p):
    R, A, E = p["reading"], p["alert"], p["entity"]
    m = R["metrics"]
    rt, at = pascal(R["table"]), pascal(A["stream"])
    thr = float(m[0]["threshold"])
    gap = abs(thr - m[0]["base"])
    return dict(
        rt=rt, at=at, eid=E["id"], reg=p["region_col"], m1=m[0]["col"], m2=m[1]["col"], m3=m[2]["col"],
        thr=thr, lo=float(m[0]["min"]), hi=float(m[0]["max"]), sent=float(R["sentinel"]),
        watch=round(m[0]["base"] + 0.6 * (thr - m[0]["base"]), 2), gap=gap,
        latest=f"{rt}Latest", agg5=f"{rt}5m", alert5=f"{at}5m",
        f_breach=f"{rt}Breaches", f_frozen=f"{rt}Frozen", f_anom=f"{rt}Anomalies", f_fault=f"{rt}Faults",
        f_silent=f"{rt}Silent", f_ctx=f"{rt}BreachesWithContext", f_burst=f"{at}Bursts",
        dim=f"dim_{E['table']}")


def reading_cols(p):
    n = names(p)
    return [("event_ts", "datetime"), (n["eid"], "int"), (n["reg"], "string")] + \
           [(m["col"], "real") for m in p["reading"]["metrics"]]


def alert_cols(p):
    n = names(p)
    return [("event_ts", "datetime"), ("alert_id", "string"), (n["eid"], "int"), (n["reg"], "string"),
            ("alert_code", "string"), ("severity", "string"), ("message", "string")]


def _block(*lines):
    return "\n".join(lines)


def _comment(text):
    return "\n".join("// " + t if t else "//" for t in text.split("\n"))


def section1(p):
    n = names(p)
    out = [_comment("1. TABLES AND UPDATE POLICIES\n"
                    f"{RAW} is the streaming Bronze: every event as received, kept for 7 days (replayable from the stream).\n"
                    f"Update policies split it into typed tables at ingestion time - no pipeline, no notebook.")]
    out.append(f".create-merge table {RAW} (payload: dynamic)")
    out.append(f".create-or-alter table {RAW} ingestion json mapping '{RAW}Mapping' "
               "'[{\"column\":\"payload\",\"Properties\":{\"Path\":\"$\"}}]'")
    out.append(f".alter-merge table {RAW} policy retention softdelete = 7d recoverability = disabled")
    for t, cols in ((n["rt"], reading_cols(p)), (n["at"], alert_cols(p))):
        out.append(f".create-merge table {t} (" + ", ".join(f"{c}: {ty}" for c, ty in cols) + ", ingested_at: datetime)")
        out.append(f".alter table {t} policy caching hot = 31d")
        out.append(f".alter-merge table {t} policy retention softdelete = 365d")
    for t, etype, cols in ((n["rt"], p["reading"]["table"], reading_cols(p)), (n["at"], p["alert"]["stream"], alert_cols(p))):
        proj = ",\n        ".join(f"{c} = {CONV[ty]}(payload['{c}'])" for c, ty in cols)
        out.append(_block(
            f".create-or-alter function with (folder = 'parse', docstring = '{RAW} -> {t}') Parse{t}()",
            "{",
            f"    {RAW}",
            f"    | where tostring(payload['event_type']) == '{etype}'",
            f"    | project {proj},",
            "        ingested_at = now()",
            "}"))
        policy = ('[{"IsEnabled": true, "Source": "' + RAW + '", "Query": "Parse' + t +
                  '()", "IsTransactional": false, "PropagateIngestionProperties": false}]')
        out.append(f".alter table {t} policy update @'{policy}'")
    return out


def section2(p):
    n = names(p)
    rt, at, eid, reg, m1, m2, m3 = n["rt"], n["at"], n["eid"], n["reg"], n["m1"], n["m2"], n["m3"]
    valid = f"{m1} between ({num(n['lo'])} .. {num(n['hi'])})"
    return [
        _comment("2. MATERIALIZED VIEWS\n"
                 "Kept current by the engine. Backfill needs the async form; ifnotexists makes re-runs safe.\n"
                 f"{n['latest']}: the newest reading per {p['entity']['label']} - the 'where is everything now' table.\n"
                 f"{n['agg5']}: 5-minute aggregates (fault values excluded). {n['alert5']}: alert counts per 5 minutes."),
        _block(f".create async ifnotexists materialized-view with (backfill = true, folder = 'views') {n['latest']} on table {rt}",
               "{",
               f"    {rt}",
               f"    | summarize arg_max(event_ts, {reg}, {m1}, {m2}, {m3}, ingested_at) by {eid}",
               "}"),
        _block(f".create async ifnotexists materialized-view with (backfill = true, folder = 'views') {n['agg5']} on table {rt}",
               "{",
               f"    {rt}",
               f"    | where {valid}",
               f"    | summarize readings = count(), avg_{m1} = avg({m1}), max_{m1} = max({m1}), min_{m1} = min({m1}),",
               f"        avg_{m2} = avg({m2}), avg_{m3} = avg({m3}), breach_readings = countif({m1} > {num(n['thr'])})",
               f"        by {eid}, {reg}, window_start = bin(event_ts, 5m)",
               "}"),
        _block(f".create async ifnotexists materialized-view with (backfill = true, folder = 'views') {n['alert5']} on table {at}",
               "{",
               f"    {at}",
               "    | summarize alerts = count(), critical = countif(severity == 'CRITICAL')",
               f"        by {reg}, alert_code, window_start = bin(event_ts, 5m)",
               "}"),
    ]


def section3(p):
    n = names(p)
    rt, at, eid, reg, m1 = n["rt"], n["at"], n["eid"], n["reg"], n["m1"]
    E, m = p["entity"], p["reading"]["metrics"][0]
    valid = f"{m1} between ({num(n['lo'])} .. {num(n['hi'])})"
    end_r = f"    let _end = toscalar({rt} | summarize max(event_ts));"
    out = [_comment("3. DETECTION FUNCTIONS\n"
                    "Every function anchors on the newest event, not now(), so it works on a live stream and on an uploaded\n"
                    "sample file. For live operations call them with a short lookback, e.g. "
                    f"{n['f_breach']}(30m).\n"
                    f"Fault values ({p['reading']['sentinel']}) and anything outside {m['min']}..{m['max']} {m['unit']} "
                    "are excluded from the breach, frozen and anomaly checks.")]
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = '{cap(m['label'])} above the threshold') "
        f"{n['f_breach']}(lookback: timespan = 6h, threshold: real = real({n['thr']}))",
        "{",
        end_r,
        f"    {rt}",
        "    | where event_ts > _end - lookback",
        f"    | where {valid} and {m1} > threshold",
        f"    | summarize first_seen = min(event_ts), last_seen = max(event_ts), breach_readings = count(),",
        f"        peak_{m1} = max({m1}) by {eid}, {reg}",
        f"    | order by peak_{m1} desc",
        "}"))
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = 'Stuck sensors: no variation in {m1} across a window') "
        f"{n['f_frozen']}(lookback: timespan = 6h, window: timespan = 30m)",
        "{",
        end_r,
        f"    {rt}",
        "    | where event_ts > _end - lookback",
        f"    | where {valid}",
        f"    | summarize readings = count(), sd = stdev({m1}), stuck_value = avg({m1})",
        f"        by {eid}, {reg}, window_start = bin(event_ts, window)",
        "    | where readings >= toint(window / 1m * 2 / 3) and sd < 0.001",
        "    | summarize first_window = min(window_start), last_window = max(window_start), frozen_windows = count(),",
        f"        stuck_value = take_any(stuck_value) by {eid}, {reg}",
        "}"))
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = 'Bursts of {p['alert']['label']}s in one "
        f"{p['region_label']}') {n['f_burst']}(lookback: timespan = 6h, window: timespan = 10m, min_alerts: int = 5)",
        "{",
        f"    let _end = toscalar({at} | summarize max(event_ts));",
        f"    {at}",
        "    | where event_ts > _end - lookback",
        f"    | summarize alerts = count(), {E['table']}s = dcount({eid}), codes = make_set(alert_code)",
        f"        by {reg}, window_start = bin(event_ts, window)",
        "    | where alerts >= min_alerts",
        "    | order by window_start desc",
        "}"))
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = 'Spikes in {m1} against each {E['label']} baseline') "
        f"{n['f_anom']}(lookback: timespan = 6h, sensitivity: real = 3.0)",
        "{",
        end_r,
        "    let _start = _end - lookback;",
        f"    {rt}",
        "    | where event_ts between (_start .. _end)",
        f"    | where {valid}",
        f"    | make-series v = avg({m1}) default = real(null) on event_ts from bin(_start, 1m) to _end + 1m step 1m by {eid}",
        "    | extend v = series_fill_linear(v)",
        "    | extend (flag, score, baseline) = series_decompose_anomalies(v, sensitivity, -1, 'linefit')",
        "    | mv-expand event_ts to typeof(datetime), v to typeof(real), flag to typeof(int), score to typeof(real),",
        "        baseline to typeof(real)",
        "    | where flag == 1",
        f"    | project {eid}, event_ts, {m1} = round(v, 2), baseline = round(baseline, 2), score = round(score, 2)",
        "}"))
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = 'Fault values and out-of-range readings') "
        f"{n['f_fault']}(lookback: timespan = 6h)",
        "{",
        end_r,
        f"    {rt}",
        "    | where event_ts > _end - lookback",
        f"    | where isnull({m1}) or {m1} == real({n['sent']}) or not({valid})",
        f"    | summarize fault_readings = count(), first_seen = min(event_ts), last_seen = max(event_ts),",
        f"        fault_values = make_set({m1}) by {eid}, {reg}",
        "}"))
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = 'No reading for longer than the gap') "
        f"{n['f_silent']}(gap: timespan = 10m)",
        "{",
        end_r,
        f"    {n['latest']}",
        "    | where event_ts < _end - gap",
        f"    | project {eid}, {reg}, last_reading = event_ts, silent_for = _end - event_ts",
        "}"))
    out.append(_comment(
        f"Optional: join to Gold. In the KQL database add a OneLake shortcut to lh_mdf > Tables > gold > {n['dim']}\n"
        f"(keep the name). skipvalidation lets you create the function before the shortcut exists."))
    out.append(_block(
        f".create-or-alter function with (folder = 'detect', docstring = 'Breaches with {E['label']} details from Gold', "
        f"skipvalidation = 'true') {n['f_ctx']}(lookback: timespan = 6h)",
        "{",
        f"    {n['f_breach']}(lookback)",
        f"    | lookup kind = leftouter (external_table('{n['dim']}')",
        f"        | project {eid} = toint({eid}), {E['code']}, {E['name_col']}, {E['type_col']}, {E['tier_col']}) on {eid}",
        "}"))
    return out


def section4(p):
    n = names(p)
    rt, at, eid, reg, m1, m2 = n["rt"], n["at"], n["eid"], n["reg"], n["m1"], n["m2"]
    m = p["reading"]["metrics"]
    E, A = p["entity"], p["alert"]
    valid = f"{m1} between ({num(n['lo'])} .. {num(n['hi'])})"
    endv = f"let _end = toscalar({rt} | summarize max(event_ts));"
    tiles = [
        (f"{cap(E['plural'])} now by status (table)",
         [f"{n['latest']}",
          f"| extend status_now = case(not({valid}), 'FAULT', {m1} > {num(n['thr'])}, 'BREACH', {m1} > {num(n['watch'])}, 'WATCH', 'NORMAL')",
          f"| summarize {E['table']}s = count() by status_now, {reg}",
          "| order by status_now asc"]),
        (f"{cap(m[0]['label'])}, top 5 {E['plural']} (line chart)",
         [endv,
          f"let top5 = {n['agg5']} | where window_start > _end - 6h | summarize peak = max(max_{m1}) by {eid} | top 5 by peak | project {eid};",
          f"{n['agg5']}",
          f"| where window_start > _end - 6h and {eid} in (top5)",
          f"| project window_start, {eid} = tostring({eid}), max_{m1}"]),
        (f"Threshold breaches - {m[0]['label']} above {m[0]['threshold']} {m[0]['unit']} (table)", [f"{n['f_breach']}()"]),
        ("Anomalies against baseline (table)",
         [f"{n['f_anom']}()",
          f"| summarize first_seen = min(event_ts), last_seen = max(event_ts), peak = max({m1}), points = count() by {eid}"]),
        ("Frozen sensors (table)", [f"{n['f_frozen']}()"]),
        ("Fault values (table)", [f"{n['f_fault']}()"]),
        (f"{cap(A['label'])}s by {p['region_label']}, 10-minute bins (column chart)",
         [f"let _end = toscalar({at} | summarize max(event_ts));",
          f"{n['alert5']}",
          "| where window_start > _end - 6h",
          f"| summarize alerts = sum(alerts) by {reg}, window_start = bin(window_start, 10m)"]),
        (f"Alert bursts - {A['storm']} (table)", [f"{n['f_burst']}()"]),
        (f"Average {m[1]['label']} by {p['region_label']}, 5 minutes (line chart)",
         [endv, f"{n['agg5']}", "| where window_start > _end - 6h",
          f"| summarize avg_{m2} = round(avg(avg_{m2}), 2) by {reg}, window_start"]),
        ("Ingestion latency p95, seconds (stat)",
         [f"{rt}", "| where ingested_at > ago(15m)",
          "| summarize p95_latency_s = percentile((ingested_at - event_ts) / 1s, 95)"]),
    ]
    out = [_comment("4. REAL-TIME DASHBOARD TILES\n"
                    "One query per tile; the suggested visual is in brackets. Auto-refresh 30 s (or Continuous).\n"
                    "Tiles anchor on the newest event so they also work on an uploaded sample file.\n"
                    "The latency tile only shows data while events are streaming.")]
    for title, q in tiles:
        out.append(_block(f"// ---- Tile: {title}", *q))
    return out


def section5(p):
    n = names(p)
    E, A, m = p["entity"], p["alert"], p["reading"]["metrics"][0]
    owner = p["reading"]["owner"]
    rules = [
        (f"{cap(m['label'])} breach",
         f"{n['rt']} (Eventstream route to Activator, or the {n['f_breach']} dashboard tile)",
         f"{n['m1']} > {m['threshold']} for 3 consecutive readings per {n['eid']}",
         f"Teams message to {owner}; optional Power Automate flow to log a follow-up"),
        (f"Critical {A['label']}", f"{n['at']}",
         f"severity == 'CRITICAL' (codes: {', '.join(A['codes'])})",
         f"Email and Teams to the on-call lead for the {p['region_label']}"),
        (f"{cap(A['storm'])}", f"{n['f_burst']}() dashboard tile (Set alert)",
         f"alerts >= 5 in 10 minutes for one {n['reg']}", f"Teams post to the {owner} channel"),
        ("Stuck or faulty sensor", f"{n['f_frozen']}() and {n['f_fault']}() tiles",
         "any row returned", "Teams message to the telemetry support team"),
        ("Telemetry stopped", f"{n['latest']} (or {n['f_silent']}())",
         f"no new reading for 10 minutes per {n['eid']}", "Teams message to the telemetry support team"),
    ]
    lines = ["5. ACTIVATOR RULE SUGGESTIONS (create in Activator - not KQL commands)",
             "Create them from a dashboard tile (Set alert) or from an Eventstream destination.", ""]
    for name, src, cond, act in rules:
        lines += [f"Rule: {name}", f"  Monitor:   {src}", f"  Condition: {cond}", f"  Action:    {act}", ""]
    return [_comment("\n".join(lines).rstrip())]


def kql(p):
    n = names(p)
    m = p["reading"]["metrics"][0]
    head = _comment(
        f"{p['org']} ({p['industry']}) - Real-Time Intelligence, pack '{p['id']}'\n"
        f"Eventhouse eh_mdf_rt, KQL database {DB}. Generated by tools/build_rti.py - edit the builder, not this file.\n"
        f"Streams: {p['reading']['table']} -> {n['rt']}, {p['alert']['stream']} -> {n['at']} (both land in {RAW} first).\n"
        f"Primary metric: {m['col']} ({m['label']}, {m['unit']}), threshold {m['threshold']}, fault value {p['reading']['sentinel']}.\n"
        "Run the sections in order. Blocks are separated by blank lines: put the cursor in a block and Run\n"
        "(Shift+Enter). Every command is safe to re-run.")
    blocks = [head] + section1(p) + section2(p) + section3(p) + section4(p) + section5(p)
    return "\n\n".join(blocks) + "\n"


# ------------------------------------------------------------------------------------------ README pack table
def pack_table(packs):
    sys.path.insert(0, os.path.join(kitlib.KIT, "rti"))
    sys.dont_write_bytecode = True          # keep rti/ free of __pycache__
    import rti_simulator as sim
    L = ["| Pack | Organisation | Reading stream -> table | Alert stream -> table | Primary metric (threshold) "
         "| Planted incidents (minute from start) |",
         "|---|---|---|---|---|---|"]
    for p in packs:
        n = names(p)
        m = p["reading"]["metrics"][0]
        plan = {i["what"]: i for i in sim.incident_plan(p)}
        inc = (f"{plan['SENTINEL']['at'][0]} fault value {p['reading']['sentinel']}; "
               f"{plan['RAMP']['at']} {m['label']} ramp + {p['alert']['codes'][0]}; "
               f"{plan['FROZEN']['at']} frozen sensor; {plan['BURST']['at']} {p['alert']['storm']}")
        L.append(f"| `{p['id']}` | {p['org']} | `{p['reading']['table']}` -> `{n['rt']}` | "
                 f"`{p['alert']['stream']}` -> `{n['at']}` | `{m['col']}` > {m['threshold']} {m['unit']} | {inc} |")
    return "\n".join(L)


def update_readme(kit_dir):
    path = os.path.join(kit_dir, "rti", "README.md")
    if not os.path.exists(path):
        return
    text = open(path, encoding="utf-8").read()
    a, b = "<!-- packs:start -->", "<!-- packs:end -->"
    if a not in text or b not in text:
        return
    new = text.split(a)[0] + a + "\n" + pack_table(kitlib.all_packs()) + "\n" + b + text.split(b, 1)[1]
    if new != text:
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(new)


def build(p, kit_dir=kitlib.KIT):
    path = os.path.join(kit_dir, "rti", f"{p['id']}.kql")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(kql(p))
    update_readme(kit_dir)
    return path


if __name__ == "__main__":
    ids = sys.argv[1:]
    for pk in kitlib.all_packs():
        if not ids or pk["id"] in ids:
            print("wrote", build(pk))
