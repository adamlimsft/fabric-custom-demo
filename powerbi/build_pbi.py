"""Generates a Power BI project (PBIP: TMDL semantic model + PBIR report) for any industry pack.

  python powerbi/build_pbi.py --pack retail                    (Import from the lh_mdf SQL analytics endpoint)
  python powerbi/build_pbi.py --pack energy --out C:/pbi/en    (choose the output folder)
  python powerbi/build_pbi.py --pack telco --offline           (also writes Gold CSVs and points the model at them)

Model: dim_date, dim_<entity>, fact_<event>, fact_<reading>_hourly and <entity>_health_scorecard from the gold
schema of lh_mdf, ten measures named from the pack, a row-level security role for one region and three report
pages. Standard library only; --offline also runs tools/local_check.py (stdlib too) to build the Gold tables.
The output is generated, not shipped: regenerate it whenever the pack changes.
"""
import argparse, csv, hashlib, json, os, shutil, sys, uuid
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
KIT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(KIT, "tools"))
import kitlib  # noqa: E402

NAVY, TEAL, SAND, RED, INK, GREY, BG = "#0B2545", "#13807A", "#E9C46A", "#C8553D", "#1F2933", "#5F6B7A", "#F4F6F8"
VC = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.4.0/schema.json"
PG = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json"
KM = "Key Measures"

# logical type -> (tmdl dataType, M type, default formatString). TMDL has no date type: dates are dateTime.
TYPES = {"int64": ("int64", "Int64.Type", None), "double": ("double", "type number", "#,0.00"),
         "string": ("string", "type text", None), "date": ("dateTime", "type date", "dd/mm/yyyy"),
         "dateTime": ("dateTime", "type datetime", "dd/mm/yyyy hh:nn"), "boolean": ("boolean", "type logical", None)}


def gid(s):
    """Deterministic GUID so rebuilds diff cleanly in Git."""
    return str(uuid.UUID(hashlib.md5(s.encode()).hexdigest()))


def q(n):
    """TMDL object name quoting."""
    return n if n.replace("_", "").isalnum() else "'" + n.replace("'", "''") + "'"


def title(s):
    return " ".join(w[:1].upper() + w[1:] for w in s.replace("_", " ").split())


def stem(verb):
    """completed -> complete, finished -> finish, fulfilled -> fulfil."""
    if not verb.endswith("ed"):
        return verb
    s = verb[:-2]
    if s.endswith("ll"):
        return s[:-1]
    if s.endswith(("sh", "ch")):
        return s
    return verb[:-1]


# ------------------------------------------------------------------------------------------ model spec
def model_spec(p):
    """Tables, columns (name, tmdl type, summarizeBy), measures, relationships and RLS - all from the pack."""
    n, E, V, R = kitlib.N(p), p["entity"], p["event"], p["reading"]
    reg = n["reg"]
    m = R["metrics"]
    m1 = m[0]["col"]
    vt = V["table"]
    s, i, d, dt, dd, b = "string", "int64", "double", "dateTime", "date", "boolean"
    tables = {
        n["g_date"]: [("date_key", i), ("calendar_date", dd), ("calendar_year", i), ("calendar_quarter", i),
                      ("month_number", i), ("month_name", s), ("year_month", s), ("day_of_month", i), ("day_name", s),
                      ("fy_year", i), ("fy_label", s), ("is_weekend", b)],
        n["g_dim"]: [(E["id"], i), (E["code"], s), (E["name_col"], s), (E["type_col"], s), (reg, s), ("status", s),
                     (E["tier_col"], i), (E["cap_col"], d), (E["start_col"], dd), ("age_years", i),
                     ("version_valid_from", dt)],
        n["g_fact"]: [(V["id"], i), (E["id"], i), (V["type_col"], s), ("priority", i), ("status", s), (V["open_ts"], dt),
                      (V["close_ts"], dt), ("opened_date", dd), ("opened_date_key", i), ("hours_to_close", d),
                      ("is_open", i), (V["amount_col"], d), (V["qty_col"], d), (reg, s), (E["type_col"], s),
                      (E["tier_col"], i)],
        n["g_rfact"]: [(E["id"], i), ("reading_hour", dt), ("reading_date", dd), ("reading_date_key", i), ("readings", i),
                       (f"avg_{m1}", d), (f"max_{m1}", d)] + [(f"avg_{x['col']}", d) for x in m[1:]] +
                      [("breach_readings", i)],
        n["g_score"]: [(E["id"], i), (E["code"], s), (E["name_col"], s), (E["type_col"], s), (reg, s), ("status", s),
                       (E["tier_col"], i), (f"{vt}s_12m", i), (f"open_{vt}s", i), (f"urgent_{vt}s_12m", i),
                       (f"{V['amount_col']}_12m", d), (f"max_{m1}", d), ("breach_readings", i), (E["cap_col"], d), ("risk_score", d),
                       ("risk_band", s), (p["impact"]["col"], d)],
    }
    sums = {"readings", "breach_readings", "is_open", V["amount_col"], V["qty_col"], f"{vt}s_12m", f"open_{vt}s",
            f"urgent_{vt}s_12m", f"{V['amount_col']}_12m", p["impact"]["col"]}
    cols = {t: [(c, ty, "sum" if (c in sums and t != n["g_date"]) else "none") for c, ty in cs] for t, cs in tables.items()}
    desc = {
        n["g_date"]: "Calendar with Australian financial year (FY ends 30 June).",
        n["g_dim"]: f"Current version of each {E['label']} (history stays in Silver SCD2).",
        n["g_fact"]: f"{cap(V['plural'])} with hours to {stem(V['close_verb'])} and {E['label']} attributes.",
        n["g_rfact"]: f"Hourly {R['label']} per {E['label']} with threshold breaches ({m1} > {m[0]['threshold']}).",
        n["g_score"]: f"{cap(E['label'])} risk score and band (illustrative weights).",
    }
    F, RF, SC = n["g_fact"], n["g_rfact"], n["g_score"]
    ev = title(V["plural"])
    urgent_name = title(V["type_names"][V["types"].index(V["urgent_type"])])
    amt = V["amount_col"]
    amt_name = title(amt[:-4] if amt.endswith("_aud") else amt) + (" (AUD)" if amt.endswith("_aud") else "")
    unit = m[0]["unit"]
    measures = [  # (name, DAX, format, folder, description)
        (ev, f"COUNTROWS ( {F} )", "#,0", title(V["plural"]), f"{cap(V['plural'])} {V['open_verb']}"),
        (f"Open {ev}", f"CALCULATE ( [{ev}], {F}[is_open] = 1 )", "#,0", title(V["plural"]),
         f"{cap(V['plural'])} not yet {V['close_verb']} ({', '.join(n['open_status'])})"),
        (f"{urgent_name} {ev}", f"CALCULATE ( [{ev}], {F}[{V['type_col']}] = \"{V['urgent_type']}\" )", "#,0",
         title(V["plural"]), f"{cap(V['plural'])} of type {V['urgent_type']}"),
        (f"Avg Hours to {title(stem(V['close_verb']))}", f"AVERAGE ( {F}[hours_to_close] )", "#,0.0", title(V["plural"]),
         f"Average hours from {V['open_verb']} to {V['close_verb']}"),
        (amt_name, f"SUM ( {F}[{amt}] )", "$#,0" if amt.endswith("_aud") else "#,0", title(V["plural"]),
         f"Sum of {amt}"),
        ("Breach Readings", f"SUM ( {RF}[breach_readings] )", "#,0", title(R["label"]),
         f"Readings with {m[0]['label']} above {m[0]['threshold']} {unit}"),
        ("Breach Rate %", f"DIVIDE ( [Breach Readings], SUM ( {RF}[readings] ) )", "0.0%", title(R["label"]),
         "Breach readings / all readings"),
        (f"Peak {title(m[0]['label'])}", f"MAX ( {RF}[max_{m1}] )", "#,0.0", title(R["label"]),
         f"Highest hourly maximum of {m1} ({unit})"),
        (f"High Risk {title(E['plural'])}", f"CALCULATE ( COUNTROWS ( {SC} ), {SC}[risk_band] = \"High\" )", "#,0",
         "Risk", f"{cap(E['plural'])} in the High risk band (score 60 or more)"),
        ("Avg Risk Score", f"AVERAGE ( {SC}[risk_score] )", "#,0.0", "Risk", "Average illustrative risk score (0-100)"),
        (f"{cap(p['impact']['label'])} (AUD)", f"SUM ( {SC}[{p['impact']['col']}] )", "$#,0", "Risk",
         f"Illustrative {p['impact']['label']}: {p['impact']['rule']}"),
    ]
    rels = [(F, E["id"], n["g_dim"], E["id"]), (F, "opened_date_key", n["g_date"], "date_key"),
            (RF, E["id"], n["g_dim"], E["id"]), (RF, "reading_date_key", n["g_date"], "date_key"),
            (SC, E["id"], n["g_dim"], E["id"])]
    region = p["regions"][0]
    role = dict(name=f"{region} only", region=region,
                filters=[(n["g_dim"], reg), (n["g_score"], reg)],
                desc=f"Row-level security example: a {p['region_label']} manager sees only {region}.")
    return dict(tables=cols, desc=desc, measures=measures, rels=rels, role=role, n=n)


def cap(s):
    return s[:1].upper() + s[1:]


# ------------------------------------------------------------------------------------------ writers
class Out:
    def __init__(self, root):
        self.root = root

    def w(self, rel, text):
        path = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)

    def wj(self, rel, obj):
        self.w(rel, json.dumps(obj, indent=2, ensure_ascii=False) + "\n")


def write_model(o, p, spec, name, display, offline, csv_folder):
    SM = f"{name}.SemanticModel"
    n = spec["n"]
    for t, cols in spec["tables"].items():
        L = [f"/// {spec['desc'][t]}", f"table {t}", f"\tlineageTag: {gid(name + 't' + t)}", ""]
        for c, dt, sb in cols:
            L += [f"\tcolumn {c}", f"\t\tdataType: {TYPES[dt][0]}"]
            fmt = TYPES[dt][2]
            if c.endswith("_key") or c == "calendar_year" or c == "fy_year":
                fmt = "0" if dt == "int64" else fmt
            if fmt:
                L.append(f"\t\tformatString: {fmt}")
            L += [f"\t\tlineageTag: {gid(name + t + c)}", f"\t\tsummarizeBy: {sb}", f"\t\tsourceColumn: {c}"]
            if c.endswith("date_key") or (t != n["g_dim"] and c == p["entity"]["id"]):
                L.append("\t\tisHidden")
            if t == n["g_date"] and c == "month_name":
                L.append("\t\tsortByColumn: month_number")
            L += ["", "\t\tannotation SummarizationSetBy = Automatic", ""]
        types = ", ".join(f'{{"{c}", {TYPES[dt][1]}}}' for c, dt, _ in cols)
        L += [f"\tpartition {t} = m", "\t\tmode: import", "\t\tsource =", "\t\t\t\tlet",
              f'\t\t\t\t    Source = fnGold("{t}"),',
              f'\t\t\t\t    Typed = Table.TransformColumnTypes(Source, {{{types}}}, "en-AU")',
              "\t\t\t\tin", "\t\t\t\t    Typed", "", "\tannotation PBI_ResultType = Table", ""]
        o.w(f"{SM}/definition/tables/{t}.tmdl", "\n".join(L))

    L = ["/// Measures used by every report page. Folders group them by business area.", f"table {q(KM)}",
         f"\tlineageTag: {gid(name + 'keymeasures')}", ""]
    for mn, dax, fmt, folder, d in spec["measures"]:
        L += [f"\t/// {d}", f"\tmeasure {q(mn)} = {dax}", f"\t\tformatString: {fmt}", f"\t\tdisplayFolder: {folder}",
              f"\t\tlineageTag: {gid(name + 'm' + mn)}", ""]
    L += ["\tcolumn Placeholder", "\t\tdataType: string", "\t\tisHidden", f"\t\tlineageTag: {gid(name + 'ph')}",
          "\t\tsummarizeBy: none", "\t\tsourceColumn: Placeholder", "", "\t\tannotation SummarizationSetBy = Automatic", "",
          f"\tpartition {q(KM)} = m", "\t\tmode: import", "\t\tsource =", "\t\t\t\tlet",
          '\t\t\t\t    Source = #table(type table [Placeholder = text], {})', "\t\t\t\tin", "\t\t\t\t    Source", "",
          "\tannotation PBI_ResultType = Table", ""]
    o.w(f"{SM}/definition/tables/{KM}.tmdl", "\n".join(L))

    L = []
    for ft, fc, tt, tc in spec["rels"]:
        L += [f"relationship {gid(name + 'r' + ft + fc)}", f"\tfromColumn: {ft}.{fc}", f"\ttoColumn: {tt}.{tc}", ""]
    o.w(f"{SM}/definition/relationships.tmdl", "\n".join(L))

    mode = "CSV" if offline else "Fabric"
    folder = (csv_folder or "C:\\pbi\\gold_csv\\").replace('"', '""')
    o.w(f"{SM}/definition/expressions.tmdl", "\n".join([
        '/// Fabric = gold schema in the lakehouse SQL analytics endpoint. CSV = offline Gold files written by --offline.',
        f'expression DataSourceMode = "{mode}" meta [IsParameterQuery=true, List={{"Fabric", "CSV"}}, '
        f'DefaultValue="{mode}", Type="Text", IsParameterQueryRequired=true]',
        f"\tlineageTag: {gid(name + 'p1')}", "\tqueryGroup: Parameters", "", "\tannotation PBI_ResultType = Text", "",
        '/// SQL analytics endpoint of lh_mdf, e.g. xxxx.datawarehouse.fabric.microsoft.com (Lakehouse > Settings).',
        'expression SqlEndpoint = "your-endpoint.datawarehouse.fabric.microsoft.com" meta [IsParameterQuery=true, '
        'Type="Text", IsParameterQueryRequired=true]',
        f"\tlineageTag: {gid(name + 'p2')}", "\tqueryGroup: Parameters", "", "\tannotation PBI_ResultType = Text", "",
        '/// Lakehouse (database) name used by the demo kit.',
        'expression Database = "lh_mdf" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
        f"\tlineageTag: {gid(name + 'p3')}", "\tqueryGroup: Parameters", "", "\tannotation PBI_ResultType = Text", "",
        '/// Folder holding the offline Gold CSVs. Must end with a path separator.',
        f'expression CsvFolder = "{folder}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
        f"\tlineageTag: {gid(name + 'p4')}", "\tqueryGroup: Parameters", "", "\tannotation PBI_ResultType = Text", "",
        '/// Returns one Gold table from either source, so every table query stays one line.',
        "expression fnGold =", "\t\t(tableName as text) as table =>", "\t\tlet",
        "\t\t    Source =", '\t\t        if DataSourceMode = "Fabric" then',
        '\t\t            Sql.Database(SqlEndpoint, Database){[Schema = "gold", Item = tableName]}[Data]',
        "\t\t        else", "\t\t            Table.PromoteHeaders(",
        '\t\t                Csv.Document(File.Contents(CsvFolder & tableName & ".csv"), [Delimiter = ",", Encoding = 65001, '
        'QuoteStyle = QuoteStyle.Csv]),',
        "\t\t                [PromoteAllScalars = true])", "\t\tin", "\t\t    Source",
        f"\tlineageTag: {gid(name + 'fn')}", "\tqueryGroup: Functions", "", "\tannotation PBI_ResultType = Function", ""]))

    r = spec["role"]
    L = [f"/// {r['desc']}", f"role {q(r['name'])}", "\tmodelPermission: read", ""]
    for t, c in r["filters"]:
        L += [f'\ttablePermission {t} = [{c}] = "{r["region"]}"', ""]
    o.w(f"{SM}/definition/roles/{r['name']}.tmdl", "\n".join(L))

    order = list(spec["tables"]) + [KM]
    o.w(f"{SM}/definition/model.tmdl", "\n".join([
        "model Model", "\tculture: en-AU", "\tdefaultPowerBIDataSourceVersion: powerBI_V3", "\tdiscourageImplicitMeasures",
        "\tsourceQueryCulture: en-AU", "\tdataAccessOptions", "\t\tlegacyRedirects", "\t\treturnErrorValuesAsNull", "",
        "queryGroup Parameters", "", "\tannotation PBI_QueryGroupOrder = 0", "",
        "queryGroup Functions", "", "\tannotation PBI_QueryGroupOrder = 1", "",
        "annotation __PBI_TimeIntelligenceEnabled = 0", "",
        "annotation PBI_QueryOrder = " + json.dumps(["DataSourceMode", "SqlEndpoint", "Database", "CsvFolder", "fnGold"] + order),
        "", "annotation PBIDesktopVersion = 2.149.0", ""] + [f"ref table {q(t)}" for t in order] +
        [""]))
    o.w(f"{SM}/definition/database.tmdl", "database\n\tcompatibilityLevel: 1600\n\n")
    o.wj(f"{SM}/definition.pbism", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/"
                                               "definitionProperties/1.0.0/schema.json", "version": "4.2", "settings": {}})
    o.wj(f"{SM}/.platform", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
                                        "platformProperties/2.0.0/schema.json",
                             "metadata": {"type": "SemanticModel", "displayName": display},
                             "config": {"version": "2.0", "logicalId": gid(name + "sm")}})


# ------------------------------------------------------------------------------------------ report (PBIR)
def lit(v):
    return {"expr": {"Literal": {"Value": v}}}


def col(c):
    return {"solid": {"color": lit(f"'{c}'")}}


def fld(ref):
    t, nm = ref.split("|")
    kind = "Measure" if t == KM else "Column"
    return {kind: {"Expression": {"SourceRef": {"Entity": t}}, "Property": nm}}


def proj(ref):
    t, nm = ref.split("|")
    return {"field": fld(ref), "queryRef": f"{t}.{nm}", "nativeQueryRef": nm}


def container(ttl=None):
    o = {"background": [{"properties": {"show": lit("true"), "color": col("#FFFFFF"), "transparency": lit("0D")}}],
         "border": [{"properties": {"show": lit("true"), "color": col("#DDE3EA"), "radius": lit("6D")}}],
         "visualHeader": [{"properties": {"show": lit("false")}}],
         "padding": [{"properties": {"top": lit("8D"), "bottom": lit("8D"), "left": lit("10D"), "right": lit("10D")}}]}
    if ttl:
        o["title"] = [{"properties": {"show": lit("true"), "text": lit("'" + ttl.replace("'", "''") + "'"),
                                      "fontSize": lit("14D"), "bold": lit("true"), "fontColor": col(INK)}}]
    else:
        o["title"] = [{"properties": {"show": lit("false")}}]
    return o


def chart_obj(color=TEAL, labels=True):
    return {"dataPoint": [{"properties": {"fill": col(color)}}],
            "labels": [{"properties": {"show": lit("true" if labels else "false"), "fontSize": lit("10D")}}],
            "categoryAxis": [{"properties": {"fontSize": lit("10D")}}], "valueAxis": [{"properties": {"fontSize": lit("10D")}}]}


class Page:
    def __init__(self, key, display):
        self.key, self.display, self.visuals, self.n = key, display, [], 0

    def add(self, vtype, x, y, wd, ht, roles=None, ttl=None, objects=None, sort=None, alt=None):
        self.n += 1
        nm = f"{self.key}_{self.n:02d}_{vtype}"[:50]
        v = {"visualType": vtype}
        if roles:
            v["query"] = {"queryState": {r: {"projections": [proj(f) for f in fs]} for r, fs in roles.items()}}
            if sort:
                ref, direction = sort
                v["query"]["sortDefinition"] = {"sort": [{"field": fld(ref), "direction": direction}], "isDefaultSort": False}
        if objects:
            v["objects"] = objects
        v["visualContainerObjects"] = container(ttl)
        if alt:
            v["visualContainerObjects"]["general"] = [{"properties": {"altText": lit("'" + alt.replace("'", "''") + "'")}}]
        if vtype == "textbox":
            v["visualContainerObjects"]["background"] = [{"properties": {"show": lit("false")}}]
            v["visualContainerObjects"]["border"] = [{"properties": {"show": lit("false")}}]
        v["drillFilterOtherVisuals"] = True
        self.visuals.append({"$schema": VC, "name": nm,
                             "position": {"x": x, "y": y, "z": self.n * 1000, "height": ht, "width": wd,
                                          "tabOrder": self.n * 1000},
                             "visual": v})

    def text(self, x, y, wd, ht, runs):
        paras = [{"textRuns": [{"value": t, "textStyle": {"fontSize": f"{sz}pt", "fontWeight": "bold" if b else "normal",
                                                           "color": c}}]} for t, sz, b, c in runs]
        self.add("textbox", x, y, wd, ht, objects={"general": [{"properties": {"paragraphs": paras}}]})

    def card(self, x, y, measure, wd=196, ht=104):
        self.add("card", x, y, wd, ht, {"Values": [f"{KM}|{measure}"]},
                 objects={"labels": [{"properties": {"fontSize": lit("24D"), "color": col(NAVY), "labelDisplayUnits": lit("0D")}}],
                          "categoryLabels": [{"properties": {"show": lit("true"), "fontSize": lit("11D"), "color": col(GREY)}}]},
                 alt=f"Card showing {measure}")

    def slicer(self, x, y, ref, ttl, wd=180, ht=62):
        self.add("slicer", x, y, wd, ht, {"Values": [ref]}, ttl=ttl, objects={"data": [{"properties": {"mode": lit("'Dropdown'")}}]})

    def header(self, ttl, sub):
        self.text(24, 12, 820, 70, [(ttl, 22, True, NAVY), (sub, 11, False, GREY)])


def build_pages(p, spec):
    n, E, V, R = spec["n"], p["entity"], p["event"], p["reading"]
    reg, D, F, RF, SC, DT = n["reg"], n["g_dim"], n["g_fact"], n["g_rfact"], n["g_score"], n["g_date"]
    ms = [m[0] for m in spec["measures"]]
    ev, open_ev, urgent, hours, amount, breach, rate, peak, high, risk, impact = ms
    reg_t = cap(p["region_label"])
    m1 = R["metrics"][0]
    pages = []

    def slicers(pg):
        pg.slicer(860, 16, f"{D}|{reg}", reg_t)
        pg.slicer(1060, 16, f"{D}|{E['type_col']}", title(E["type_col"]), wd=196)

    pg = Page("p1_overview", "Overview")
    pages.append(pg)
    pg.header(f"{p['org']} - operations overview",
              f"{cap(V['plural'])} and {E['plural']} from the Gold layer of lh_mdf | fictional organisation, synthetic data")
    slicers(pg)
    for k, m in enumerate([ev, open_ev, urgent, hours, amount, high]):
        pg.card(24 + k * 207, 96, m)
    pg.add("clusteredBarChart", 24, 214, 500, 490, {"Category": [f"{D}|{reg}"], "Y": [f"{KM}|{ev}", f"{KM}|{open_ev}"]},
           ttl=f"{ev} by {p['region_label']}", sort=(f"{KM}|{ev}", "Descending"))
    pg.add("lineChart", 536, 214, 720, 240, {"Category": [f"{DT}|year_month"], "Y": [f"{KM}|{ev}"]},
           ttl=f"{ev} by month", objects={"dataPoint": [{"properties": {"fill": col(TEAL)}}]})
    pg.add("clusteredColumnChart", 536, 466, 720, 238, {"Category": [f"{DT}|year_month"], "Y": [f"{KM}|{amount}"]},
           ttl=f"{amount} by month", objects=chart_obj(NAVY, labels=False))

    pg = Page("p2_risk", f"{title(E['label'])} risk")
    pages.append(pg)
    pg.header(f"{title(E['label'])} risk and {R['label']}",
              f"Risk score from open and urgent {V['plural']} plus {m1['label']} breaches (> {m1['threshold']} "
              f"{m1['unit']}) | weights and dollar rates are illustrative")
    slicers(pg)
    for k, m in enumerate([impact, high, risk, breach, peak]):
        pg.card(24 + k * 248, 96, m, wd=236)
    pg.add("tableEx", 24, 214, 820, 490,
           {"Values": [f"{SC}|{E['code']}", f"{SC}|{E['name_col']}", f"{SC}|{E['type_col']}", f"{SC}|{reg}",
                       f"{SC}|{E['tier_col']}", f"{SC}|open_{V['table']}s", f"{SC}|urgent_{V['table']}s_12m",
                       f"{SC}|breach_readings", f"{SC}|risk_score", f"{SC}|risk_band", f"{SC}|{p['impact']['col']}"]},
           ttl=f"Highest-risk {E['plural']}", sort=(f"{SC}|risk_score", "Descending"))
    pg.add("clusteredColumnChart", 856, 214, 400, 240, {"Category": [f"{D}|{E['type_col']}"], "Y": [f"{KM}|{breach}"]},
           ttl=f"{breach} by {E['type_col'].replace('_', ' ')}", objects=chart_obj(RED), sort=(f"{KM}|{breach}", "Descending"))
    pg.add("lineChart", 856, 466, 400, 238, {"Category": [f"{RF}|reading_hour"], "Y": [f"{KM}|{breach}"]},
           ttl=f"{breach} by hour", objects={"dataPoint": [{"properties": {"fill": col(RED)}}]})

    pg = Page("p3_detail", f"{title(V['label'])} detail")
    pages.append(pg)
    pg.header(f"{title(V['label'])} detail", f"Every {V['label']} in Gold with its {E['label']} | filter with the slicers")
    pg.slicer(860, 16, f"{F}|{V['type_col']}", title(V["type_col"]))
    pg.slicer(1060, 16, f"{F}|status", "Status", wd=196)
    for k, m in enumerate([ev, open_ev, hours, amount]):
        pg.card(24 + k * 311, 96, m, wd=300)
    pg.add("clusteredColumnChart", 24, 214, 400, 240, {"Category": [f"{F}|{V['type_col']}"], "Y": [f"{KM}|{ev}"]},
           ttl=f"{ev} by {V['type_col'].replace('_', ' ')}", objects=chart_obj(NAVY), sort=(f"{KM}|{ev}", "Descending"))
    pg.add("clusteredBarChart", 24, 466, 400, 238, {"Category": [f"{F}|status"], "Y": [f"{KM}|{hours}"]},
           ttl=f"{hours} by status", objects=chart_obj(SAND))
    pg.add("tableEx", 436, 214, 820, 490,
           {"Values": [f"{F}|{V['id']}", f"{D}|{E['code']}", f"{F}|{V['type_col']}", f"{F}|priority", f"{F}|status",
                       f"{F}|{V['open_ts']}", f"{F}|{V['close_ts']}", f"{F}|hours_to_close", f"{F}|{V['amount_col']}"]},
           ttl=f"{cap(V['plural'])}, newest first", sort=(f"{F}|{V['open_ts']}", "Descending"))
    return pages


def write_report(o, pages, name, display, short):
    RP = f"{name}.Report"
    for pg in pages:
        base = f"{RP}/definition/pages/{pg.key}"
        o.wj(f"{base}/page.json", {"$schema": PG, "name": pg.key, "displayName": pg.display, "displayOption": "FitToPage",
                                   "height": 720, "width": 1280,
                                   "objects": {"background": [{"properties": {"color": col(BG), "transparency": lit("0D")}}]}})
        for v in pg.visuals:
            o.wj(f"{base}/visuals/{v['name']}/visual.json", v)
    o.wj(f"{RP}/definition/pages/pages.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [pg.key for pg in pages], "activePageName": pages[0].key})
    o.wj(f"{RP}/definition/version.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "2.0.0"})
    theme = f"{short}_Theme.json"
    rv = {"visual": "2.4.0", "report": "3.0.0", "page": "2.0.0"}
    o.wj(f"{RP}/definition/report.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.0.0/schema.json",
        "themeCollection": {"baseTheme": {"name": "CY24SU10", "reportVersionAtImport": rv, "type": "SharedResources"},
                            "customTheme": {"name": theme, "reportVersionAtImport": rv, "type": "RegisteredResources"}},
        "objects": {"outspacePane": [{"properties": {"expanded": lit("false"), "visible": lit("true")}}]},
        "resourcePackages": [{"name": "SharedResources", "type": "SharedResources",
                              "items": [{"name": "CY24SU10", "path": "BaseThemes/CY24SU10.json", "type": "BaseTheme"}]},
                             {"name": "RegisteredResources", "type": "RegisteredResources",
                              "items": [{"name": theme, "path": theme, "type": "CustomTheme"}]}],
        "settings": {"useStylableVisualContainerHeader": True, "defaultDrillFilterOtherVisuals": True,
                     "allowChangeFilterTypes": True, "useEnhancedTooltips": True, "useDefaultAggregateDisplayName": True,
                     "exportDataMode": "AllowSummarized"}})
    o.wj(f"{RP}/StaticResources/RegisteredResources/{theme}", {
        "name": theme[:-5], "dataColors": [NAVY, TEAL, SAND, RED, "#5C7AEA", "#7A8B99", "#2A9D8F", "#F4A261"],
        "foreground": INK, "background": "#FFFFFF", "tableAccent": NAVY, "good": TEAL, "neutral": SAND, "bad": RED,
        "textClasses": {"callout": {"fontSize": 24, "fontFace": "Segoe UI Semibold", "color": NAVY},
                        "title": {"fontSize": 14, "fontFace": "Segoe UI Semibold", "color": INK},
                        "header": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": INK},
                        "label": {"fontSize": 10, "fontFace": "Segoe UI", "color": INK}}})
    o.wj(f"{RP}/StaticResources/SharedResources/BaseThemes/CY24SU10.json", {
        "name": "CY24SU10", "dataColors": ["#118DFF", "#12239E", "#E66C37", "#6B007B", "#E044A7", "#744EC2", "#D9B300", "#D64550"],
        "foreground": "#252423", "foregroundNeutralSecondary": "#605E5C", "foregroundNeutralTertiary": "#B3B0AD",
        "background": "#FFFFFF", "backgroundLight": "#F3F2F1", "backgroundNeutral": "#C8C6C4", "tableAccent": "#118DFF",
        "good": "#1AAB40", "neutral": "#D9B300", "bad": "#D64554", "maximum": "#118DFF", "center": "#D9B300",
        "minimum": "#DEEFFF", "null": "#FF7F48",
        "textClasses": {"callout": {"fontSize": 45, "fontFace": "DIN", "color": "#252423"},
                        "title": {"fontSize": 12, "fontFace": "DIN", "color": "#252423"},
                        "header": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": "#252423"},
                        "label": {"fontSize": 10, "fontFace": "Segoe UI", "color": "#252423"}}})
    o.wj(f"{RP}/definition.pbir", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0", "datasetReference": {"byPath": {"path": f"../{name}.SemanticModel"}}})
    o.wj(f"{RP}/.platform", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
                                        "platformProperties/2.0.0/schema.json",
                             "metadata": {"type": "Report", "displayName": display},
                             "config": {"version": "2.0", "logicalId": gid(name + "rp")}})
    o.wj(f"{name}.pbip", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
                          "version": "1.0", "artifacts": [{"report": {"path": f"{name}.Report"}}],
                          "settings": {"enableAutoRecovery": True}})
    o.w(".gitignore", "**/.pbi/localSettings.json\n**/.pbi/cache.abf\n")


# ------------------------------------------------------------------------------------------ offline Gold CSVs
def dim_date_rows():
    rows, d = [], date(2020, 1, 1)
    while d <= date(2028, 12, 31):
        fy = d.year + 1 if d.month >= 7 else d.year
        rows.append([int(d.strftime("%Y%m%d")), d.isoformat(), d.year, (d.month - 1) // 3 + 1, d.month, d.strftime("%b"),
                     d.strftime("%Y-%m"), d.day, d.strftime("%a"), fy, f"FY{str(fy)[2:]}", d.weekday() >= 5])
        d += timedelta(days=1)
    return rows


def gold_tables(p, spec):
    """Runs the pack offline (tools/local_check.py) and returns {table: (columns, rows)} for the model tables."""
    import local_check
    problems, gold, _ = local_check.run_pack(p, verbose=False)
    if problems:
        print("local_check problems:", *problems, sep="\n  ")
    gold = dict(gold)
    gold[spec["n"]["g_date"]] = ([c for c, _, _ in spec["tables"][spec["n"]["g_date"]]], dim_date_rows())
    return {t: gold[t] for t in spec["tables"]}


def write_csvs(folder, tables, spec):
    os.makedirs(folder, exist_ok=True)
    for t, (cols, rows) in tables.items():
        want = [c for c, _, _ in spec["tables"][t]]
        if list(cols) != want:
            raise SystemExit(f"{t}: Gold columns {cols} don't match the model {want}")
        with open(os.path.join(folder, f"{t}.csv"), "w", encoding="utf-8", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(cols)
            wr.writerows([["" if v is None else v for v in r] for r in rows])


# ------------------------------------------------------------------------------------------ main
def build(pack_id, out=None, offline=False):
    p = kitlib.load_pack(pack_id)
    short = p["short"]
    name = f"{short}_Ops"
    display = f"{short} Operations"
    out = os.path.abspath(out or os.path.join(os.getcwd(), "pbi", pack_id))
    spec = model_spec(p)
    for d in (f"{name}.SemanticModel", f"{name}.Report"):
        shutil.rmtree(os.path.join(out, d), ignore_errors=True)
    o = Out(out)
    csv_folder = None
    if offline:
        csv_folder = os.path.join(out, "gold_csv") + os.sep
        write_csvs(csv_folder, gold_tables(p, spec), spec)
    write_model(o, p, spec, name, display, offline, csv_folder)
    pages = build_pages(p, spec)
    write_report(o, pages, name, display, short)
    print(f"{p['id']}: {os.path.join(out, name + '.pbip')} | {len(spec['tables'])} tables, {len(spec['measures'])} measures, "
          f"{len(pages)} pages, {sum(len(pg.visuals) for pg in pages)} visuals" + (f" | CSVs in {csv_folder}" if offline else ""))
    return out, name, spec, pages


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pack", required=True, choices=[pk["id"] for pk in kitlib.all_packs()])
    ap.add_argument("--out", help="output folder (default ./pbi/<pack>)")
    ap.add_argument("--offline", action="store_true", help="build Gold offline and point the model at CSVs")
    a = ap.parse_args(argv)
    build(a.pack, a.out, a.offline)


if __name__ == "__main__":
    main()
