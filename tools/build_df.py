"""Dataflow Gen2: business-owned Excel reference data -> Silver, and a parameterised Gold transform.

Per pack:
  dataflows/<pack>.pq            every query for both dataflows (paste each block into Blank query > Advanced editor)
  dataflows/excel/<pack>.xlsx    the business workbook (also embedded in the .pq as Base64, so no SharePoint is needed)
plus dataflows/README.md (rewritten on every build so the pack table stays current).

Usage:  python tools/build_df.py            build all packs
        python tools/build_df.py --check    build all packs, then run the offline checks (M syntax, pandas replay)
build(p, kit_dir) is called by tools/build_kit.py for each pack. Needs openpyxl (and pandas for --check).
"""
import base64, io, os, re, sys, zipfile
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kitlib  # noqa: E402

LH = "lh_mdf"
SHEET = "Targets"
TBC_ROW = 2            # which ref row the business hasn't agreed yet
FIXED_TS = (2026, 7, 1, 9, 0, 0)
AS_AT_DEFAULT = "2026-11-04"
M_KEYWORDS = {"and", "as", "each", "else", "error", "false", "if", "in", "is", "let", "meta", "not", "null", "or",
              "otherwise", "section", "shared", "then", "true", "try", "type"}
M_TYPE_WORDS = {"text", "number", "date", "datetime", "datetimezone", "logical", "nullable", "any", "table", "record",
                "list", "function", "binary", "duration", "time", "none", "optional"}


def names(p):
    V, E = p["event"], p["entity"]
    return dict(df_ref=f"df_{p['id']}_reference", df_gold=f"df_{p['id']}_gold",
                ref_table=f"ref_{V['table']}_target", gold_table=f"{V['table']}_target_compliance",
                xlsx=f"{p['id']}.xlsx", vtype=V["type_col"], vid=V["id"], eid=E["id"])


def mf(col):
    """M field access for a column name."""
    return f'[{col}]' if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col) and col not in M_KEYWORDS else f'[#"{col}"]'


def ms(text):
    return '"' + text.replace('"', '""') + '"'


# ---------------------------------------------------------------------------------------------- workbook
def headers(p):
    return [f"{p['event']['label'].capitalize()} type", "Code", "Category", "Target (hours)", "Owner team", "Notes"]


def sheet_rows(p):
    """The messy business sheet, row by row (None = empty cell)."""
    V, ref = p["event"], p["ref"]
    rows = [[ref["title"]],
            [f"Owner: {ref['owner']} | Effective 1 July 2026 | Values marked TBC are still being agreed"],
            [],
            headers(p)]
    for idx, (code, cat, hrs, team) in enumerate(ref["rows"]):
        tname = V["type_names"][V["types"].index(code)] if code in V["types"] else code.title()
        target = "TBC" if idx == TBC_ROW else hrs
        note = "Under review with the business owner" if idx == TBC_ROW else None
        rows.append([tname, code, cat, target, team + (" " if idx == 1 else ""), note])
    code, cat, hrs, team = ref["rows"][0]
    tname = V["type_names"][V["types"].index(code)] if code in V["types"] else code.title()
    rows.append([tname, code.lower() + " ", cat, hrs, team, "Copied from last year's sheet"])     # duplicate
    rows.append([])
    rows.append([f"* TBC = target not agreed yet. Hours are calendar hours from {V['open_verb']} to {V['close_verb']}."])
    return rows


def expected_ref(p):
    """What dataflow 1 should write (without the two audit columns)."""
    out = []
    for idx, (code, cat, hrs, team) in enumerate(p["ref"]["rows"]):
        out.append((code.strip().upper(), cat.strip(), None if idx == TBC_ROW else float(hrs), team.strip()))
    return out


def workbook_bytes(p):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    wb = Workbook()
    ws = wb.active
    ws.title = SHEET
    for r in sheet_rows(p):
        ws.append(r)
    ws["A1"].font = Font(bold=True, size=14, color="0B2545")
    ws["A2"].font = Font(italic=True, size=10, color="5F6B7A")
    for c in ws[4]:
        c.font, c.fill, c.alignment = Font(bold=True, color="FFFFFF"), PatternFill("solid", fgColor="0B2545"), Alignment(wrap_text=True)
    ws.cell(ws.max_row, 1).font = Font(italic=True, size=9, color="5F6B7A")
    for col, wd in zip("ABCDEF", (28, 20, 14, 14, 28, 36)):
        ws.column_dimensions[col].width = wd
    wb.properties.creator = p["ref"]["owner"]
    wb.properties.created = wb.properties.modified = datetime(*FIXED_TS)
    buf = io.BytesIO()
    wb.save(buf)
    # repack with fixed timestamps so the Base64 in the .pq only changes when the content changes
    src = zipfile.ZipFile(io.BytesIO(buf.getvalue()))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for info in sorted(src.infolist(), key=lambda i: i.filename):
            data = src.read(info.filename)
            if info.filename == "docProps/core.xml":
                data = re.sub(rb"(<dcterms:(created|modified)[^>]*>)[^<]*(<)", rb"\g<1>2026-07-01T09:00:00Z\g<3>", data)
            zi = zipfile.ZipInfo(info.filename, date_time=FIXED_TS)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = 0o600 << 16
            z.writestr(zi, data)
    return out.getvalue()


# ---------------------------------------------------------------------------------------------- Power Query M
PARAM = '{value} meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = "Text"]'

FN_LAKEHOUSE = '''// Reads a table from lakehouse lh_mdf. Tries schema navigation first (silver.x / gold.x), then two fallbacks.
// If your tenant navigates differently, replace the body with the steps Get data > Lakehouse writes for you.
(schemaName as text, tableName as text) as table =>
    let
        Root = Lakehouse.Contents([HierarchicalNavigation = true]),
        LakehouseNav = Root{[workspaceId = WorkspaceId]}[Data]{[lakehouseId = LakehouseId]}[Data],
        BySchema = try LakehouseNav{[Id = schemaName, ItemKind = "Schema"]}[Data]{[Id = tableName, ItemKind = "Table"]}[Data] otherwise null,
        ByDottedId = try LakehouseNav{[Id = schemaName & "." & tableName, ItemKind = "Table"]}[Data] otherwise null,
        ByName = try Table.SelectRows(LakehouseNav, each [Name] = tableName){0}[Data] otherwise null,
        Result =
            if BySchema <> null then BySchema
            else if ByDottedId <> null then ByDottedId
            else if ByName <> null then ByName
            else error Error.Record("TableNotFound", "Lakehouse table not found - check WorkspaceId and LakehouseId, or re-pick the table with Get data", schemaName & "." & tableName)
    in
        Result'''


def m_reference_workbook(b64):
    return f'''// The business workbook, embedded as Base64 so this dataflow needs no SharePoint, OneDrive, gateway or connection.
// To use the live file instead, see dataflows/README.md ("Point it at the live Excel file").
let
    Source = Binary.FromText("{b64}", BinaryEncoding.Base64),
    Workbook = Excel.Workbook(Source, null, true)
in
    Workbook'''


def m_reference_table(p):
    nm = names(p)
    vt = nm["vtype"]
    return f'''// Cleans the business sheet: title rows, friendly headers, a TBC cell, a duplicate row and a footnote.
let
    Sheet = ReferenceWorkbook{{[Item = "{SHEET}", Kind = "Sheet"]}}[Data],
    SkipTitle = Table.Skip(Sheet, each [Column2] <> "Code"),
    Promoted = Table.PromoteHeaders(SkipTitle, [PromoteAllScalars = true]),
    DataRows = Table.SelectRows(Promoted, each [Code] <> null and Text.Trim(Text.From([Code])) <> ""),
    Codes = Table.TransformColumns(DataRows, {{{{"Code", each Text.Upper(Text.Trim(Text.From(_))), type text}}}}),
    Deduped = Table.Distinct(Codes, {{"Code"}}),
    Target = Table.AddColumn(Deduped, "target_hours", each try Number.From([#"Target (hours)"]) otherwise null, type nullable number),
    Renamed = Table.RenameColumns(Target, {{{{"Code", "{vt}"}}, {{"Category", "category"}}, {{"Owner team", "owner_team"}}}}),
    Trimmed = Table.TransformColumns(Renamed, {{{{"category", each Text.Trim(Text.From(_)), type text}}, {{"owner_team", each Text.Trim(Text.From(_)), type text}}}}),
    WithSource = Table.AddColumn(Trimmed, "_source_file", each ReferenceFileName, type text),
    WithLoaded = Table.AddColumn(WithSource, "_loaded_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime),
    Selected = Table.SelectColumns(WithLoaded, {{"{vt}", "category", "target_hours", "owner_team", "_source_file", "_loaded_at"}})
in
    Selected'''


def m_gold(p):
    nm = names(p)
    E, V = p["entity"], p["event"]
    vt, vid, eid, reg = nm["vtype"], nm["vid"], nm["eid"], p["region_col"]
    o, c = V["open_ts"], V["close_ts"]
    return f'''// One row per {V['label']} with its target and status as at AsAtDate: met_target, breached, open_overdue,
// open_within_target or no_target. Cancelled and deleted {V['plural']} are left out.
let
    AsAt = Date.FromText(AsAtDate, [Format = "yyyy-MM-dd", Culture = "en-AU"]),
    AsAtEnd = DateTime.From(Date.AddDays(AsAt, 1)),
    Events = fnLakehouseTable("silver", "{V['table']}"),
    Live = Table.SelectRows(Events, each [_is_deleted] = false and [status] <> "{V['status'][3]}"),
    Columns = Table.SelectColumns(Live, {{"{vid}", "{eid}", "{vt}", "priority", "status", "{o}", "{c}"}}),
    Stamps = Table.TransformColumns(Columns, {{{{"{o}", each if _ = null then null else DateTime.From(_), type nullable datetime}}, {{"{c}", each if _ = null then null else DateTime.From(_), type nullable datetime}}}}),
    InScope = Table.SelectRows(Stamps, each {mf(o)} <> null and {mf(o)} < AsAtEnd),
    Register = Table.SelectRows(fnLakehouseTable("silver", "{E['table']}"), each [_is_current] = true),
    Entities = Table.SelectColumns(Register, {{"{eid}", "{E['name_col']}", "{reg}"}}),
    WithEntity = Table.ExpandTableColumn(Table.NestedJoin(InScope, {{"{eid}"}}, Entities, {{"{eid}"}}, "e", JoinKind.LeftOuter), "e", {{"{E['name_col']}", "{reg}"}}),
    Targets = Table.SelectColumns(fnLakehouseTable("silver", "{nm['ref_table']}"), {{"{vt}", "category", "target_hours", "owner_team"}}),
    WithTarget = Table.ExpandTableColumn(Table.NestedJoin(WithEntity, {{"{vt}"}}, Targets, {{"{vt}"}}, "t", JoinKind.LeftOuter), "t", {{"category", "target_hours", "owner_team"}}),
    OpenFlag = Table.AddColumn(WithTarget, "is_open", each {mf(c)} = null or {mf(c)} >= AsAtEnd, type logical),
    Hours = Table.AddColumn(OpenFlag, "hours_to_close", each Number.Round(Duration.TotalHours((if [is_open] then AsAtEnd else {mf(c)}) - {mf(o)}), 2), type number),
    Status = Table.AddColumn(Hours, "compliance_status", each
        if [target_hours] = null then "no_target"
        else if [is_open] then (if [hours_to_close] > [target_hours] then "open_overdue" else "open_within_target")
        else if [hours_to_close] <= [target_hours] then "met_target"
        else "breached", type text),
    Over = Table.AddColumn(Status, "hours_over_target", each if [target_hours] = null then null else Number.Round(List.Max({{0, [hours_to_close] - [target_hours]}}), 2), type nullable number),
    Stamped = Table.AddColumn(Over, "as_at_date", each AsAt, type date),
    Typed = Table.TransformColumnTypes(Stamped, {{{{"{vid}", Int64.Type}}, {{"{eid}", Int64.Type}}, {{"priority", Int64.Type}}, {{"target_hours", type nullable number}}}})
in
    Typed'''


def queries(p, b64):
    """[(dataflow number, dataflow name, query name, kind, destination or None, M text)]"""
    nm = names(p)
    d1, d2 = nm["df_ref"], nm["df_gold"]
    return [
        (1, d1, "ReferenceFileName", "parameter, Text", None, PARAM.format(value=ms(nm["xlsx"]))),
        (1, d1, "ReferenceWorkbook", "helper, no destination", None, m_reference_workbook(b64)),
        (1, d1, nm["ref_table"], "output", f"{LH} > silver > {nm['ref_table']} (Replace)", m_reference_table(p)),
        (2, d2, "WorkspaceId", "parameter, Text", None, PARAM.format(value=ms("00000000-0000-0000-0000-000000000000"))),
        (2, d2, "LakehouseId", "parameter, Text", None, PARAM.format(value=ms("00000000-0000-0000-0000-000000000000"))),
        (2, d2, "AsAtDate", "parameter, Text yyyy-MM-dd", None, PARAM.format(value=ms(AS_AT_DEFAULT))),
        (2, d2, "fnLakehouseTable", "helper function, no destination", None, FN_LAKEHOUSE),
        (2, d2, nm["gold_table"], "output", f"{LH} > gold > {nm['gold_table']} (Replace)", m_gold(p)),
    ]


BAR = "// " + "=" * 110


def pq_text(p, b64):
    nm = names(p)
    head = [f"// {p['org']} ({p['industry']}) - Dataflow Gen2 queries for the {p['id']} pack. Generated by tools/build_df.py.",
            f"// Dataflow 1 {nm['df_ref']}: the business workbook -> {LH}.silver.{nm['ref_table']}",
            f"// Dataflow 2 {nm['df_gold']}: Silver + targets -> {LH}.gold.{nm['gold_table']}",
            "// For each block: Get data > Blank query > Advanced editor, paste the block (header lines included is fine),",
            "// then rename the query to the QUERY name. Create the blocks in the order they appear.", ""]
    parts = ["\n".join(head)]
    for n, df, qn, kind, dest, text in queries(p, b64):
        hdr = [BAR, f"// DATAFLOW {n}: {df}   QUERY: {qn}   ({kind})"]
        if dest:
            hdr.append(f"// Data destination: {dest}")
        hdr.append(BAR)
        parts.append("\n".join(hdr) + "\n" + text + "\n")
    return "\n".join(parts)


# ---------------------------------------------------------------------------------------------- README
README = """# Dataflow Gen2

The notebooks are the engine for lots of objects that follow one pattern. Dataflow Gen2 does a different job here:

1. **Business-owned reference data in Excel.** Each pack has a small target table (for example work order response
   targets) that a business team keeps in a workbook. It changes on their timetable, has title rows, friendly column
   names and the odd "TBC". Power Query is good at exactly that, and the business can read the steps.
2. **A small, parameterised Gold transform.** It joins Silver events to those targets and works out, per event,
   whether the target was met, breached or is overdue as at a date the pipeline passes in.

Both land in the same lakehouse (`lh_mdf`), next to the framework's tables.

| File | What it is |
|---|---|
| `<pack>.pq` | Every query for both dataflows, one block per query, each with a header naming the dataflow and query |
| `excel/<pack>.xlsx` | The business workbook. The same file is embedded in the `.pq` as Base64 |

## Why a dataflow and not a notebook

| Need | First choice | Why |
|---|---|---|
| Many sources, same pattern | Metadata-driven notebooks | New source = new rows in the control tables, not a new item |
| A business workbook with an odd layout | Dataflow Gen2 | Visual steps the owner can follow; Excel, SharePoint and OneDrive connectors built in |
| Small Gold logic an analyst should own | Dataflow Gen2 | Low code, parameterised, callable from the pipeline |
| SCD2, DQ rules, PII handling at volume | Notebooks | Full control, testable, cheaper at volume |

I'd use public parameters for values (dates, IDs), not for shapes. If you're tempted to parameterise table names so
one dataflow serves many differently shaped tables, that's the job of the metadata-driven notebooks.

## The workbook is messy on purpose

| In the workbook | Power Query step that deals with it |
|---|---|
| Two title rows and a blank row | Skip rows until the one with `Code` in it, then promote headers. Another title line won't break it |
| Friendly headers ("Target (hours)", "Owner team") | Rename to Silver names |
| One target typed as `TBC` | `try Number.From(...) otherwise null`: the target is null, and Gold shows `no_target` |
| The first row repeated in lower case with a trailing space | Trim and upper-case the code, then remove duplicates |
| A trailing space in one owner team | Trim |
| A footnote under the table | Dropped because it has no code |

Every row gets `_source_file` and `_loaded_at`, the same lineage idea as the framework's audit columns.

## Before you start

- Run the notebook path for your pack at least once, so `lh_mdf` has `silver.<event table>` and `silver.<entity table>`.
- Dataflow 1 needs nothing else: no connection, no gateway.
- Dataflow 2 needs the Lakehouse connection, signed in with your own account, and the two GUIDs for `WorkspaceId`
  and `LakehouseId`. Open `lh_mdf` in the browser: the GUID after `/groups/` is the workspace, the one after
  `/lakehouses/` is the lakehouse.

## Create the dataflows (about 10 minutes each)

1. In the workspace, select **New item > Dataflow Gen2**. Name it `df_<pack>_reference` (the header of each block
   in `<pack>.pq` gives the exact names).
2. For each block marked `DATAFLOW 1`, in order:
   - **Get data > Blank query**, then **Advanced editor**.
   - Delete what's there, paste the block and select **OK**.
   - Rename the query to the `QUERY` name in the block header.
   - Parameter blocks (`ReferenceFileName`) become parameters as soon as you paste them. If yours doesn't, create it
     with **Home > Manage parameters > New**: type Text, same name and default value.
   - `ReferenceWorkbook` is one long line of Base64. Paste it as it is.
3. Right-click `ReferenceWorkbook` and clear **Enable staging** (or **Enable load**) so only the output query loads.
4. Select the output query `ref_<event>_target` and set the **data destination**:
   - **Lakehouse**, then `lh_mdf`, schema `silver`, new table `ref_<event>_target`.
   - Update method **Replace**. Turn off automatic settings and check the column types:
     `target_hours` is a decimal number, `_loaded_at` a date/time.
5. **Save and run**. Check `silver.ref_<event>_target` in the lakehouse: one row per type, the TBC row has a null
   target, and the duplicate has gone.
6. Create a second Dataflow Gen2 named `df_<pack>_gold` and repeat for the `DATAFLOW 2` blocks: the three
   parameters, `fnLakehouseTable`, then `<event>_target_compliance`.
7. Set `WorkspaceId` and `LakehouseId`. Sign in to the Lakehouse connection when asked.
8. Destination for `<event>_target_compliance`: `lh_mdf`, schema `gold`, **Replace**. Save and run.

Run dataflow 1 before dataflow 2: the Gold query reads the reference table.

## Parameters

| Dataflow | Parameter | Default | What it does |
|---|---|---|---|
| 1 | `ReferenceFileName` | `<pack>.xlsx` | A label only: written to `_source_file` for lineage |
| 2 | `WorkspaceId` | zeros | Workspace that holds `lh_mdf` |
| 2 | `LakehouseId` | zeros | The `lh_mdf` lakehouse |
| 2 | `AsAtDate` | `""" + AS_AT_DEFAULT + """` | The reporting date, `yyyy-MM-dd`. Open events are aged to the end of this day |

All parameters are Text on purpose. Pipeline values arrive as strings, and the query converts them with `Date.FromText`.

## Call them from the pipeline

1. In each dataflow, open **Home > Options > Parameters** and allow parameters to be overridden at run time
   (public parameters). Wording varies between releases: verify in your tenant.
2. In `pl_mdf_orchestrator`, add a **Dataflow** activity for `df_<pack>_reference`. It has no upstream dependency, so
   it can run at the start.
3. Add a second **Dataflow** activity for `df_<pack>_gold`. Make it depend on the Bronze to Silver step **and** on the
   reference dataflow.
4. In the Gold activity, under **Settings > Dataflow parameters**, add `AsAtDate` as a string with dynamic content:
   `@formatDateTime(utcNow(), 'yyyy-MM-dd')`
   That's the UTC date. For the Sydney date use
   `@convertFromUtc(utcNow(), 'AUS Eastern Standard Time', 'yyyy-MM-dd')`.
5. Optional: schedule the reference dataflow on its own (say 6 am daily) if the business edits the workbook during
   the day.

## Point it at the live Excel file

Use this to show the owner changing a target and the next refresh picking it up. It still needs no SharePoint.

1. In `lh_mdf`, select **Files > New subfolder** `reference`, then **Upload > Upload files** and pick
   `excel/<pack>.xlsx`.
2. In dataflow 1, select `ReferenceWorkbook` and open the **Advanced editor**.
3. Replace the `Source` line with the file from the lakehouse. The easiest way: **Get data > Lakehouse > lh_mdf >
   Files > reference**, pick the file, then copy the navigation steps Power Query writes into `ReferenceWorkbook`
   and point `Workbook = Excel.Workbook(...)` at the file content. Delete the `Binary.FromText` step.
4. Download the file, change the TBC to a number, upload it again and refresh dataflow 1. The null target turns into
   a number. Refresh dataflow 2 and the `no_target` rows become `met_target`, `breached` or one of the open statuses.

In production the workbook usually lives in SharePoint or OneDrive. Swap the source for **Get data > SharePoint
folder** (or OneDrive) the same way: the cleaning steps don't change.

## Packs

| Pack | Reference workbook | Silver table (dataflow 1) | Gold table (dataflow 2) | TBC row |
|---|---|---|---|---|
{pack_rows}

## Checks and notes

- `python tools/build_df.py --check` rebuilds everything and, for every pack:
  - replays the reference cleaning in pandas on the generated workbook and compares it with the expected rows;
  - checks the codes cover every event type in the pack;
  - checks the Base64 in the `.pq` decodes to exactly `excel/<pack>.xlsx`;
  - checks each M block: balanced brackets and quotes, `let` and `in` paired, a binding on every line, every step
    name defined.
- `fnLakehouseTable` tries schema navigation first, then two fallbacks. If your tenant's Lakehouse connector
  navigates differently, replace its body with the steps **Get data > Lakehouse** writes for you.
- Writing to a schema (`silver`, `gold`) in a schema-enabled lakehouse from a dataflow destination is newer than the
  rest. Verify in your tenant; if the schema isn't offered, write to `dbo` and adjust the Gold query.
- The targets are illustrative. They come from `ref` in `config/<pack>.json`.
"""


def readme(packs):
    rows = []
    for p in packs:
        nm = names(p)
        tbc = p["ref"]["rows"][TBC_ROW][0]
        rows.append(f"| {p['id']} | {p['ref']['title']} (`excel/{nm['xlsx']}`) | `silver.{nm['ref_table']}` | "
                    f"`gold.{nm['gold_table']}` | `{tbc}` |")
    return README.replace("{pack_rows}", "\n".join(rows))


# ---------------------------------------------------------------------------------------------- build
def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if isinstance(data, bytes):
        with open(path, "wb") as f:
            f.write(data)
    else:
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(data)


def build(p, kit_dir=kitlib.KIT):
    xb = workbook_bytes(p)
    b64 = base64.b64encode(xb).decode("ascii")
    d = os.path.join(kit_dir, "dataflows")
    write(os.path.join(d, "excel", f"{p['id']}.xlsx"), xb)
    write(os.path.join(d, f"{p['id']}.pq"), pq_text(p, b64))
    write(os.path.join(d, "README.md"), readme(kitlib.all_packs()))
    return d


# ---------------------------------------------------------------------------------------------- checks
TOKEN = re.compile(r'''(?P<ws>\s+)|(?P<lc>//[^\n]*)|(?P<bc>/\*.*?\*/)|(?P<str>"(?:[^"]|"")*")|(?P<qid>\#"(?:[^"]|"")*")|'''
                   r'''(?P<hash>\#[A-Za-z]+)|(?P<num>\d+(?:\.\d+)?)|(?P<id>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)|'''
                   r'''(?P<op>=>|<=|>=|<>|[=<>&+\-*/,;?@!])|(?P<br>[()\[\]{}])''', re.S)


def m_tokens(text):
    toks, i = [], 0
    while i < len(text):
        mt = TOKEN.match(text, i)
        if not mt:
            raise ValueError(f"cannot tokenise at {text[i:i + 30]!r}")
        kind = mt.lastgroup
        if kind not in ("ws", "lc", "bc"):
            toks.append((kind, mt.group()))
        i = mt.end()
    return toks


def check_m(block, known):
    """Light M syntax check: brackets, let/in pairing, one binding per segment, every step name bound."""
    errs = []
    try:
        toks = m_tokens(block)
    except ValueError as e:
        return [str(e)]
    stack, pairs = [], {")": "(", "]": "[", "}": "{"}
    for k, v in toks:
        if v in "([{" and k == "br":
            stack.append(v)
        elif k == "br":
            if not stack or stack.pop() != pairs[v]:
                return [f"unbalanced bracket near {v}"]
    if stack:
        return [f"unclosed {stack}"]
    words = [v for k, v in toks if k == "id"]
    if words.count("let") != words.count("in"):
        errs.append(f"let {words.count('let')} vs in {words.count('in')}")
    # walk let blocks
    bound, inside_field = set(), []
    params = set()
    for j, (k, v) in enumerate(toks):
        if v == "as" and j > 0 and toks[j - 1][0] == "id" and j > 1 and toks[j - 2][1] in ("(", ","):
            params.add(toks[j - 1][1])
    for j, (k, v) in enumerate(toks):
        if k == "id" and v == "let":
            depth, nest, seg, segs = 0, 0, [], []
            for k2, v2 in toks[j + 1:]:
                if k2 == "br" and v2 in "([{":
                    depth += 1
                elif k2 == "br":
                    depth -= 1
                if k2 == "id" and v2 == "let":
                    nest += 1
                if k2 == "id" and v2 == "in":
                    if nest == 0 and depth == 0:
                        break
                    nest -= 1
                if v2 == "," and depth == 0 and nest == 0:
                    segs.append(seg)
                    seg = []
                    continue
                seg.append((k2, v2, depth, nest))
            segs.append(seg)
            for s in segs:
                if len(s) < 3 or s[0][0] not in ("id", "qid") or s[1][1] != "=":
                    errs.append(f"bad binding near {' '.join(x[1] for x in s[:6])!r}")
                    continue
                bound.add(s[0][1])
                for a in range(2, len(s) - 1):
                    if s[a][2] == 0 and s[a][3] == 0 and s[a][0] in ("id", "qid") and s[a + 1][1] == "=" \
                            and s[a - 1][1] not in ("[", ","):
                        errs.append(f"missing comma before {s[a][1]!r}?")
    # every plain identifier outside [...] must be a step, parameter, query, keyword or library member
    depth_sq = 0
    for j, (k, v) in enumerate(toks):
        if v == "[":
            depth_sq += 1
        elif v == "]":
            depth_sq -= 1
        if k != "id" or depth_sq > 0 or "." in v:
            continue
        if v in M_KEYWORDS or v in M_TYPE_WORDS or v in bound or v in params or v in known or v == "_":
            continue
        errs.append(f"unknown name {v!r}")
    if block.count('"') % 2:
        errs.append("odd number of double quotes")
    return sorted(set(errs))


def split_pq(text):
    """{(dataflow number, query name): block text}"""
    out = {}
    parts = re.split(r"^" + re.escape(BAR) + r"\n// DATAFLOW (\d): (\S+)\s+QUERY: (\S+)[^\n]*\n(?:// Data destination[^\n]*\n)?"
                     + re.escape(BAR) + r"\n", text, flags=re.M)
    for i in range(1, len(parts), 4):
        out[(int(parts[i]), parts[i + 2])] = parts[i + 3]
    return out


def replay_reference(xlsx_path):
    """The dataflow 1 steps, in pandas."""
    import pandas as pd
    raw = pd.read_excel(xlsx_path, sheet_name=SHEET, header=None, dtype=object)
    first = raw.index[raw[1].astype(str) == "Code"][0]
    raw = raw.iloc[first:].reset_index(drop=True)                                 # Table.Skip(Sheet, each [Column2] <> "Code")
    raw.columns = [str(c) for c in raw.iloc[0]]                                    # PromoteHeaders
    df = raw.iloc[1:].reset_index(drop=True)
    df = df[df["Code"].notna() & (df["Code"].astype(str).str.strip() != "")].copy()
    df["Code"] = df["Code"].astype(str).str.strip().str.upper()
    df = df.drop_duplicates("Code", keep="first")
    df["target_hours"] = pd.to_numeric(df["Target (hours)"], errors="coerce")
    df["category"] = df["Category"].astype(str).str.strip()
    df["owner_team"] = df["Owner team"].astype(str).str.strip()
    return [(r.Code, r.category, None if pd.isna(r.target_hours) else float(r.target_hours), r.owner_team)
            for r in df.itertuples()]


def check(p, kit_dir=kitlib.KIT):
    errs = []
    nm = names(p)
    d = os.path.join(kit_dir, "dataflows")
    xlsx = os.path.join(d, "excel", f"{p['id']}.xlsx")
    text = open(os.path.join(d, f"{p['id']}.pq"), encoding="utf-8").read()
    blocks = split_pq(text)
    want = [(n, qn) for n, _, qn, *_ in queries(p, "x")]
    if list(blocks) != want:
        errs.append(f"blocks {list(blocks)} expected {want}")
    for (n, qn), block in blocks.items():
        known = {q for (n2, q) in blocks if n2 == n and q != qn}
        for e in check_m(block, known):
            errs.append(f"M {qn}: {e}")
    # embedded workbook = the shipped workbook, and the build is deterministic
    b64 = re.search(r'Binary\.FromText\("([^"]+)"', text).group(1)
    if base64.b64decode(b64) != open(xlsx, "rb").read():
        errs.append("embedded Base64 does not match excel/" + nm["xlsx"])
    if workbook_bytes(p) != open(xlsx, "rb").read():
        errs.append("workbook build is not deterministic")
    got, exp = replay_reference(xlsx), expected_ref(p)
    if got != exp:
        errs.append(f"reference replay {got} expected {exp}")
    if sorted(r[0] for r in got) != sorted(p["event"]["types"]):
        errs.append(f"reference codes {sorted(r[0] for r in got)} do not cover event types {sorted(p['event']['types'])}")
    raw = [r for r in sheet_rows(p) if len(r) > 1 and r[1] is not None]
    if len(raw) - 1 != len(exp) + 1 or not any(r[3] == "TBC" for r in raw):
        errs.append("workbook is missing the duplicate or the TBC cell")
    # columns used by the Gold query exist in Silver (column map, PII drops removed, framework columns)
    cr = kitlib.control_rows(p)
    sv = {}
    for o in cr["ingestion_object"]:
        sv[o["silver_table"]] = {m["target_column"] for m in cr["column_map"] if m["object_id"] == o["object_id"]
                                 and m["pii_treatment"] != "drop"} | {"_is_deleted", "_is_current"}
    gold = blocks.get((2, nm["gold_table"]), "")
    for tbl, cols in re.findall(r'fnLakehouseTable\("silver", "(\w+)"\)[^{]*\{([^}]*)\}', gold):
        if tbl == nm["ref_table"]:
            continue
        for c in re.findall(r'"(\w+)"', cols):
            if c not in sv.get(tbl, set()):
                errs.append(f"gold query reads {tbl}.{c}, which is not in Silver")
    for c in re.findall(r'Table\.SelectColumns\(Live, \{([^}]*)\}', gold):
        for col in re.findall(r'"(\w+)"', c):
            if col not in sv[p["event"]["table"]]:
                errs.append(f"gold query reads {p['event']['table']}.{col}, which is not in Silver")
    for path in (xlsx, os.path.join(d, f"{p['id']}.pq")):
        rel = os.path.relpath(path, kit_dir).replace(os.sep, "/")
        if len(rel) >= 80:
            errs.append(f"path too long: {rel}")
    return errs, got


def main(argv):
    do_check = "--check" in argv
    only = [a for a in argv if not a.startswith("--")]
    bad = 0
    for p in kitlib.all_packs():
        if only and p["id"] not in only:
            continue
        build(p)
        msg = f"{p['id']:14s} -> dataflows/{p['id']}.pq, dataflows/excel/{p['id']}.xlsx"
        if do_check:
            e, rows = check(p)
            tbc = [r[0] for r in rows if r[2] is None]
            msg += f" | {len(rows)} reference rows, TBC {tbc} | {len(e)} problem(s)"
            for x in e:
                msg += "\n    " + x
            bad += len(e)
        print(msg)
    if do_check:
        print("DATAFLOW CHECKS PASSED" if not bad else f"DATAFLOW CHECKS FAILED ({bad})")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
