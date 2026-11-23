# Dataflow Gen2

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

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
| 2 | `AsAtDate` | `2026-11-04` | The reporting date, `yyyy-MM-dd`. Open events are aged to the end of this day |

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
| energy | Work order response targets (`excel/energy.xlsx`) | `silver.ref_work_order_target` | `gold.work_order_target_compliance` | `INSP` |
| telco | Fault restoration targets (`excel/telco.xlsx`) | `silver.ref_fault_ticket_target` | `gold.fault_ticket_target_compliance` | `HARDWARE` |
| fsi | Risk case handling targets (`excel/fsi.xlsx`) | `silver.ref_risk_case_target` | `gold.risk_case_target_compliance` | `ACCOUNT_TAKEOVER` |
| manufacturing | Maintenance response targets (`excel/manufacturing.xlsx`) | `silver.ref_maintenance_order_target` | `gold.maintenance_order_target_compliance` | `CALIBRATION` |
| healthcare | Length-of-stay targets (`excel/healthcare.xlsx`) | `silver.ref_admission_target` | `gold.admission_target_compliance` | `TRANSFER` |
| ites | Incident resolution SLAs (`excel/ites.xlsx`) | `silver.ref_incident_target` | `gold.incident_target_compliance` | `SECURITY` |
| government | Citizen case service standards (`excel/government.xlsx`) | `silver.ref_citizen_case_target` | `gold.citizen_case_target_compliance` | `PUBLIC_SAFETY` |
| retail | Stock issue response targets (`excel/retail.xlsx`) | `silver.ref_stock_issue_target` | `gold.stock_issue_target_compliance` | `COLD_CHAIN` |
| digital | Support SLAs by ticket type (`excel/digital.xlsx`) | `silver.ref_support_ticket_target` | `gold.support_ticket_target_compliance` | `BILLING` |

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
