# Power BI report for any pack

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

`build_pbi.py` writes a Power BI project (PBIP) for the pack you pick: a TMDL semantic model and a PBIR
report, all plain text. It sits on the Gold layer of `lh_mdf`, so it closes the demo loop: source files >
Landing > Bronze > Silver > Gold > a report the business would recognise.

The project isn't shipped in the kit. You generate it, because it's named and shaped from the pack
definition, and it stays in step with the Gold SQL when a pack changes.

## Generate it

Run from the kit root. Python 3 with the standard library is enough.

```
python powerbi/build_pbi.py --pack retail
python powerbi/build_pbi.py --pack energy --out C:/pbi/energy
python powerbi/build_pbi.py --pack telco --offline
```

| Option | What it does |
|---|---|
| `--pack` | Pack id: energy, telco, fsi, manufacturing, healthcare, ites, government, retail, digital |
| `--out` | Output folder. Default `./pbi/<pack>`. Keep it short: Windows still trips over long paths |
| `--offline` | Builds Gold on your laptop and points the model at CSVs (see below) |

The project is named from the pack's short code, for example `WCO_Ops` for Wattle & Co:

```
pbi/retail/
  WCO_Ops.pbip              open this in Power BI Desktop
  WCO_Ops.SemanticModel/    TMDL: 5 tables, 10 measures, relationships, RLS role
  WCO_Ops.Report/           PBIR: 3 pages, one JSON file per visual
  gold_csv/                 only with --offline
```

Running it again replaces the two project folders. Commit any hand edits to Git first.

## What's in the model

Five Gold tables from the `gold` schema of `lh_mdf`:

| Table | Grain |
|---|---|
| `dim_date` | One row per day, 2020 to 2028, with Australian financial year |
| `dim_<entity>` | Current version of each entity (asset, store, ward...) |
| `fact_<event>` | One row per event (work order, stock issue, admission...) |
| `fact_<reading>_hourly` | Readings per entity per hour, with threshold breaches |
| `<entity>_health_scorecard` | Risk score, band and the pack's illustrative dollar impact per entity |

- **Relationships.** Both facts and the scorecard relate to `dim_<entity>` on the entity id. The event fact
  relates to `dim_date` on `opened_date_key`, the hourly fact on `reading_date_key`. All single direction.
- **Measures.** Ten measures in a *Key Measures* table, named from the pack. For energy: Work Orders, Open
  Work Orders, Emergency Work Orders, Avg Hours to Complete, Cost (AUD), Breach Readings, Breach Rate %,
  Peak Oil Temperature, High Risk Assets, Avg Risk Score, Outage Cost At Risk (AUD) – each pack gets its own dollar measure. Implicit measures are off.
- **Row-level security.** One role, `<first region> only`, filters the entity dimension and the scorecard
  to that region. The facts follow through the relationships. Try it with *Modeling > View as*.
- **Parameters.** `DataSourceMode`, `SqlEndpoint`, `Database` (default `lh_mdf`) and `CsvFolder`. A helper
  function, `fnGold`, keeps every table query to one line.

## Report pages

| Page | What's on it |
|---|---|
| Overview | Six KPI cards, events by region (bar), events and value by month |
| `<Entity>` risk | Risk and breach cards, highest-risk entities table, breaches by type and by hour |
| `<Event>` detail | Event cards, events by type, hours to close by status, a detail table |

Every page has slicers. Pages 1 and 2 slice on region and entity type, page 3 on event type and status.

## Open it against Fabric

Do this after the kit has built Gold in `lh_mdf` (the pipeline or `nb_mdf_90_run_all`).

1. Use a current Power BI Desktop. PBIR is the default report format now.
2. Open `<short>_Ops.pbip`.
3. *Transform data > Edit parameters*. Set **SqlEndpoint** to the lakehouse SQL analytics endpoint. You'll
   find it under Lakehouse > Settings > SQL analytics endpoint. Leave **Database** as `lh_mdf` unless you
   renamed the lakehouse.
4. Refresh and sign in with your organisational account.
5. Publish (*Home > Publish*) to the workspace. In the service, set the credentials for the semantic model
   and assign people or groups to the RLS role.

This is Import mode, refreshed from the SQL analytics endpoint. Schedule a refresh after the nightly run.

## Offline (no Fabric)

`--offline` runs the pack's Bronze > Silver > Gold flow on your laptop (`tools/local_check.py`, same Gold
SQL, in SQLite). It writes the five tables to `gold_csv/` next to the project and sets `DataSourceMode` to
`CSV`, with `CsvFolder` pointing at that folder. Open the `.pbip` and refresh.

If you move the folder, update **CsvFolder** and keep the trailing separator. If Desktop asks about privacy
levels for the local files, choose *Organizational*. To switch to Fabric later, set **DataSourceMode** to
`Fabric` and fill in **SqlEndpoint**.

## Or build a Direct Lake model in the service

For anything beyond a demo, Direct Lake is the better choice. There's no import refresh, and it reads the
Delta tables in OneLake directly.

1. In the lakehouse, choose *New semantic model* and tick the five Gold tables in the `gold` schema.
2. Create the five relationships listed above.
3. Copy the measures across. Open `definition/tables/Key Measures.tmdl` from the generated project and
   paste each DAX expression, or use the TMDL view in the web modelling editor.
4. Add the RLS role from `definition/roles/`.
5. Point the report at it. Publish the generated report, then rebind it to the Direct Lake model with
   the Rebind Report REST API, or `report_rebind` from semantic-link-labs in a notebook. Measure, table
   and column names must match, which they will if you kept them.

## Notes

- Open and refresh the project once in Desktop before a customer session. If Desktop flags a formatting
  property, it usually repairs it on save.
- Risk scores, bands and thresholds are teaching examples, not operating rules.
