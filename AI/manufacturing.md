# AI demo – Manufacturing: Kestrel Components

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Kestrel Components Gold layer. It's a 15-minute build once the framework has run. I use it with plant operations and maintenance audiences (discrete and process manufacturers, including food and beverage).

Kestrel Components is fictional: a automotive and agricultural components manufacturer. Its machines sit in `gold.dim_machine`, its maintenance orders in `gold.fact_maintenance_order`, and machine telemetry in `gold.fact_machine_telemetry_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `KCO_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `KCO_Ops_Agent` | 4 min |
| 3 | Ontology `KCO_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "manufacturing"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.machine_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_maintenance_order;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `KCO_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `KCO_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_machine` | One row per machine |
   | `fact_maintenance_order` | One row per maintenance order |
   | `fact_machine_telemetry_hourly` | Hourly roll-up of machine telemetry, one row per machine per hour |
   | `machine_health_scorecard` | Risk score, band and downtime cost at risk (AUD) per machine |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_maintenance_order[machine_id]` | `dim_machine[machine_id]` |
   | `fact_maintenance_order[opened_date_key]` | `dim_date[date_key]` |
   | `fact_machine_telemetry_hourly[machine_id]` | `dim_machine[machine_id]` |
   | `fact_machine_telemetry_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `machine_health_scorecard[machine_id]` | `dim_machine[machine_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_maintenance_order`, then **New measure** for each:

   ```dax
   Maintenance Orders = COUNTROWS(fact_maintenance_order)
   Open Maintenance Orders = CALCULATE(COUNTROWS(fact_maintenance_order), fact_maintenance_order[is_open] = 1)
   Breakdown Maintenance Orders = CALCULATE(COUNTROWS(fact_maintenance_order), fact_maintenance_order[mo_type] = "BREAKDOWN")
   Avg Hours to Close = AVERAGE(fact_maintenance_order[hours_to_close])
   Total Parts Cost (AUD) = SUM(fact_maintenance_order[parts_cost_aud])
   Breach Readings = SUM(fact_machine_telemetry_hourly[breach_readings])
   High Risk Machines = CALCULATE(COUNTROWS(machine_health_scorecard), machine_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(machine_health_scorecard[risk_score])
   Downtime Cost At Risk (AUD) = SUM(machine_health_scorecard[downtime_cost_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `downtime_cost_at_risk_aud` is an illustrative dollar figure: $5,000 per open maintenance order + $3,000 per 15-min window above the vibration limit + rated units/hour x criticality x $2. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `machine_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `machine_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Kestrel Components, a fictional automotive and agricultural components manufacturer.
     A machine is identified by machine_code. Plant is in dim_machine[plant].
     Criticality is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open maintenance order has is_open = 1. 'Breakdown' means mo_type = 'BREAKDOWN'.
     A breach is a vibration reading above 7.1 mm/s.
     Downtime Cost At Risk is machine_health_scorecard[downtime_cost_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Maintenance Orders* by `plant`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open maintenance orders by plant*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `KCO_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `KCO_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_machine`, `fact_maintenance_order`, `fact_machine_telemetry_hourly`, `machine_health_scorecard`, `distributor_contact_summary`
   - Semantic model **KCO_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the plant operations and maintenance team at Kestrel Components, a fictional automotive and agricultural components manufacturer.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual machines or risk scores.
   Plant is the column plant. Criticality is 1-5; 'critical' means 4 or 5.
   Open maintenance orders: is_open = 1. Breakdown maintenance orders: mo_type = 'BREAKDOWN'.
   A breach means vibration_mm_s above 7.1 (mm/s). Use breach_readings in fact_machine_telemetry_hourly.
   'Last N days' is relative to the latest reading_date in fact_machine_telemetry_hourly, not today's date - the data is a demo snapshot.
   Distributor contacts are only available as aggregates in distributor_contact_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   machine_health_scorecard: one row per machine, risk_score 0-100, risk_band High/Medium/Low, open_maintenance_orders, breach_readings, downtime_cost_at_risk_aud (illustrative downtime cost at risk in AUD - always call it an estimate).
   fact_maintenance_order: one row per maintenance order; hours_to_close is NULL while open; plant is copied onto each row.
   fact_machine_telemetry_hourly: hourly; join to dim_machine on machine_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 machines have the highest downtime cost at risk?**
   ```sql
   SELECT TOP 10 machine_code, machine_name, plant, downtime_cost_at_risk_aud, risk_score, risk_band, open_maintenance_orders
   FROM gold.machine_health_scorecard
   ORDER BY downtime_cost_at_risk_aud DESC;
   ```
   **Average hours to close BREAKDOWN maintenance orders by plant**
   ```sql
   SELECT plant, COUNT(*) AS maintenance_orders, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_maintenance_order
   WHERE mo_type = 'BREAKDOWN' AND hours_to_close IS NOT NULL
   GROUP BY plant
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which machines breached the vibration threshold in the last 3 days?**
   ```sql
   SELECT d.machine_code, d.machine_name, d.plant, SUM(f.breach_readings) AS breach_readings, MAX(f.max_vibration_mm_s) AS max_vibration_mm_s
   FROM gold.fact_machine_telemetry_hourly AS f
   JOIN gold.dim_machine AS d ON d.machine_id = f.machine_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_machine_telemetry_hourly))
   GROUP BY d.machine_code, d.machine_name, d.plant
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total parts_cost_aud by mo_type this financial year**
   ```sql
   SELECT mo_type, COUNT(*) AS maintenance_orders, SUM(parts_cost_aud) AS total_parts_cost_aud
   FROM gold.fact_maintenance_order
   WHERE opened_date >= '2026-07-01'
   GROUP BY mo_type
   ORDER BY total_parts_cost_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `KCO_Ontology` (4 min, preview)

1. Open the semantic model `KCO_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `KCO_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_machine` | **Machine** | `machine_id` (display: `machine_name`) |
   | `fact_maintenance_order` | **MaintenanceOrder** | `mo_id` |
   | `machine_health_scorecard` | **MachineRisk** | `machine_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **MaintenanceOrder → raised against → Machine**, **Machine → has risk → MachineRisk**.
5. Add the people side: **New entity type** `DistributorContact` ▸ bind to lakehouse **lh_mdf** table `silver.distributor_contact` ▸ key `contact_id` ▸ keep `channel`, `plant`, `status`, `contact_id`. Then add the relationship **DistributorContact → linked to → Machine** on `machine_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **Machine**, add a time-series property bound to `gold.fact_machine_telemetry_hourly` with timestamp `reading_hour`, key `machine_id` and value `max_vibration_mm_s`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk machine. This is the visual moment.
8. Add the ontology as a source to `KCO_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which plant had the most breakdown hours this month?
- Show parts cost by machine type and month.
- Which bottleneck machines (criticality 4 or 5) have open maintenance orders?
- Build a report page with Open Maintenance Orders by plant and a monthly trend.

**Data agent `KCO_Ops_Agent`:**
- Which 10 machines have the highest risk score and how many open maintenance orders does each have?
- Compare average hours to finish breakdowns by plant.
- Which machines exceeded 7.1 mm/s vibration in the last 3 days?
- What was the total downtime caused by breakdowns in Geelong?
- How many OEM contacts are active per plant?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show injection moulders in Dandenong with an open breakdown and a high risk band.
- For the machine with the highest vibration, list its maintenance orders and their parts cost.
- Which distributor contacts are linked to machines currently in maintenance?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.machine_health_scorecard and plot risk_score by plant.*
- In the lakehouse SQL endpoint: *Write a query that finds machines with more than 3 open maintenance orders.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from distributor contact to machine to maintenance order.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Downtime Cost At Risk* turns a risk list into a business case. The rates are made up for the demo ($5,000 per open maintenance order + $3,000 per 15-min window above the vibration limit + rated units/hour x criticality x $2) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

