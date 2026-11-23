# AI demo – Government: Riverbend Regional Authority

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Riverbend Regional Authority Gold layer. It's a 15-minute build once the framework has run. I use it with public sector operations and citizen services audiences (state agencies, local councils, transport, water and emergency services).

Riverbend Regional Authority is fictional: a government authority delivering local infrastructure and citizen services (fits a council or a state agency). Its public assets sit in `gold.dim_public_asset`, its citizen cases in `gold.fact_citizen_case`, and smart infrastructure sensor readings in `gold.fact_sensor_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `RRA_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `RRA_Ops_Agent` | 4 min |
| 3 | Ontology `RRA_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "government"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.public_asset_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_citizen_case;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `RRA_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `RRA_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_public_asset` | One row per public asset |
   | `fact_citizen_case` | One row per citizen case |
   | `fact_sensor_hourly` | Hourly roll-up of smart infrastructure sensor readings, one row per public asset per hour |
   | `public_asset_health_scorecard` | Risk score, band and cost at risk (AUD) per public asset |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_citizen_case[asset_id]` | `dim_public_asset[asset_id]` |
   | `fact_citizen_case[opened_date_key]` | `dim_date[date_key]` |
   | `fact_sensor_hourly[asset_id]` | `dim_public_asset[asset_id]` |
   | `fact_sensor_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `public_asset_health_scorecard[asset_id]` | `dim_public_asset[asset_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_citizen_case`, then **New measure** for each:

   ```dax
   Citizen Cases = COUNTROWS(fact_citizen_case)
   Open Citizen Cases = CALCULATE(COUNTROWS(fact_citizen_case), fact_citizen_case[is_open] = 1)
   Flooding And Drainage Citizen Cases = CALCULATE(COUNTROWS(fact_citizen_case), fact_citizen_case[case_type] = "FLOOD_DRAINAGE")
   Avg Hours to Close = AVERAGE(fact_citizen_case[hours_to_close])
   Total Cost (AUD) = SUM(fact_citizen_case[cost_aud])
   Breach Readings = SUM(fact_sensor_hourly[breach_readings])
   High Risk Public Assets = CALCULATE(COUNTROWS(public_asset_health_scorecard), public_asset_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(public_asset_health_scorecard[risk_score])
   Cost At Risk (AUD) = SUM(public_asset_health_scorecard[cost_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `cost_at_risk_aud` is an illustrative dollar figure: $2,500 per open case + $1,200 per 15-min window above the water-level threshold + replacement value (k) x criticality x $0.50. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `asset_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `public_asset_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Riverbend Regional Authority, a fictional government authority delivering local infrastructure and citizen services (fits a council or a state agency).
     A public asset is identified by asset_ref. District is in dim_public_asset[district].
     Criticality is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open citizen case has is_open = 1. 'Flooding And Drainage' means case_type = 'FLOOD_DRAINAGE'.
     A breach is a water level reading above 150 cm.
     Cost At Risk is public_asset_health_scorecard[cost_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Citizen Cases* by `district`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open citizen cases by district*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `RRA_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `RRA_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_public_asset`, `fact_citizen_case`, `fact_sensor_hourly`, `public_asset_health_scorecard`, `citizen_summary`
   - Semantic model **RRA_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the public sector operations and citizen services team at Riverbend Regional Authority, a fictional government authority delivering local infrastructure and citizen services (fits a council or a state agency).
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual public assets or risk scores.
   District is the column district. Criticality is 1-5; 'critical' means 4 or 5.
   Open citizen cases: is_open = 1. Flood Drainage citizen cases: case_type = 'FLOOD_DRAINAGE'.
   A breach means water_level_cm above 150 (cm). Use breach_readings in fact_sensor_hourly.
   'Last N days' is relative to the latest reading_date in fact_sensor_hourly, not today's date - the data is a demo snapshot.
   Citizens are only available as aggregates in citizen_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   public_asset_health_scorecard: one row per public asset, risk_score 0-100, risk_band High/Medium/Low, open_citizen_cases, breach_readings, cost_at_risk_aud (illustrative cost at risk in AUD - always call it an estimate).
   fact_citizen_case: one row per citizen case; hours_to_close is NULL while open; district is copied onto each row.
   fact_sensor_hourly: hourly; join to dim_public_asset on asset_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 public assets have the highest cost at risk?**
   ```sql
   SELECT TOP 10 asset_ref, asset_name, district, cost_at_risk_aud, risk_score, risk_band, open_citizen_cases
   FROM gold.public_asset_health_scorecard
   ORDER BY cost_at_risk_aud DESC;
   ```
   **Average hours to close FLOOD_DRAINAGE citizen cases by district**
   ```sql
   SELECT district, COUNT(*) AS citizen_cases, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_citizen_case
   WHERE case_type = 'FLOOD_DRAINAGE' AND hours_to_close IS NOT NULL
   GROUP BY district
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which public assets breached the water level threshold in the last 3 days?**
   ```sql
   SELECT d.asset_ref, d.asset_name, d.district, SUM(f.breach_readings) AS breach_readings, MAX(f.max_water_level_cm) AS max_water_level_cm
   FROM gold.fact_sensor_hourly AS f
   JOIN gold.dim_public_asset AS d ON d.asset_id = f.asset_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_sensor_hourly))
   GROUP BY d.asset_ref, d.asset_name, d.district
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total cost_aud by case_type this financial year**
   ```sql
   SELECT case_type, COUNT(*) AS citizen_cases, SUM(cost_aud) AS total_cost_aud
   FROM gold.fact_citizen_case
   WHERE opened_date >= '2026-07-01'
   GROUP BY case_type
   ORDER BY total_cost_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `RRA_Ontology` (4 min, preview)

1. Open the semantic model `RRA_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `RRA_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_public_asset` | **PublicAsset** | `asset_id` (display: `asset_name`) |
   | `fact_citizen_case` | **CitizenCase** | `case_id` |
   | `public_asset_health_scorecard` | **PublicAssetRisk** | `asset_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **CitizenCase → raised against → PublicAsset**, **PublicAsset → has risk → PublicAssetRisk**.
5. Add the people side: **New entity type** `Citizen` ▸ bind to lakehouse **lh_mdf** table `silver.citizen` ▸ key `citizen_id` ▸ keep `citizen_type`, `district`, `status`, `citizen_id`. Then add the relationship **Citizen → linked to → PublicAsset** on `asset_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **PublicAsset**, add a time-series property bound to `gold.fact_sensor_hourly` with timestamp `reading_hour`, key `asset_id` and value `max_water_level_cm`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk public asset. This is the visual moment.
8. Add the ontology as a source to `RRA_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which district lodged the most citizen cases this month?
- Show citizen case cost by case type and month.
- Which critical assets have open flooding cases?
- Build a report page with Open Citizen Cases by district and a monthly trend.

**Data agent `RRA_Ops_Agent`:**
- Which 10 public assets have the highest cost at risk, and how many open cases does each have?
- Compare average hours to complete flooding cases by district against the 4-hour standard.
- Which stormwater pits went above 150 cm water level in the last 3 days?
- How many permit applications are still open and how long have they been waiting?
- How many concession holders lodged cases in each district?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show bridges in East District with an open case and a high risk band.
- For the stormwater pit with the highest water level, list its citizen cases and their cost.
- Which citizens are linked to assets that are closed?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.public_asset_health_scorecard and plot risk_score by district.*
- In the lakehouse SQL endpoint: *Write a query that finds public assets with more than 3 open citizen cases.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from citizen to public asset to citizen case.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Cost At Risk* turns a risk list into a business case. The rates are made up for the demo ($2,500 per open case + $1,200 per 15-min window above the water-level threshold + replacement value (k) x criticality x $0.50) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

