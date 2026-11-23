# AI demo – Healthcare: Coastal Health Network

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Coastal Health Network Gold layer. It's a 15-minute build once the framework has run. I use it with patient flow and clinical operations audiences (state health departments, local health districts, private hospital groups, aged care).

Coastal Health Network is fictional: a public health service running five hospitals. Its wards sit in `gold.dim_ward`, its admissions in `gold.fact_admission`, and ward occupancy (15-min) in `gold.fact_ward_occupancy_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `CHN_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `CHN_Ops_Agent` | 4 min |
| 3 | Ontology `CHN_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "healthcare"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.ward_health_scorecard;   -- about 121
   SELECT COUNT(*) FROM gold.fact_admission;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `CHN_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `CHN_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_ward` | One row per ward |
   | `fact_admission` | One row per admission |
   | `fact_ward_occupancy_hourly` | Hourly roll-up of ward occupancy (15-min), one row per ward per hour |
   | `ward_health_scorecard` | Risk score, band and access-block cost at risk (AUD) per ward |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_admission[ward_id]` | `dim_ward[ward_id]` |
   | `fact_admission[opened_date_key]` | `dim_date[date_key]` |
   | `fact_ward_occupancy_hourly[ward_id]` | `dim_ward[ward_id]` |
   | `fact_ward_occupancy_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `ward_health_scorecard[ward_id]` | `dim_ward[ward_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_admission`, then **New measure** for each:

   ```dax
   Admissions = COUNTROWS(fact_admission)
   Open Admissions = CALCULATE(COUNTROWS(fact_admission), fact_admission[is_open] = 1)
   Emergency Admissions = CALCULATE(COUNTROWS(fact_admission), fact_admission[admission_type] = "EMERGENCY")
   Avg Hours to Close = AVERAGE(fact_admission[hours_to_close])
   Total Episode Cost (AUD) = SUM(fact_admission[episode_cost_aud])
   Breach Readings = SUM(fact_ward_occupancy_hourly[breach_readings])
   High Risk Wards = CALCULATE(COUNTROWS(ward_health_scorecard), ward_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(ward_health_scorecard[risk_score])
   Access-block Cost At Risk (AUD) = SUM(ward_health_scorecard[access_block_cost_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `access_block_cost_aud` is an illustrative dollar figure: $1,500 per open admission + $2,000 per 15-min window above 95% occupancy + beds x acuity x $500. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `ward_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `ward_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Coastal Health Network, a fictional public health service running five hospitals.
     A ward is identified by ward_code. Hospital is in dim_ward[hospital].
     Acuity level is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open admission has is_open = 1. 'Emergency' means admission_type = 'EMERGENCY'.
     A breach is a bed occupancy reading above 95 %.
     Access-block Cost At Risk is ward_health_scorecard[access_block_cost_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Admissions* by `hospital`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open admissions by hospital*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `CHN_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `CHN_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_ward`, `fact_admission`, `fact_ward_occupancy_hourly`, `ward_health_scorecard`, `patient_summary`
   - Semantic model **CHN_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the patient flow and clinical operations team at Coastal Health Network, a fictional public health service running five hospitals.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual wards or risk scores.
   Hospital is the column hospital. Acuity level is 1-5; 'critical' means 4 or 5.
   Open admissions: is_open = 1. Emergency admissions: admission_type = 'EMERGENCY'.
   A breach means occupancy_pct above 95 (%). Use breach_readings in fact_ward_occupancy_hourly.
   'Last N days' is relative to the latest reading_date in fact_ward_occupancy_hourly, not today's date - the data is a demo snapshot.
   Patients are only available as aggregates in patient_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   ward_health_scorecard: one row per ward, risk_score 0-100, risk_band High/Medium/Low, open_admissions, breach_readings, access_block_cost_aud (illustrative access-block cost at risk in AUD - always call it an estimate).
   fact_admission: one row per admission; hours_to_close is NULL while open; hospital is copied onto each row.
   fact_ward_occupancy_hourly: hourly; join to dim_ward on ward_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 wards have the highest access-block cost at risk?**
   ```sql
   SELECT TOP 10 ward_code, ward_name, hospital, access_block_cost_aud, risk_score, risk_band, open_admissions
   FROM gold.ward_health_scorecard
   ORDER BY access_block_cost_aud DESC;
   ```
   **Average hours to close EMERGENCY admissions by hospital**
   ```sql
   SELECT hospital, COUNT(*) AS admissions, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_admission
   WHERE admission_type = 'EMERGENCY' AND hours_to_close IS NOT NULL
   GROUP BY hospital
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which wards breached the bed occupancy threshold in the last 3 days?**
   ```sql
   SELECT d.ward_code, d.ward_name, d.hospital, SUM(f.breach_readings) AS breach_readings, MAX(f.max_occupancy_pct) AS max_occupancy_pct
   FROM gold.fact_ward_occupancy_hourly AS f
   JOIN gold.dim_ward AS d ON d.ward_id = f.ward_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_ward_occupancy_hourly))
   GROUP BY d.ward_code, d.ward_name, d.hospital
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total episode_cost_aud by admission_type this financial year**
   ```sql
   SELECT admission_type, COUNT(*) AS admissions, SUM(episode_cost_aud) AS total_episode_cost_aud
   FROM gold.fact_admission
   WHERE opened_date >= '2026-07-01'
   GROUP BY admission_type
   ORDER BY total_episode_cost_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `CHN_Ontology` (4 min, preview)

1. Open the semantic model `CHN_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `CHN_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_ward` | **Ward** | `ward_id` (display: `ward_name`) |
   | `fact_admission` | **Admission** | `admission_id` |
   | `ward_health_scorecard` | **WardRisk** | `ward_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **Admission → raised against → Ward**, **Ward → has risk → WardRisk**.
5. Add the people side: **New entity type** `Patient` ▸ bind to lakehouse **lh_mdf** table `silver.patient` ▸ key `patient_id` ▸ keep `funding_type`, `hospital`, `status`, `patient_id`. Then add the relationship **Patient → linked to → Ward** on `ward_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **Ward**, add a time-series property bound to `gold.fact_ward_occupancy_hourly` with timestamp `reading_hour`, key `ward_id` and value `max_occupancy_pct`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk ward. This is the visual moment.
8. Add the ontology as a source to `CHN_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which hospital had the most emergency admissions this month?
- Show average length of stay by admission type.
- Which high-acuity wards have more than 10 patients currently admitted?
- Build a report page with Open Admissions by hospital and a monthly trend.

**Data agent `CHN_Ops_Agent`:**
- Which 10 wards have the highest risk score and how many patients are currently admitted?
- Compare average length of stay for emergency admissions by hospital.
- Which wards went above 95% occupancy in the last 3 days?
- What did elective admissions cost in Newcastle?
- How many DVA patients are active per hospital?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show ICU wards in Gosford with emergency admissions still open and a high risk band.
- For the ward with the highest occupancy, list current admissions and their episode cost.
- Which patients are linked to wards that are partially closed?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.ward_health_scorecard and plot risk_score by hospital.*
- In the lakehouse SQL endpoint: *Write a query that finds wards with more than 3 open admissions.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from patient to ward to admission.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Access-block Cost At Risk* turns a risk list into a business case. The rates are made up for the demo ($1,500 per open admission + $2,000 per 15-min window above 95% occupancy + beds x acuity x $500) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

