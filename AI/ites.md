# AI demo – IT & ITeS: Bluegum Digital Services

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Bluegum Digital Services Gold layer. It's a 15-minute build once the framework has run. I use it with service delivery, SLA and account management audiences (IT services providers, managed service providers and internal IT teams).

Bluegum Digital Services is fictional: a IT services provider running managed services for enterprise clients. Its client services sit in `gold.dim_client_service`, its incidents in `gold.fact_incident`, and service performance (15-min) in `gold.fact_service_perf_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `BDS_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `BDS_Ops_Agent` | 4 min |
| 3 | Ontology `BDS_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "ites"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.client_service_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_incident;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `BDS_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `BDS_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_client_service` | One row per client service |
   | `fact_incident` | One row per incident |
   | `fact_service_perf_hourly` | Hourly roll-up of service performance (15-min), one row per client service per hour |
   | `client_service_health_scorecard` | Risk score, band and contract revenue at risk (AUD) per client service |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_incident[service_id]` | `dim_client_service[service_id]` |
   | `fact_incident[opened_date_key]` | `dim_date[date_key]` |
   | `fact_service_perf_hourly[service_id]` | `dim_client_service[service_id]` |
   | `fact_service_perf_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `client_service_health_scorecard[service_id]` | `dim_client_service[service_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_incident`, then **New measure** for each:

   ```dax
   Incidents = COUNTROWS(fact_incident)
   Open Incidents = CALCULATE(COUNTROWS(fact_incident), fact_incident[is_open] = 1)
   Outage Incidents = CALCULATE(COUNTROWS(fact_incident), fact_incident[category] = "OUTAGE")
   Avg Hours to Close = AVERAGE(fact_incident[hours_to_close])
   Total Sla Penalty (AUD) = SUM(fact_incident[sla_penalty_aud])
   Breach Readings = SUM(fact_service_perf_hourly[breach_readings])
   High Risk Client Services = CALCULATE(COUNTROWS(client_service_health_scorecard), client_service_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(client_service_health_scorecard[risk_score])
   Contract Revenue At Risk (AUD) = SUM(client_service_health_scorecard[revenue_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `revenue_at_risk_aud` is an illustrative dollar figure: $2,000 SLA penalty exposure per open incident + $500 per slow 15-min window + monthly fee x SLA tier x 1%. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `service_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `client_service_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Bluegum Digital Services, a fictional IT services provider running managed services for enterprise clients.
     A client service is identified by contract_ref. Delivery centre is in dim_client_service[delivery_centre].
     Sla tier is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open incident has is_open = 1. 'Outage' means category = 'OUTAGE'.
     A breach is a p95 response time reading above 2500 ms.
     Contract Revenue At Risk is client_service_health_scorecard[revenue_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Incidents* by `delivery_centre`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open incidents by delivery centre*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `BDS_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `BDS_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_client_service`, `fact_incident`, `fact_service_perf_hourly`, `client_service_health_scorecard`, `client_contact_summary`
   - Semantic model **BDS_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the service delivery, SLA and account management team at Bluegum Digital Services, a fictional IT services provider running managed services for enterprise clients.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual client services or risk scores.
   Delivery centre is the column delivery_centre. Sla tier is 1-5; 'critical' means 4 or 5.
   Open incidents: is_open = 1. Outage incidents: category = 'OUTAGE'.
   A breach means p95_response_ms above 2500 (ms). Use breach_readings in fact_service_perf_hourly.
   'Last N days' is relative to the latest reading_date in fact_service_perf_hourly, not today's date - the data is a demo snapshot.
   Client contacts are only available as aggregates in client_contact_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   client_service_health_scorecard: one row per client service, risk_score 0-100, risk_band High/Medium/Low, open_incidents, breach_readings, revenue_at_risk_aud (illustrative contract revenue at risk in AUD - always call it an estimate).
   fact_incident: one row per incident; hours_to_close is NULL while open; delivery centre is copied onto each row.
   fact_service_perf_hourly: hourly; join to dim_client_service on service_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 client services have the highest contract revenue at risk?**
   ```sql
   SELECT TOP 10 contract_ref, service_name, delivery_centre, revenue_at_risk_aud, risk_score, risk_band, open_incidents
   FROM gold.client_service_health_scorecard
   ORDER BY revenue_at_risk_aud DESC;
   ```
   **Average hours to close OUTAGE incidents by delivery centre**
   ```sql
   SELECT delivery_centre, COUNT(*) AS incidents, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_incident
   WHERE category = 'OUTAGE' AND hours_to_close IS NOT NULL
   GROUP BY delivery_centre
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which client services breached the p95 response time threshold in the last 3 days?**
   ```sql
   SELECT d.contract_ref, d.service_name, d.delivery_centre, SUM(f.breach_readings) AS breach_readings, MAX(f.max_p95_response_ms) AS max_p95_response_ms
   FROM gold.fact_service_perf_hourly AS f
   JOIN gold.dim_client_service AS d ON d.service_id = f.service_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_service_perf_hourly))
   GROUP BY d.contract_ref, d.service_name, d.delivery_centre
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total sla_penalty_aud by category this financial year**
   ```sql
   SELECT category, COUNT(*) AS incidents, SUM(sla_penalty_aud) AS total_sla_penalty_aud
   FROM gold.fact_incident
   WHERE opened_date >= '2026-07-01'
   GROUP BY category
   ORDER BY total_sla_penalty_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `BDS_Ontology` (4 min, preview)

1. Open the semantic model `BDS_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `BDS_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_client_service` | **ClientService** | `service_id` (display: `service_name`) |
   | `fact_incident` | **Incident** | `incident_id` |
   | `client_service_health_scorecard` | **ClientServiceRisk** | `service_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **Incident → raised against → ClientService**, **ClientService → has risk → ClientServiceRisk**.
5. Add the people side: **New entity type** `ClientContact` ▸ bind to lakehouse **lh_mdf** table `silver.client_contact` ▸ key `contact_id` ▸ keep `contact_role`, `delivery_centre`, `status`, `contact_id`. Then add the relationship **ClientContact → linked to → ClientService** on `service_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **ClientService**, add a time-series property bound to `gold.fact_service_perf_hourly` with timestamp `reading_hour`, key `service_id` and value `max_p95_response_ms`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk client service. This is the visual moment.
8. Add the ontology as a source to `BDS_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which delivery centre had the most outages this month?
- Show SLA penalties by incident category and month.
- Which platinum client services (SLA tier 4 or 5) have open incidents?
- Build a report page with Open Incidents by delivery centre and a monthly trend.

**Data agent `BDS_Ops_Agent`:**
- Which 10 client services have the most contract revenue at risk, and how many open incidents does each have?
- Compare average hours to resolve outages by delivery centre.
- Which client services had p95 response time above 2500 ms in the last 3 days?
- How much have we paid in SLA penalties for security incidents?
- Which service lines carry the most monthly fee in the High risk band?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show managed infrastructure services delivered from Bengaluru with an open outage and a high risk band.
- For the client service with the worst response time, list its incidents and the client contacts behind it.
- Which executive sponsors are linked to client services that are at risk?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.client_service_health_scorecard and plot risk_score by delivery_centre.*
- In the lakehouse SQL endpoint: *Write a query that finds client services with more than 3 open incidents.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from client contact to client service to incident.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Contract Revenue At Risk* turns a risk list into a business case. The rates are made up for the demo ($2,000 SLA penalty exposure per open incident + $500 per slow 15-min window + monthly fee x SLA tier x 1%) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

