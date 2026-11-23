# AI demo – Telecommunications: Southern Cross Telecom

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Southern Cross Telecom Gold layer. It's a 15-minute build once the framework has run. I use it with network operations and service assurance audiences (mobile, fixed and wholesale network operators).

Southern Cross Telecom is fictional: a mobile, fixed and NBN operator. Its network elements sit in `gold.dim_network_element`, its fault tickets in `gold.fact_fault_ticket`, and cell KPI counters in `gold.fact_cell_kpi_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `SCT_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `SCT_Ops_Agent` | 4 min |
| 3 | Ontology `SCT_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "telco"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.network_element_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_fault_ticket;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `SCT_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `SCT_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_network_element` | One row per network element |
   | `fact_fault_ticket` | One row per fault ticket |
   | `fact_cell_kpi_hourly` | Hourly roll-up of cell KPI counters, one row per network element per hour |
   | `network_element_health_scorecard` | Risk score, band and revenue at risk (AUD) per network element |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_fault_ticket[element_id]` | `dim_network_element[element_id]` |
   | `fact_fault_ticket[opened_date_key]` | `dim_date[date_key]` |
   | `fact_cell_kpi_hourly[element_id]` | `dim_network_element[element_id]` |
   | `fact_cell_kpi_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `network_element_health_scorecard[element_id]` | `dim_network_element[element_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_fault_ticket`, then **New measure** for each:

   ```dax
   Fault Tickets = COUNTROWS(fact_fault_ticket)
   Open Fault Tickets = CALCULATE(COUNTROWS(fact_fault_ticket), fact_fault_ticket[is_open] = 1)
   Service Outage Fault Tickets = CALCULATE(COUNTROWS(fact_fault_ticket), fact_fault_ticket[fault_category] = "OUTAGE")
   Avg Hours to Close = AVERAGE(fact_fault_ticket[hours_to_close])
   Total Field Cost (AUD) = SUM(fact_fault_ticket[field_cost_aud])
   Breach Readings = SUM(fact_cell_kpi_hourly[breach_readings])
   High Risk Network Elements = CALCULATE(COUNTROWS(network_element_health_scorecard), network_element_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(network_element_health_scorecard[risk_score])
   Revenue At Risk (AUD) = SUM(network_element_health_scorecard[revenue_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `revenue_at_risk_aud` is an illustrative dollar figure: $4,000 per open fault + $600 per congested 15-min window + capacity (users) x criticality x $0.50. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `element_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `network_element_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Southern Cross Telecom, a fictional mobile, fixed and NBN operator.
     A network element is identified by element_code. Region is in dim_network_element[region].
     Criticality is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open fault ticket has is_open = 1. 'Service Outage' means fault_category = 'OUTAGE'.
     A breach is a PRB utilisation reading above 90 %.
     Revenue At Risk is network_element_health_scorecard[revenue_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Fault Tickets* by `region`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open fault tickets by region*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `SCT_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `SCT_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_network_element`, `fact_fault_ticket`, `fact_cell_kpi_hourly`, `network_element_health_scorecard`, `customer_summary`
   - Semantic model **SCT_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the network operations and service assurance team at Southern Cross Telecom, a fictional mobile, fixed and NBN operator.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual network elements or risk scores.
   Region is the column region. Criticality is 1-5; 'critical' means 4 or 5.
   Open fault tickets: is_open = 1. Outage fault tickets: fault_category = 'OUTAGE'.
   A breach means prb_util_pct above 90 (%). Use breach_readings in fact_cell_kpi_hourly.
   'Last N days' is relative to the latest reading_date in fact_cell_kpi_hourly, not today's date - the data is a demo snapshot.
   Customers are only available as aggregates in customer_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   network_element_health_scorecard: one row per network element, risk_score 0-100, risk_band High/Medium/Low, open_fault_tickets, breach_readings, revenue_at_risk_aud (illustrative revenue at risk in AUD - always call it an estimate).
   fact_fault_ticket: one row per fault ticket; hours_to_close is NULL while open; region is copied onto each row.
   fact_cell_kpi_hourly: hourly; join to dim_network_element on element_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 network elements have the highest revenue at risk?**
   ```sql
   SELECT TOP 10 element_code, element_name, region, revenue_at_risk_aud, risk_score, risk_band, open_fault_tickets
   FROM gold.network_element_health_scorecard
   ORDER BY revenue_at_risk_aud DESC;
   ```
   **Average hours to close OUTAGE fault tickets by region**
   ```sql
   SELECT region, COUNT(*) AS fault_tickets, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_fault_ticket
   WHERE fault_category = 'OUTAGE' AND hours_to_close IS NOT NULL
   GROUP BY region
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which network elements breached the PRB utilisation threshold in the last 3 days?**
   ```sql
   SELECT d.element_code, d.element_name, d.region, SUM(f.breach_readings) AS breach_readings, MAX(f.max_prb_util_pct) AS max_prb_util_pct
   FROM gold.fact_cell_kpi_hourly AS f
   JOIN gold.dim_network_element AS d ON d.element_id = f.element_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_cell_kpi_hourly))
   GROUP BY d.element_code, d.element_name, d.region
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total field_cost_aud by fault_category this financial year**
   ```sql
   SELECT fault_category, COUNT(*) AS fault_tickets, SUM(field_cost_aud) AS total_field_cost_aud
   FROM gold.fact_fault_ticket
   WHERE opened_date >= '2026-07-01'
   GROUP BY fault_category
   ORDER BY total_field_cost_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `SCT_Ontology` (4 min, preview)

1. Open the semantic model `SCT_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `SCT_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_network_element` | **NetworkElement** | `element_id` (display: `element_name`) |
   | `fact_fault_ticket` | **FaultTicket** | `ticket_id` |
   | `network_element_health_scorecard` | **NetworkElementRisk** | `element_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **FaultTicket → raised against → NetworkElement**, **NetworkElement → has risk → NetworkElementRisk**.
5. Add the people side: **New entity type** `Customer` ▸ bind to lakehouse **lh_mdf** table `silver.customer` ▸ key `customer_id` ▸ keep `segment`, `region`, `status`, `customer_id`. Then add the relationship **Customer → linked to → NetworkElement** on `element_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **NetworkElement**, add a time-series property bound to `gold.fact_cell_kpi_hourly` with timestamp `reading_hour`, key `element_id` and value `max_prb_util_pct`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk network element. This is the visual moment.
8. Add the ontology as a source to `SCT_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which region had the most outage tickets this month?
- Show average hours to resolve by fault category.
- Which critical network elements have open fault tickets?
- Build a report page with Open Fault Tickets by region and a monthly trend.

**Data agent `SCT_Ops_Agent`:**
- Which 10 cells have the highest PRB utilisation and how many open faults do they have?
- Compare mean time to resolve outage tickets between Sydney Metro and Melbourne Metro.
- Which network elements breached 90% PRB utilisation in the last 3 days?
- What did field visits cost us for hardware faults by region?
- How many enterprise customers are active in Brisbane?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show 5G elements in Sydney Metro with an open outage ticket and a high risk band.
- For the most congested cell, list its fault tickets and the customers linked to it.
- Which customers are linked to elements that are currently faulty?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.network_element_health_scorecard and plot risk_score by region.*
- In the lakehouse SQL endpoint: *Write a query that finds network elements with more than 3 open fault tickets.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from customer to network element to fault ticket.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Revenue At Risk* turns a risk list into a business case. The rates are made up for the demo ($4,000 per open fault + $600 per congested 15-min window + capacity (users) x criticality x $0.50) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

