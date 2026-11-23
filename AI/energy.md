# AI demo – Energy & utilities: Southern Grid Transmission

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Southern Grid Transmission Gold layer. It's a 15-minute build once the framework has run. I use it with network operations and asset management audiences (electricity transmission and distribution network operators).

Southern Grid Transmission is fictional: a electricity transmission network operator in regional NSW. Its assets sit in `gold.dim_asset`, its work orders in `gold.fact_work_order`, and transformer telemetry in `gold.fact_tx_load_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `SGT_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `SGT_Ops_Agent` | 4 min |
| 3 | Ontology `SGT_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "energy"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.asset_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_work_order;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `SGT_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `SGT_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_asset` | One row per asset |
   | `fact_work_order` | One row per work order |
   | `fact_tx_load_hourly` | Hourly roll-up of transformer telemetry, one row per asset per hour |
   | `asset_health_scorecard` | Risk score, band and outage cost at risk (AUD) per asset |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_work_order[asset_id]` | `dim_asset[asset_id]` |
   | `fact_work_order[opened_date_key]` | `dim_date[date_key]` |
   | `fact_tx_load_hourly[asset_id]` | `dim_asset[asset_id]` |
   | `fact_tx_load_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `asset_health_scorecard[asset_id]` | `dim_asset[asset_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_work_order`, then **New measure** for each:

   ```dax
   Work Orders = COUNTROWS(fact_work_order)
   Open Work Orders = CALCULATE(COUNTROWS(fact_work_order), fact_work_order[is_open] = 1)
   Emergency Work Orders = CALCULATE(COUNTROWS(fact_work_order), fact_work_order[wo_type] = "EMERG")
   Avg Hours to Close = AVERAGE(fact_work_order[hours_to_close])
   Total Cost (AUD) = SUM(fact_work_order[cost_aud])
   Breach Readings = SUM(fact_tx_load_hourly[breach_readings])
   High Risk Assets = CALCULATE(COUNTROWS(asset_health_scorecard), asset_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(asset_health_scorecard[risk_score])
   Outage Cost At Risk (AUD) = SUM(asset_health_scorecard[outage_cost_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `outage_cost_at_risk_aud` is an illustrative dollar figure: $6,000 per open work order + $2,500 per 15-min window above the oil-temperature threshold + rating (MVA) x criticality x $20. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `asset_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `asset_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Southern Grid Transmission, a fictional electricity transmission network operator in regional NSW.
     A asset is identified by asset_tag. Region is in dim_asset[region].
     Criticality is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open work order has is_open = 1. 'Emergency' means wo_type = 'EMERG'.
     A breach is a oil temperature reading above 95 °C.
     Outage Cost At Risk is asset_health_scorecard[outage_cost_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Work Orders* by `region`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open work orders by region*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `SGT_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `SGT_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_asset`, `fact_work_order`, `fact_tx_load_hourly`, `asset_health_scorecard`, `landholder_summary`
   - Semantic model **SGT_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the network operations and asset management team at Southern Grid Transmission, a fictional electricity transmission network operator in regional NSW.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual assets or risk scores.
   Region is the column region. Criticality is 1-5; 'critical' means 4 or 5.
   Open work orders: is_open = 1. Emerg work orders: wo_type = 'EMERG'.
   A breach means oil_temp_c above 95 (°C). Use breach_readings in fact_tx_load_hourly.
   'Last N days' is relative to the latest reading_date in fact_tx_load_hourly, not today's date - the data is a demo snapshot.
   Landholders are only available as aggregates in landholder_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   asset_health_scorecard: one row per asset, risk_score 0-100, risk_band High/Medium/Low, open_work_orders, breach_readings, outage_cost_at_risk_aud (illustrative outage cost at risk in AUD - always call it an estimate).
   fact_work_order: one row per work order; hours_to_close is NULL while open; region is copied onto each row.
   fact_tx_load_hourly: hourly; join to dim_asset on asset_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 assets have the highest outage cost at risk?**
   ```sql
   SELECT TOP 10 asset_tag, asset_name, region, outage_cost_at_risk_aud, risk_score, risk_band, open_work_orders
   FROM gold.asset_health_scorecard
   ORDER BY outage_cost_at_risk_aud DESC;
   ```
   **Average hours to close EMERG work orders by region**
   ```sql
   SELECT region, COUNT(*) AS work_orders, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_work_order
   WHERE wo_type = 'EMERG' AND hours_to_close IS NOT NULL
   GROUP BY region
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which assets breached the oil temperature threshold in the last 3 days?**
   ```sql
   SELECT d.asset_tag, d.asset_name, d.region, SUM(f.breach_readings) AS breach_readings, MAX(f.max_oil_temp_c) AS max_oil_temp_c
   FROM gold.fact_tx_load_hourly AS f
   JOIN gold.dim_asset AS d ON d.asset_id = f.asset_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_tx_load_hourly))
   GROUP BY d.asset_tag, d.asset_name, d.region
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total cost_aud by wo_type this financial year**
   ```sql
   SELECT wo_type, COUNT(*) AS work_orders, SUM(cost_aud) AS total_cost_aud
   FROM gold.fact_work_order
   WHERE opened_date >= '2026-07-01'
   GROUP BY wo_type
   ORDER BY total_cost_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `SGT_Ontology` (4 min, preview)

1. Open the semantic model `SGT_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `SGT_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_asset` | **Asset** | `asset_id` (display: `asset_name`) |
   | `fact_work_order` | **WorkOrder** | `wo_id` |
   | `asset_health_scorecard` | **AssetRisk** | `asset_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **WorkOrder → raised against → Asset**, **Asset → has risk → AssetRisk**.
5. Add the people side: **New entity type** `Landholder` ▸ bind to lakehouse **lh_mdf** table `silver.landholder` ▸ key `landholder_id` ▸ keep `landholder_type`, `region`, `status`, `landholder_id`. Then add the relationship **Landholder → linked to → Asset** on `asset_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **Asset**, add a time-series property bound to `gold.fact_tx_load_hourly` with timestamp `reading_hour`, key `asset_id` and value `max_oil_temp_c`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk asset. This is the visual moment.
8. Add the ontology as a source to `SGT_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which region had the most unplanned work orders this financial year?
- Show total work order cost by asset class and month.
- Which critical assets (criticality 4 or 5) have open work orders?
- Build a report page with Open Work Orders by region and a monthly trend.

**Data agent `SGT_Ops_Agent`:**
- Which 10 transformers have the highest risk score, and how many open work orders does each have?
- Compare average hours to complete emergency work orders by region.
- Which transformers breached the 95 °C oil temperature threshold in the last 3 days?
- What did we spend on corrective maintenance in the Hunter region?
- List landholders by type in New England and how many are active.
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show me every asset in the Riverina that has an open emergency work order and a high risk band.
- For the transformer with the highest oil temperature, what work orders have been raised against it and what did they cost?
- Which landholders are linked to assets that are out of service?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.asset_health_scorecard and plot risk_score by region.*
- In the lakehouse SQL endpoint: *Write a query that finds assets with more than 3 open work orders.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from landholder to asset to work order.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Outage Cost At Risk* turns a risk list into a business case. The rates are made up for the demo ($6,000 per open work order + $2,500 per 15-min window above the oil-temperature threshold + rating (MVA) x criticality x $20) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

