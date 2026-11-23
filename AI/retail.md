# AI demo – Retail: Wattle & Co

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Wattle & Co Gold layer. It's a 15-minute build once the framework has run. I use it with store operations, availability and supply chain audiences (supermarkets, general merchandise and specialty retailers).

Wattle & Co is fictional: a national supermarket and general merchandise retailer. Its stores sit in `gold.dim_store`, its stock issues in `gold.fact_stock_issue`, and store trading and availability (15-min) in `gold.fact_store_trading_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `WCO_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `WCO_Ops_Agent` | 4 min |
| 3 | Ontology `WCO_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "retail"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.store_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_stock_issue;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `WCO_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `WCO_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_store` | One row per store |
   | `fact_stock_issue` | One row per stock issue |
   | `fact_store_trading_hourly` | Hourly roll-up of store trading and availability (15-min), one row per store per hour |
   | `store_health_scorecard` | Risk score, band and lost sales at risk (AUD) per store |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_stock_issue[store_id]` | `dim_store[store_id]` |
   | `fact_stock_issue[opened_date_key]` | `dim_date[date_key]` |
   | `fact_store_trading_hourly[store_id]` | `dim_store[store_id]` |
   | `fact_store_trading_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `store_health_scorecard[store_id]` | `dim_store[store_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_stock_issue`, then **New measure** for each:

   ```dax
   Stock Issues = COUNTROWS(fact_stock_issue)
   Open Stock Issues = CALCULATE(COUNTROWS(fact_stock_issue), fact_stock_issue[is_open] = 1)
   Out Of Stock Stock Issues = CALCULATE(COUNTROWS(fact_stock_issue), fact_stock_issue[issue_type] = "OUT_OF_STOCK")
   Avg Hours to Close = AVERAGE(fact_stock_issue[hours_to_close])
   Total Lost Sales (AUD) = SUM(fact_stock_issue[lost_sales_aud])
   Breach Readings = SUM(fact_store_trading_hourly[breach_readings])
   High Risk Stores = CALCULATE(COUNTROWS(store_health_scorecard), store_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(store_health_scorecard[risk_score])
   Lost Sales At Risk (AUD) = SUM(store_health_scorecard[lost_sales_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `lost_sales_at_risk_aud` is an illustrative dollar figure: $1,800 per open stock issue + $900 per 15-min window above 8% out-of-stock + weekly sales (k) x store tier x $1. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `store_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `store_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Wattle & Co, a fictional national supermarket and general merchandise retailer.
     A store is identified by store_code. State is in dim_store[state].
     Store tier is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open stock issue has is_open = 1. 'Out Of Stock' means issue_type = 'OUT_OF_STOCK'.
     A breach is a out-of-stock lines reading above 8 %.
     Lost Sales At Risk is store_health_scorecard[lost_sales_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Stock Issues* by `state`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open stock issues by state*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `WCO_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `WCO_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_store`, `fact_stock_issue`, `fact_store_trading_hourly`, `store_health_scorecard`, `loyalty_member_summary`
   - Semantic model **WCO_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the store operations, availability and supply chain team at Wattle & Co, a fictional national supermarket and general merchandise retailer.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual stores or risk scores.
   State is the column state. Store tier is 1-5; 'critical' means 4 or 5.
   Open stock issues: is_open = 1. Out Of Stock stock issues: issue_type = 'OUT_OF_STOCK'.
   A breach means out_of_stock_pct above 8 (%). Use breach_readings in fact_store_trading_hourly.
   'Last N days' is relative to the latest reading_date in fact_store_trading_hourly, not today's date - the data is a demo snapshot.
   Loyalty members are only available as aggregates in loyalty_member_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   store_health_scorecard: one row per store, risk_score 0-100, risk_band High/Medium/Low, open_stock_issues, breach_readings, lost_sales_at_risk_aud (illustrative lost sales at risk in AUD - always call it an estimate).
   fact_stock_issue: one row per stock issue; hours_to_close is NULL while open; state is copied onto each row.
   fact_store_trading_hourly: hourly; join to dim_store on store_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 stores have the highest lost sales at risk?**
   ```sql
   SELECT TOP 10 store_code, store_name, state, lost_sales_at_risk_aud, risk_score, risk_band, open_stock_issues
   FROM gold.store_health_scorecard
   ORDER BY lost_sales_at_risk_aud DESC;
   ```
   **Average hours to close OUT_OF_STOCK stock issues by state**
   ```sql
   SELECT state, COUNT(*) AS stock_issues, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_stock_issue
   WHERE issue_type = 'OUT_OF_STOCK' AND hours_to_close IS NOT NULL
   GROUP BY state
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which stores breached the out-of-stock lines threshold in the last 3 days?**
   ```sql
   SELECT d.store_code, d.store_name, d.state, SUM(f.breach_readings) AS breach_readings, MAX(f.max_out_of_stock_pct) AS max_out_of_stock_pct
   FROM gold.fact_store_trading_hourly AS f
   JOIN gold.dim_store AS d ON d.store_id = f.store_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_store_trading_hourly))
   GROUP BY d.store_code, d.store_name, d.state
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total lost_sales_aud by issue_type this financial year**
   ```sql
   SELECT issue_type, COUNT(*) AS stock_issues, SUM(lost_sales_aud) AS total_lost_sales_aud
   FROM gold.fact_stock_issue
   WHERE opened_date >= '2026-07-01'
   GROUP BY issue_type
   ORDER BY total_lost_sales_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `WCO_Ontology` (4 min, preview)

1. Open the semantic model `WCO_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `WCO_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_store` | **Store** | `store_id` (display: `store_name`) |
   | `fact_stock_issue` | **StockIssue** | `issue_id` |
   | `store_health_scorecard` | **StoreRisk** | `store_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **StockIssue → raised against → Store**, **Store → has risk → StoreRisk**.
5. Add the people side: **New entity type** `LoyaltyMember` ▸ bind to lakehouse **lh_mdf** table `silver.loyalty_member` ▸ key `member_id` ▸ keep `tier`, `state`, `status`, `member_id`. Then add the relationship **LoyaltyMember → linked to → Store** on `store_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **Store**, add a time-series property bound to `gold.fact_store_trading_hourly` with timestamp `reading_hour`, key `store_id` and value `max_out_of_stock_pct`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk store. This is the visual moment.
8. Add the ontology as a source to `WCO_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which state lost the most sales to stock issues this month?
- Show lost sales by issue type and month.
- Which flagship stores (tier 4 or 5) have open out-of-stock issues?
- Build a report page with Open Stock Issues by state and a monthly trend.

**Data agent `WCO_Ops_Agent`:**
- Which 10 stores have the most lost sales at risk, and how many stock issues are still open?
- Compare average hours to resolve out-of-stock issues by state against the 4-hour target.
- Which stores had more than 8% of lines out of stock in the last 3 days?
- On half-price promotion days, do we see more out-of-stocks?
- How many Gold loyalty members are active in each state?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show supermarkets in NSW with an open out-of-stock issue and a high risk band.
- For the store with the worst availability, list its open stock issues and the lost sales.
- Which loyalty members shop at stores with an open cold-chain breach?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.store_health_scorecard and plot risk_score by state.*
- In the lakehouse SQL endpoint: *Write a query that finds stores with more than 3 open stock issues.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from loyalty member to store to stock issue.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Lost Sales At Risk* turns a risk list into a business case. The rates are made up for the demo ($1,800 per open stock issue + $900 per 15-min window above 8% out-of-stock + weekly sales (k) x store tier x $1) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

