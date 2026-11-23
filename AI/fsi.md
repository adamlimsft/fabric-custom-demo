# AI demo – Financial services: Harbour Mutual Bank

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Harbour Mutual Bank Gold layer. It's a 15-minute build once the framework has run. I use it with fraud, credit risk and collections audiences (retail and business banks, mutuals and credit unions).

Harbour Mutual Bank is fictional: a retail and business bank. Its accounts sit in `gold.dim_account`, its risk cases in `gold.fact_risk_case`, and account transaction monitoring (15-min windows) in `gold.fact_txn_monitor_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `HMB_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `HMB_Ops_Agent` | 4 min |
| 3 | Ontology `HMB_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "fsi"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.account_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_risk_case;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `HMB_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `HMB_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_account` | One row per account |
   | `fact_risk_case` | One row per risk case |
   | `fact_txn_monitor_hourly` | Hourly roll-up of account transaction monitoring (15-min windows), one row per account per hour |
   | `account_health_scorecard` | Risk score, band and expected loss (AUD) per account |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_risk_case[account_id]` | `dim_account[account_id]` |
   | `fact_risk_case[opened_date_key]` | `dim_date[date_key]` |
   | `fact_txn_monitor_hourly[account_id]` | `dim_account[account_id]` |
   | `fact_txn_monitor_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `account_health_scorecard[account_id]` | `dim_account[account_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_risk_case`, then **New measure** for each:

   ```dax
   Risk Cases = COUNTROWS(fact_risk_case)
   Open Risk Cases = CALCULATE(COUNTROWS(fact_risk_case), fact_risk_case[is_open] = 1)
   Account Takeover Risk Cases = CALCULATE(COUNTROWS(fact_risk_case), fact_risk_case[case_type] = "ACCOUNT_TAKEOVER")
   Avg Hours to Close = AVERAGE(fact_risk_case[hours_to_close])
   Total Case Amount (AUD) = SUM(fact_risk_case[case_amount_aud])
   Breach Readings = SUM(fact_txn_monitor_hourly[breach_readings])
   High Risk Accounts = CALCULATE(COUNTROWS(account_health_scorecard), account_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(account_health_scorecard[risk_score])
   Expected Loss (AUD) = SUM(account_health_scorecard[expected_loss_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `expected_loss_aud` is an illustrative dollar figure: $3,500 per open case + $250 per suspicious 15-min window + balance x risk grade x 0.2%. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `account_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `account_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Harbour Mutual Bank, a fictional retail and business bank.
     A account is identified by account_number. State is in dim_account[state].
     Risk grade is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open risk case has is_open = 1. 'Account Takeover' means case_type = 'ACCOUNT_TAKEOVER'.
     A breach is a declined transaction rate reading above 30 %.
     Expected Loss is account_health_scorecard[expected_loss_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Risk Cases* by `state`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open risk cases by state*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `HMB_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `HMB_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_account`, `fact_risk_case`, `fact_txn_monitor_hourly`, `account_health_scorecard`, `customer_summary`
   - Semantic model **HMB_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the fraud, credit risk and collections team at Harbour Mutual Bank, a fictional retail and business bank.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual accounts or risk scores.
   State is the column state. Risk grade is 1-5; 'critical' means 4 or 5.
   Open risk cases: is_open = 1. Account Takeover risk cases: case_type = 'ACCOUNT_TAKEOVER'.
   A breach means decline_pct above 30 (%). Use breach_readings in fact_txn_monitor_hourly.
   'Last N days' is relative to the latest reading_date in fact_txn_monitor_hourly, not today's date - the data is a demo snapshot.
   Customers are only available as aggregates in customer_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   account_health_scorecard: one row per account, risk_score 0-100, risk_band High/Medium/Low, open_risk_cases, breach_readings, expected_loss_aud (illustrative expected loss in AUD - always call it an estimate).
   fact_risk_case: one row per risk case; hours_to_close is NULL while open; state is copied onto each row.
   fact_txn_monitor_hourly: hourly; join to dim_account on account_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 accounts have the highest expected loss?**
   ```sql
   SELECT TOP 10 account_number, account_name, state, expected_loss_aud, risk_score, risk_band, open_risk_cases
   FROM gold.account_health_scorecard
   ORDER BY expected_loss_aud DESC;
   ```
   **Average hours to close ACCOUNT_TAKEOVER risk cases by state**
   ```sql
   SELECT state, COUNT(*) AS risk_cases, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_risk_case
   WHERE case_type = 'ACCOUNT_TAKEOVER' AND hours_to_close IS NOT NULL
   GROUP BY state
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which accounts breached the declined transaction rate threshold in the last 3 days?**
   ```sql
   SELECT d.account_number, d.account_name, d.state, SUM(f.breach_readings) AS breach_readings, MAX(f.max_decline_pct) AS max_decline_pct
   FROM gold.fact_txn_monitor_hourly AS f
   JOIN gold.dim_account AS d ON d.account_id = f.account_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_txn_monitor_hourly))
   GROUP BY d.account_number, d.account_name, d.state
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total case_amount_aud by case_type this financial year**
   ```sql
   SELECT case_type, COUNT(*) AS risk_cases, SUM(case_amount_aud) AS total_case_amount_aud
   FROM gold.fact_risk_case
   WHERE opened_date >= '2026-07-01'
   GROUP BY case_type
   ORDER BY total_case_amount_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `HMB_Ontology` (4 min, preview)

1. Open the semantic model `HMB_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `HMB_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_account` | **Account** | `account_id` (display: `account_name`) |
   | `fact_risk_case` | **RiskCase** | `case_id` |
   | `account_health_scorecard` | **AccountRisk** | `account_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **RiskCase → raised against → Account**, **Account → has risk → AccountRisk**.
5. Add the people side: **New entity type** `Customer` ▸ bind to lakehouse **lh_mdf** table `silver.customer` ▸ key `customer_id` ▸ keep `segment`, `state`, `status`, `customer_id`. Then add the relationship **Customer → linked to → Account** on `account_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **Account**, add a time-series property bound to `gold.fact_txn_monitor_hourly` with timestamp `reading_hour`, key `account_id` and value `max_decline_pct`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk account. This is the visual moment.
8. Add the ontology as a source to `HMB_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- What was the total case amount by case type this financial year?
- Which state has the most accounts in arrears?
- Show total expected loss by product type.
- Build a report page with Open Risk Cases by state and a monthly trend.

**Data agent `HMB_Ops_Agent`:**
- Which 10 accounts have the highest expected loss, and what is their risk band and number of open cases?
- Compare average hours to close account takeover cases by state.
- Which accounts had a declined transaction rate above 30% in the last 3 days?
- How many home loan accounts are in arrears with an open hardship case, and what is their total balance?
- How much have we lost to scams in NSW compared with VIC?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show credit card accounts in NSW with an open account takeover case and a high risk band.
- For the account with the highest expected loss, list its risk cases and the customer segment behind it.
- Which customers own accounts that are in arrears and have an open fraud or hardship case?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.account_health_scorecard and plot risk_score by state.*
- In the lakehouse SQL endpoint: *Write a query that finds accounts with more than 3 open risk cases.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from customer to account to risk case.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Expected Loss* turns a risk list into a business case. The rates are made up for the demo ($3,500 per open case + $250 per suspicious 15-min window + balance x risk grade x 0.2%) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

