# AI demo – Digital natives: Lyrebird Apps

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Copilot, a Fabric data agent and an ontology on top of the Lyrebird Apps Gold layer. It's a 15-minute build once the framework has run. I use it with product, growth and customer success audiences (SaaS scale-ups and fast-growing start-ups).

Lyrebird Apps is fictional: a B2B SaaS scale-up selling a collaboration platform. Its tenants sit in `gold.dim_tenant`, its support tickets in `gold.fact_support_ticket`, and product usage metrics in `gold.fact_usage_hourly`.

| Step | What you build | Time |
|---|---|---|
| 0 | Check tenant settings and data | 2 min |
| 1 | Semantic model `LYB_Operations_Model` (Direct Lake) | 4 min |
| 2 | Data agent `LYB_Ops_Agent` | 4 min |
| 3 | Ontology `LYB_Ontology` | 4 min |
| 4 | Run the demo prompts | 1 min to rehearse |

## 0. Before you start (2 min)

1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = "digital"`, and days 1–3 have been released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:
   ```sql
   SELECT COUNT(*) FROM gold.tenant_health_scorecard;   -- about 241
   SELECT COUNT(*) FROM gold.fact_support_ticket;    -- about 1,300
   ```
2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.
3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):
   - *Users can use Copilot and other features powered by Azure OpenAI*
   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* – **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region
   - *Users can create and share Fabric data agent item types*
   - *Users can create ontology (preview) items* (for step 3)
   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)

> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, *data agent* or *ontology*.

## 1. Semantic model `LYB_Operations_Model` (4 min)

1. Open lakehouse **lh_mdf** ▸ **New semantic model**.
2. Name it `LYB_Operations_Model`, pick the workspace, and tick these five tables in the **gold** schema:

   | Table | Role |
   |---|---|
   | `dim_date` | Calendar with Australian financial year (`fy_label`) |
   | `dim_tenant` | One row per tenant |
   | `fact_support_ticket` | One row per support ticket |
   | `fact_usage_hourly` | Hourly roll-up of product usage metrics, one row per tenant per hour |
   | `tenant_health_scorecard` | Risk score, band and ARR at risk (AUD) per tenant |

3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.
4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):

   | From (many) | To (one) |
   |---|---|
   | `fact_support_ticket[tenant_id]` | `dim_tenant[tenant_id]` |
   | `fact_support_ticket[opened_date_key]` | `dim_date[date_key]` |
   | `fact_usage_hourly[tenant_id]` | `dim_tenant[tenant_id]` |
   | `fact_usage_hourly[reading_date_key]` | `dim_date[date_key]` |
   | `tenant_health_scorecard[tenant_id]` | `dim_tenant[tenant_id]` (one-to-one is fine too) |

5. **Measures** – select `fact_support_ticket`, then **New measure** for each:

   ```dax
   Support Tickets = COUNTROWS(fact_support_ticket)
   Open Support Tickets = CALCULATE(COUNTROWS(fact_support_ticket), fact_support_ticket[is_open] = 1)
   Outage Support Tickets = CALCULATE(COUNTROWS(fact_support_ticket), fact_support_ticket[ticket_type] = "OUTAGE")
   Avg Hours to Close = AVERAGE(fact_support_ticket[hours_to_close])
   Total Credit (AUD) = SUM(fact_support_ticket[credit_aud])
   Breach Readings = SUM(fact_usage_hourly[breach_readings])
   High Risk Tenants = CALCULATE(COUNTROWS(tenant_health_scorecard), tenant_health_scorecard[risk_band] = "High")
   Avg Risk Score = AVERAGE(tenant_health_scorecard[risk_score])
   Arr At Risk (AUD) = SUM(tenant_health_scorecard[arr_at_risk_aud])
   ```
   Set the format string on the money measures to currency and on the averages to one decimal place.
   `arr_at_risk_aud` is an illustrative dollar figure: $1,500 per open ticket + $400 per slow 15-min window + seats x account tier x $12. Say so when you show it.

6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):
   - Hide the key columns: `tenant_id` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.
   - Set `dim_date[month_name]` to sort by `month_number`.
   - Add descriptions to the measures (one line each) and to `tenant_health_scorecard[risk_score]`: *0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*
7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):
   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.
   - **AI instructions** – paste:
     ```text
     This model describes Lyrebird Apps, a fictional B2B SaaS scale-up selling a collaboration platform.
     A tenant is identified by tenant_slug. Cloud region is in dim_tenant[cloud_region].
     Account tier is 1-5; 'critical' or 'high tier' means 4 or 5.
     An open support ticket has is_open = 1. 'Outage' means ticket_type = 'OUTAGE'.
     A breach is a p95 API latency reading above 800 ms.
     Arr At Risk is tenant_health_scorecard[arr_at_risk_aud], an illustrative AUD estimate - call it an estimate.
     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).
     Always show amounts in AUD and round hours to one decimal place.
     ```
   - **Verified answers** (optional): build a bar chart of *Open Support Tickets* by `cloud_region`, then right-click ▸ *Set up a verified answer* with the trigger phrase *open support tickets by cloud region*.
8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot shows a low-quality warning.

## 2. Data agent `LYB_Ops_Agent` (4 min)

1. Workspace ▸ **New item ▸ Data agent** ▸ name `LYB_Ops_Agent`.
2. **Add data source** ▸ OneLake catalog ▸ add both:
   - Lakehouse **lh_mdf** – tick only these gold tables: `dim_tenant`, `fact_support_ticket`, `fact_usage_hourly`, `tenant_health_scorecard`, `app_user_summary`
   - Semantic model **LYB_Operations_Model**
   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*
3. **Agent instructions** – paste:
   ```text
   You answer questions for the product, growth and customer success team at Lyrebird Apps, a fictional B2B SaaS scale-up selling a collaboration platform.
   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).
   Use the lakehouse gold tables for lists, rankings and anything about individual tenants or risk scores.
   Cloud region is the column cloud_region. Account tier is 1-5; 'critical' means 4 or 5.
   Open support tickets: is_open = 1. Outage support tickets: ticket_type = 'OUTAGE'.
   A breach means p95_latency_ms above 800 (ms). Use breach_readings in fact_usage_hourly.
   'Last N days' is relative to the latest reading_date in fact_usage_hourly, not today's date - the data is a demo snapshot.
   App users are only available as aggregates in app_user_summary; never try to identify a person.
   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.
   ```
4. Select the lakehouse source ▸ **Data source instructions** – paste:
   ```text
   tenant_health_scorecard: one row per tenant, risk_score 0-100, risk_band High/Medium/Low, open_support_tickets, breach_readings, arr_at_risk_aud (illustrative ARR at risk in AUD - always call it an estimate).
   fact_support_ticket: one row per support ticket; hours_to_close is NULL while open; cloud region is copied onto each row.
   fact_usage_hourly: hourly; join to dim_tenant on tenant_id for names.
   ```
5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):

   **Which 10 tenants have the highest ARR at risk?**
   ```sql
   SELECT TOP 10 tenant_slug, tenant_name, cloud_region, arr_at_risk_aud, risk_score, risk_band, open_support_tickets
   FROM gold.tenant_health_scorecard
   ORDER BY arr_at_risk_aud DESC;
   ```
   **Average hours to close OUTAGE support tickets by cloud region**
   ```sql
   SELECT cloud_region, COUNT(*) AS support_tickets, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close
   FROM gold.fact_support_ticket
   WHERE ticket_type = 'OUTAGE' AND hours_to_close IS NOT NULL
   GROUP BY cloud_region
   ORDER BY avg_hours_to_close DESC;
   ```
   **Which tenants breached the p95 API latency threshold in the last 3 days?**
   ```sql
   SELECT d.tenant_slug, d.tenant_name, d.cloud_region, SUM(f.breach_readings) AS breach_readings, MAX(f.max_p95_latency_ms) AS max_p95_latency_ms
   FROM gold.fact_usage_hourly AS f
   JOIN gold.dim_tenant AS d ON d.tenant_id = f.tenant_id
   WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.fact_usage_hourly))
   GROUP BY d.tenant_slug, d.tenant_name, d.cloud_region
   HAVING SUM(f.breach_readings) > 0
   ORDER BY breach_readings DESC;
   ```
   **Total credit_aud by ticket_type this financial year**
   ```sql
   SELECT ticket_type, COUNT(*) AS support_tickets, SUM(credit_aud) AS total_credit_aud
   FROM gold.fact_support_ticket
   WHERE opened_date >= '2026-07-01'
   GROUP BY ticket_type
   ORDER BY total_credit_aud DESC;
   ```
   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.
6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.
   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or Microsoft Foundry (use the Fabric IQ tool there).

> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. That's a good governance moment in the demo.

## 3. Ontology `LYB_Ontology` (4 min, preview)

1. Open the semantic model `LYB_Operations_Model` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). You need Build permission on the model.
2. Name it `LYB_Ontology` (letters, numbers and underscores only).
3. Fabric creates entity types from the tables, properties from the columns, relationships from the model relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:

   | Generated from | Rename to | Key |
   |---|---|---|
   | `dim_tenant` | **Tenant** | `tenant_id` (display: `tenant_name`) |
   | `fact_support_ticket` | **SupportTicket** | `ticket_id` |
   | `tenant_health_scorecard` | **TenantRisk** | `tenant_id` |
   | `dim_date` | **Day** | `date_key` |

4. Name the relationships in plain words: **SupportTicket → raised against → Tenant**, **Tenant → has risk → TenantRisk**.
5. Add the people side: **New entity type** `AppUser` ▸ bind to lakehouse **lh_mdf** table `silver.app_user` ▸ key `user_id` ▸ keep `user_role`, `cloud_region`, `status`, `user_id`. Then add the relationship **AppUser → linked to → Tenant** on `tenant_id`.
   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.
6. Time series (optional, 1 min): on **Tenant**, add a time-series property bound to `gold.fact_usage_hourly` with timestamp `reading_hour`, key `tenant_id` and value `max_p95_latency_ms`. Time series aren't generated – you bind them by hand.
7. Open the **graph** view and click through one high-risk tenant. This is the visual moment.
8. Add the ontology as a source to `LYB_Ops_Agent` (or create an ontology agent) and ask the multi-hop questions below.

> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.

## 4. Demo prompts

**Copilot in the report or standalone Copilot** (on the semantic model):
- Which cloud region raised the most support tickets this month?
- Show credits issued by ticket type and month.
- Which strategic tenants (tier 4 or 5) have open outage tickets?
- Build a report page with Open Support Tickets by cloud region and a monthly trend.

**Data agent `LYB_Ops_Agent`:**
- Which 10 tenants have the highest risk score and how many open tickets does each have?
- Compare average hours to solve outage tickets by cloud region.
- Which tenants saw p95 latency above 800 ms in the last 3 days?
- How much credit have we issued to Enterprise tenants?
- How many admins are active across tenants in AU East?
- Now show that as a chart.  *(follow-up – shows conversation context)*

**Ontology (graph and agent):**
- Show Enterprise tenants in AU East with an open outage ticket and a high risk band.
- For the tenant with the worst latency, list its support tickets and the users affected.
- Which app users belong to tenants that are past due?

**Copilot in notebooks and SQL** (quick extras if there's time):
- In a notebook attached to lh_mdf: *Load gold.tenant_health_scorecard and plot risk_score by cloud_region.*
- In the lakehouse SQL endpoint: *Write a query that finds tenants with more than 3 open support tickets.*

## What to say while it runs

- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – the agent never sees the quarantined rows.
- Semantic model = governed definitions ("what does *open* mean"). Agent = conversation over several sources. Ontology = the business vocabulary and relationships, so questions can hop from app user to tenant to support ticket.
- Everything respects the asker's permissions. Show what a user with only the RLS role sees.
- Put a dollar figure on it: *Arr At Risk* turns a risk list into a business case. The rates are made up for the demo ($1,500 per open ticket + $400 per slow 15-min window + seats x account tier x $12) – in a real project the business owns them, ideally in a control table.

## If something goes wrong

| Symptom | Fix |
|---|---|
| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |
| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |
| "Last 3 days" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |
| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |
| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |

