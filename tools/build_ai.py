"""Writes AI/<pack>.md - a 15-minute build guide for Copilot, a Fabric data agent and an ontology on the pack's Gold layer.
Called by build_kit.py. Standard library only."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kitlib  # noqa: E402


def title(s):
    return " ".join(w.capitalize() for w in s.replace("_", " ").split())


def onto_name(s):
    return "".join(w.capitalize() for w in s.split("_"))


def example_sql(p):
    n, E, V, R = kitlib.N(p), p["entity"], p["event"], p["reading"]
    reg, m1 = n["reg"], R["metrics"][0]
    urgent = V["urgent_type"]
    return [
        (f"Which 10 {E['plural']} have the highest {p['impact']['label']}?",
         f"SELECT TOP 10 {E['code']}, {E['name_col']}, {reg}, {p['impact']['col']}, risk_score, risk_band, open_{V['table']}s\n"
         f"FROM gold.{n['g_score']}\nORDER BY {p['impact']['col']} DESC;"),
        (f"Average hours to close {urgent} {V['plural']} by {p['region_label']}",
         f"SELECT {reg}, COUNT(*) AS {V['table']}s, ROUND(AVG(hours_to_close), 1) AS avg_hours_to_close\n"
         f"FROM gold.{n['g_fact']}\nWHERE {V['type_col']} = '{urgent}' AND hours_to_close IS NOT NULL\n"
         f"GROUP BY {reg}\nORDER BY avg_hours_to_close DESC;"),
        (f"Which {E['plural']} breached the {m1['label']} threshold in the last 3 days?",
         f"SELECT d.{E['code']}, d.{E['name_col']}, d.{reg}, SUM(f.breach_readings) AS breach_readings, "
         f"MAX(f.max_{m1['col']}) AS max_{m1['col']}\n"
         f"FROM gold.{n['g_rfact']} AS f\nJOIN gold.{n['g_dim']} AS d ON d.{E['id']} = f.{E['id']}\n"
         f"WHERE f.reading_date >= DATEADD(day, -2, (SELECT MAX(reading_date) FROM gold.{n['g_rfact']}))\n"
         f"GROUP BY d.{E['code']}, d.{E['name_col']}, d.{reg}\nHAVING SUM(f.breach_readings) > 0\n"
         f"ORDER BY breach_readings DESC;"),
        (f"Total {V['amount_col']} by {V['type_col']} this financial year",
         f"SELECT {V['type_col']}, COUNT(*) AS {V['table']}s, SUM({V['amount_col']}) AS total_{V['amount_col']}\n"
         f"FROM gold.{n['g_fact']}\nWHERE opened_date >= '2026-07-01'\nGROUP BY {V['type_col']}\n"
         f"ORDER BY total_{V['amount_col']} DESC;"),
    ]


def measures(p):
    n, E, V, R = kitlib.N(p), p["entity"], p["event"], p["reading"]
    f, s, r = n["g_fact"], n["g_score"], n["g_rfact"]
    vp, ep = title(V["plural"]), title(E["plural"])
    urgent_name = V["type_names"][V["types"].index(V["urgent_type"])]
    return [
        (vp, f"COUNTROWS({f})", "0"),
        (f"Open {vp}", f"CALCULATE(COUNTROWS({f}), {f}[is_open] = 1)", "0"),
        (f"{title(urgent_name)} {vp}", f"CALCULATE(COUNTROWS({f}), {f}[{V['type_col']}] = \"{V['urgent_type']}\")", "0"),
        ("Avg Hours to Close", f"AVERAGE({f}[hours_to_close])", "0.0"),
        (f"Total {title(V['amount_col'].replace('_aud', ''))} (AUD)", f"SUM({f}[{V['amount_col']}])", "$#,0"),
        ("Breach Readings", f"SUM({r}[breach_readings])", "0"),
        (f"High Risk {ep}", f"CALCULATE(COUNTROWS({s}), {s}[risk_band] = \"High\")", "0"),
        ("Avg Risk Score", f"AVERAGE({s}[risk_score])", "0.0"),
        (f"{title(p['impact']['label'])} (AUD)", f"SUM({s}[{p['impact']['col']}])", "$#,0"),
    ]


def render(p):
    n, E, V, R, Pa, A = kitlib.N(p), p["entity"], p["event"], p["reading"], p["party"], p["ai"]
    reg, m1 = n["reg"], R["metrics"][0]
    org, short = p["org"], p["short"]
    model = f"{short}_Operations_Model"
    agent = f"{short}_Ops_Agent"
    onto = f"{short}_Ontology"
    ms = measures(p)
    sqls = example_sql(p)
    urgent = V["urgent_type"]
    tier_hi = "4 or 5"
    ent_o, ev_o, pa_o = onto_name(E["table"]), onto_name(V["table"]), onto_name(Pa["table"])
    L = []
    w = L.append
    w(f"# AI demo – {p['industry']}: {org}")
    w("")
    w(f"Copilot, a Fabric data agent and an ontology on top of the {org} Gold layer. It's a 15-minute build once the "
      f"framework has run. I use it with {A['persona']} audiences ({A['cust']}).")
    w("")
    w(f"{org} is fictional: a {p['tagline']}. Its {E['plural']} sit in `gold.{n['g_dim']}`, its {V['plural']} in "
      f"`gold.{n['g_fact']}`, and {R['label']} in `gold.{n['g_rfact']}`.")
    w("")
    w("| Step | What you build | Time |")
    w("|---|---|---|")
    w("| 0 | Check tenant settings and data | 2 min |")
    w(f"| 1 | Semantic model `{model}` (Direct Lake) | 4 min |")
    w(f"| 2 | Data agent `{agent}` | 4 min |")
    w(f"| 3 | Ontology `{onto}` | 4 min |")
    w("| 4 | Run the demo prompts | 1 min to rehearse |")
    w("")
    w("## 0. Before you start (2 min)")
    w("")
    w(f"1. In the workspace, `nb_mdf_00_setup_framework` has run with `industry = \"{p['id']}\"`, and days 1–3 have been "
      "released and processed (`nb_mdf_90_run_all` three times). Check the Gold tables have rows:")
    w("   ```sql")
    w(f"   SELECT COUNT(*) FROM gold.{n['g_score']};   -- about {E['count'] + 1}")
    w(f"   SELECT COUNT(*) FROM gold.{n['g_fact']};    -- about 1,300")
    w("   ```")
    w("2. The workspace is on a paid capacity: **F2 or higher, or P1 or higher**. Copilot and data agents don't run on trial capacities.")
    w("3. In the **Admin portal ▸ Tenant settings**, these are on for you (ask your Fabric admin):")
    w("   - *Users can use Copilot and other features powered by Azure OpenAI*")
    w("   - *Data sent to Azure OpenAI can be processed outside your capacity's geographic region...* and *...stored outside...* "
      "– **needed in Australia**, because Azure OpenAI for Fabric isn't hosted in every region")
    w("   - *Users can create and share Fabric data agent item types*")
    w("   - *Users can create ontology (preview) items* (for step 3)")
    w("   - *Users can access a standalone, cross-item Power BI Copilot experience* (optional, for the standalone Copilot prompts)")
    w("")
    w("> Setting names move around between releases. If you can't find one, search the tenant settings page for *Copilot*, "
      "*data agent* or *ontology*.")
    w("")
    w(f"## 1. Semantic model `{model}` (4 min)")
    w("")
    w("1. Open lakehouse **lh_mdf** ▸ **New semantic model**.")
    w(f"2. Name it `{model}`, pick the workspace, and tick these five tables in the **gold** schema:")
    w("")
    w("   | Table | Role |")
    w("   |---|---|")
    w(f"   | `dim_date` | Calendar with Australian financial year (`fy_label`) |")
    w(f"   | `{n['g_dim']}` | One row per {E['label']} |")
    w(f"   | `{n['g_fact']}` | One row per {V['label']} |")
    w(f"   | `{n['g_rfact']}` | Hourly roll-up of {R['label']}, one row per {E['label']} per hour |")
    w(f"   | `{n['g_score']}` | Risk score, band and {p['impact']['label']} (AUD) per {E['label']} |")
    w("")
    w("3. Select **Confirm**. The model opens in the web editor in Direct Lake mode.")
    w("4. **Relationships** (drag the column from the many side to the one side; all single direction, many-to-one):")
    w("")
    w("   | From (many) | To (one) |")
    w("   |---|---|")
    w(f"   | `{n['g_fact']}[{E['id']}]` | `{n['g_dim']}[{E['id']}]` |")
    w(f"   | `{n['g_fact']}[opened_date_key]` | `dim_date[date_key]` |")
    w(f"   | `{n['g_rfact']}[{E['id']}]` | `{n['g_dim']}[{E['id']}]` |")
    w(f"   | `{n['g_rfact']}[reading_date_key]` | `dim_date[date_key]` |")
    w(f"   | `{n['g_score']}[{E['id']}]` | `{n['g_dim']}[{E['id']}]` (one-to-one is fine too) |")
    w("")
    w(f"5. **Measures** – select `{n['g_fact']}`, then **New measure** for each:")
    w("")
    w("   ```dax")
    for name, expr, fmt in ms:
        w(f"   {name} = {expr}")
    w("   ```")
    w("   Set the format string on the money measures to currency and on the averages to one decimal place.")
    w(f"   `{p['impact']['col']}` is an illustrative dollar figure: {p['impact']['rule']}. Say so when you show it.")
    w("")
    w("6. Tidy up for AI (this is what makes Copilot answers good, so don't skip it):")
    w(f"   - Hide the key columns: `{E['id']}` on the fact tables, `opened_date_key`, `reading_date_key`, `date_key`.")
    w(f"   - Set `dim_date[month_name]` to sort by `month_number`.")
    w(f"   - Add descriptions to the measures (one line each) and to `{n['g_score']}[risk_score]`: "
      f"*0–100. Higher means more attention needed. Bands: High ≥ 60, Medium ≥ 35.*")
    w("7. **Prep data for AI** (Home ribbon ▸ *Prep data for AI*):")
    w("   - **AI data schema**: untick `version_valid_from`, `age_years` and the hourly averages you won't ask about.")
    w("   - **AI instructions** – paste:")
    w("     ```text")
    w(f"     This model describes {org}, a fictional {p['tagline']}.")
    w(f"     A {E['label']} is identified by {E['code']}. {p['region_label'].capitalize()} is in {n['g_dim']}[{reg}].")
    w(f"     {E['tier_col'].replace('_', ' ').capitalize()} is 1-5; 'critical' or 'high tier' means {tier_hi}.")
    w(f"     An open {V['label']} has is_open = 1. '{title(V['type_names'][V['types'].index(urgent)])}' means {V['type_col']} = '{urgent}'.")
    w(f"     A breach is a {m1['label']} reading above {m1['threshold']} {m1['unit']}.")
    w(f"     {title(p['impact']['label'])} is {n['g_score']}[{p['impact']['col']}], an illustrative AUD estimate - call it an estimate.")
    w(f"     'This financial year' means dim_date[fy_label] = 'FY27' (1 July 2026 to 30 June 2027).")
    w("     Always show amounts in AUD and round hours to one decimal place.")
    w("     ```")
    w("   - **Verified answers** (optional): build a bar chart of *Open " + title(V["plural"]) + f"* by `{reg}`, then right-click "
      f"▸ *Set up a verified answer* with the trigger phrase *open {V['plural']} by {p['region_label']}*.")
    w("8. In the model **Settings ▸ Q&A**, turn on Q&A, and tick **Approved for Copilot**. Without it, standalone Copilot "
      "shows a low-quality warning.")
    w("")
    w(f"## 2. Data agent `{agent}` (4 min)")
    w("")
    w(f"1. Workspace ▸ **New item ▸ Data agent** ▸ name `{agent}`.")
    w("2. **Add data source** ▸ OneLake catalog ▸ add both:")
    w(f"   - Lakehouse **lh_mdf** – tick only these gold tables: `{n['g_dim']}`, `{n['g_fact']}`, `{n['g_rfact']}`, "
      f"`{n['g_score']}`, `{n['g_party']}`")
    w(f"   - Semantic model **{model}**")
    w("   *(Optional, if you built the Warehouse module: also add warehouse **wh_mdf** and tick the `dw` tables.)*")
    w("3. **Agent instructions** – paste:")
    w("   ```text")
    w(f"   You answer questions for the {A['persona']} team at {org}, a fictional {p['tagline']}.")
    w("   Use the semantic model for KPIs that already exist as measures (counts, totals, averages by region or month).")
    w(f"   Use the lakehouse gold tables for lists, rankings and anything about individual {E['plural']} or risk scores.")
    w(f"   {p['region_label'].capitalize()} is the column {reg}. {E['tier_col'].replace('_', ' ').capitalize()} is 1-5; "
      f"'critical' means {tier_hi}.")
    w(f"   Open {V['plural']}: is_open = 1. {title(urgent.lower())} {V['plural']}: {V['type_col']} = '{urgent}'.")
    w(f"   A breach means {m1['col']} above {m1['threshold']} ({m1['unit']}). Use breach_readings in {n['g_rfact']}.")
    w(f"   'Last N days' is relative to the latest reading_date in {n['g_rfact']}, not today's date - the data is a demo snapshot.")
    w(f"   {Pa['plural'].capitalize()} are only available as aggregates in {n['g_party']}; never try to identify a person.")
    w("   Show money in AUD. Keep answers short: a one-line summary, then a table. Say which table you used.")
    w("   ```")
    w("4. Select the lakehouse source ▸ **Data source instructions** – paste:")
    w("   ```text")
    w(f"   {n['g_score']}: one row per {E['label']}, risk_score 0-100, risk_band High/Medium/Low, open_{V['table']}s, breach_readings, "
      f"{p['impact']['col']} (illustrative {p['impact']['label']} in AUD - always call it an estimate).")
    w(f"   {n['g_fact']}: one row per {V['label']}; hours_to_close is NULL while open; {p['region_label']} is copied onto each row.")
    w(f"   {n['g_rfact']}: hourly; join to {n['g_dim']} on {E['id']} for names.")
    w("   ```")
    w("5. Still on the lakehouse source ▸ **Example queries** ▸ add these pairs (question, then SQL):")
    w("")
    for q, sql in sqls:
        w(f"   **{q}**")
        w("   ```sql")
        for line in sql.splitlines():
            w("   " + line)
        w("   ```")
    w("   Semantic model sources don't take example queries – the AI instructions from step 1 do that job.")
    w("6. Test two or three questions from section 4, open **Show steps** to see the generated query, then **Publish**.")
    w("   Publishing gives you a URL you can share, and the agent can be added to Copilot in Microsoft 365 or "
      "Microsoft Foundry (use the Fabric IQ tool there).")
    w("")
    w("> The agent is read-only and runs as the person asking. RLS, OneLake security and Purview DLP still apply. "
      "That's a good governance moment in the demo.")
    w("")
    w(f"## 3. Ontology `{onto}` (4 min, preview)")
    w("")
    w(f"1. Open the semantic model `{model}` ▸ **Generate ontology** (or New item ▸ Ontology ▸ *from semantic model*). "
      "You need Build permission on the model.")
    w(f"2. Name it `{onto}` (letters, numbers and underscores only).")
    w("3. Fabric creates entity types from the tables, properties from the columns, relationships from the model "
      "relationships, and metrics from the DAX measures. Rename the entity types so they read like the business:")
    w("")
    w("   | Generated from | Rename to | Key |")
    w("   |---|---|---|")
    w(f"   | `{n['g_dim']}` | **{ent_o}** | `{E['id']}` (display: `{E['name_col']}`) |")
    w(f"   | `{n['g_fact']}` | **{ev_o}** | `{V['id']}` |")
    w(f"   | `{n['g_score']}` | **{ent_o}Risk** | `{E['id']}` |")
    w(f"   | `dim_date` | **Day** | `date_key` |")
    w("")
    w(f"4. Name the relationships in plain words: **{ev_o} → raised against → {ent_o}**, **{ent_o} → has risk → {ent_o}Risk**.")
    w(f"5. Add the people side: **New entity type** `{pa_o}` ▸ bind to lakehouse **lh_mdf** table "
      f"`silver.{Pa['table']}` ▸ key `{Pa['id']}` ▸ keep `{Pa['segment_col']}`, `{reg}`, `status`, `{Pa['id']}`. "
      f"Then add the relationship **{pa_o} → linked to → {ent_o}** on `{E['id']}`.")
    w("   The PII was masked in Silver (names dropped, email and mobile masked), so this is safe to show.")
    w(f"6. Time series (optional, 1 min): on **{ent_o}**, add a time-series property bound to "
      f"`gold.{n['g_rfact']}` with timestamp `reading_hour`, key `{E['id']}` and value `max_{m1['col']}`. "
      "Time series aren't generated – you bind them by hand.")
    w("7. Open the **graph** view and click through one high-risk " + E["label"] + ". This is the visual moment.")
    w(f"8. Add the ontology as a source to `{agent}` (or create an ontology agent) and ask the multi-hop questions below.")
    w("")
    w("> Ontology is in preview: menu names and steps change often. If a label differs, look for the same idea nearby.")
    w("")
    w("## 4. Demo prompts")
    w("")
    w("**Copilot in the report or standalone Copilot** (on the semantic model):")
    for q in A["q_model"]:
        w(f"- {q}")
    w(f"- Build a report page with Open {title(V['plural'])} by {p['region_label']} and a monthly trend.")
    w("")
    w(f"**Data agent `{agent}`:**")
    for q in A["q_agent"]:
        w(f"- {q}")
    w(f"- Now show that as a chart.  *(follow-up – shows conversation context)*")
    w("")
    w("**Ontology (graph and agent):**")
    for q in A["q_onto"]:
        w(f"- {q}")
    w("")
    w("**Copilot in notebooks and SQL** (quick extras if there's time):")
    w(f"- In a notebook attached to lh_mdf: *Load gold.{n['g_score']} and plot risk_score by {reg}.*")
    w(f"- In the lakehouse SQL endpoint: *Write a query that finds {E['plural']} with more than 3 open {V['plural']}.*")
    w("")
    w("## What to say while it runs")
    w("")
    w("- The answers are only as good as the Gold layer. This is why the Medallion and data-quality work matters – "
      "the agent never sees the quarantined rows.")
    w("- Semantic model = governed definitions (\"what does *open* mean\"). Agent = conversation over several sources. "
      "Ontology = the business vocabulary and relationships, so questions can hop from " + Pa["label"] + " to "
      + E["label"] + " to " + V["label"] + ".")
    w("- Everything respects the asker's permissions. Show what a user with only the RLS role sees.")
    w(f"- Put a dollar figure on it: *{title(p['impact']['label'])}* turns a risk list into a business case. The rates are "
      f"made up for the demo ({p['impact']['rule']}) – in a real project the business owns them, ideally in a control table.")
    w("")
    w("## If something goes wrong")
    w("")
    w("| Symptom | Fix |")
    w("|---|---|")
    w("| Copilot button greyed out | Capacity is trial or below F2, or the cross-geo settings are off |")
    w("| Agent answers with the wrong table | Add or sharpen the data source instructions and one more example query |")
    w(f"| \"Last 3 days\" returns nothing | The data stops at the last released day; the instructions tell the agent to use MAX(reading_date) |")
    w("| Generate ontology is missing | Tenant setting for ontology is off, or you don't have Build on the model |")
    w("| Low-quality warning in standalone Copilot | Tick **Approved for Copilot** on the semantic model |")
    w("")
    return "\n".join(L) + "\n"


def build(p, kit_dir):
    os.makedirs(os.path.join(kit_dir, "AI"), exist_ok=True)
    with open(os.path.join(kit_dir, "AI", f"{p['id']}.md"), "w", encoding="utf-8", newline="\n") as f:
        f.write(render(p))


if __name__ == "__main__":
    for p in kitlib.all_packs():
        build(p, kitlib.KIT)
    print("AI guides written")
