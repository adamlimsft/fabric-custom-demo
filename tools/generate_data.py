"""Synthetic, deliberately imperfect source data for any industry pack in the demo kit.

Every pack has the same shape - three source systems:
  ops  database      : <entity> register (SCD2) + <event> transactions (incremental, late-arriving)
  tel  file drop     : 15-minute <reading> CSV files + an unconfigured daily feed (onboarded live)
  crm  SaaS API      : <party> JSON snapshot with PII
so the framework, notebooks and pipelines run unchanged. All names and identifiers are fictional.

Usage (locally):
  python generate_data.py --pack retail --out ./out/retail
  python generate_data.py --pack energy --out ./out/energy --no-db-extracts
Inside Fabric, nb_mdf_01_generate_sample_data embeds this file and calls build().
Needs only the Python standard library.
"""
import argparse, csv, json, math, os, random
from datetime import date, datetime, timedelta

FIRST = ["Olivia", "Jack", "Charlotte", "Noah", "Amelia", "William", "Isla", "Oliver", "Mia", "Thomas", "Ava", "Lucas",
         "Grace", "Henry", "Chloe", "Leo", "Zoe", "James", "Ruby", "Ethan", "Priya", "Wei", "Aisha", "Mateo", "Hannah", "Arjun"]
LAST = ["Smith", "Jones", "Williams", "Brown", "Wilson", "Taylor", "Nguyen", "Johnson", "Martin", "White", "Anderson",
        "Thompson", "Walker", "Harris", "Lee", "Ryan", "Robinson", "Kelly", "King", "Singh", "Chen", "Patel", "Murphy", "Wright"]
MAIL = ["mail.example.com", "inbox.example.net", "post.example.org", "example.com.au"]
LATE_ID = 199999


def _kv(p):
    return p["entity"], p["event"], p["reading"], p["party"], p["extra"]


def _ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def stable_seed(pack_id, seed):
    return seed * 1000 + sum(ord(c) * (i + 1) for i, c in enumerate(pack_id))


def entities(p, seed=42, start="2026-11-02"):
    """The entity register as at day 1 (deterministic) - also used by the RTI simulator."""
    E = p["entity"]
    r = random.Random(stable_seed(p["id"], seed) + 1)
    d1 = datetime.strptime(start, "%Y-%m-%d")
    reg = p["region_col"]
    rows = []
    for i in range(E["count"]):
        eid = 100001 + i
        t = E["types"][r.randrange(len(E["types"]))]
        nm = f"{p['name_words'][r.randrange(len(p['name_words']))]} {t.replace('_', ' ').title()} {i % 50 + 1:02d}"
        y0, y1 = E["start_years"]
        sd = date(r.randint(y0, y1), r.randint(1, 12), r.randint(1, 28))
        st = r.choices(E["status"], weights=[82, 8, 6, 4])[0]
        lo, hi = E["cap_range"]
        cap = round(lo + (hi - lo) * (r.random() ** 2), 1)
        rows.append({E["id"]: eid, E["code"]: f"{E['code_prefix']}-{eid}", E["name_col"]: nm, E["type_col"]: t,
                     reg: p["regions"][i % len(p["regions"])], "status": st,
                     E["tier_col"]: r.choices([1, 2, 3, 4, 5], weights=[10, 25, 35, 20, 10])[0], E["cap_col"]: cap,
                     E["start_col"]: sd.isoformat(),
                     "last_modified": _ts(d1 - timedelta(days=r.randint(30, 700), minutes=r.randint(0, 1439)))})
    return rows


def monitored(p, ents):
    R, E = p["reading"], p["entity"]
    pool = [e for e in ents if (not R["monitored_types"] or e[E["type_col"]] in R["monitored_types"])
            and e["status"] == E["status"][0] and e[E["type_col"]] in E["types"] and 1 <= e[E["tier_col"]] <= 5]
    return pool[:R["monitored"]]


class Manifest:
    def __init__(self):
        self.items = []

    def add(self, day, obj, issue, rows, expected):
        self.items.append({"day": day, "object": obj, "issue": issue, "rows": rows, "expected_behaviour": expected})


def generate(p, days=3, start="2026-11-02", seed=42):
    """Returns everything in memory: per-day DB rows, file payloads and the manifest."""
    E, V, R, Pa, X = _kv(p)
    reg = p["region_col"]
    r = random.Random(stable_seed(p["id"], seed))
    d0 = datetime.strptime(start, "%Y-%m-%d")
    dates = [d0 + timedelta(days=i) for i in range(days)]
    cutoffs = [d + timedelta(hours=6) for d in dates]           # extract taken at 06:00 on each load date
    m = Manifest()
    out = {"dates": [d.date().isoformat() for d in dates], "entity": {}, "event": {}, "reading": {}, "party": {},
           "extra": {}}

    # ------------------------------------------------------------------ entity (ops database, SCD2)
    ents = entities(p, seed, start)
    cur = {e[E["id"]]: dict(e) for e in ents}
    mon = monitored(p, ents)
    mon_ids = [e[E["id"]] for e in mon]
    bad = [e for e in ents if e[E["id"]] not in mon_ids]
    for e in bad[5:8]:
        cur[e[E["id"]]][E["type_col"]] = "TBC"
    for e, v in zip(bad[9:12], [0, 9, 9]):
        cur[e[E["id"]]][E["tier_col"]] = v
    m.add(1, E["table"], f"3 rows with {E['type_col']} 'TBC'; 3 rows with {E['tier_col']} outside 1-5", 6,
          "Loaded to Silver with DQ warnings (ENT-02, ENT-03) - visible in the DQ scorecard")
    out["entity"][1] = [dict(v) for v in cur.values()]
    drift = E["drift_col"]
    changed2 = r.sample([e for e in ents if e[E["id"]] not in mon_ids[:3]], 25)
    day2 = []
    for e in changed2:
        row = cur[e[E["id"]]]
        if r.random() < 0.6:
            row["status"] = r.choice([s for s in E["status"][:3] if s != row["status"]])
        else:
            row[E["tier_col"]] = min(5, max(1, int(row[E["tier_col"]]) + r.choice([-1, 1])))
        row["last_modified"] = _ts(cutoffs[0] + timedelta(hours=r.randint(1, 20), minutes=r.randint(0, 59)))
        day2.append(dict(row))
    out["entity"][2] = day2
    m.add(2, E["table"], f"25 {E['plural']} changed status or {E['tier_col']}", 25, "New SCD2 versions in Silver")
    if days >= 3:
        day3 = []
        for e in r.sample(ents, 10):
            row = cur[e[E["id"]]]
            row["status"] = r.choice(E["status"][:3])
            row["last_modified"] = _ts(cutoffs[1] + timedelta(hours=r.randint(1, 20), minutes=r.randint(0, 59)))
            day3.append(dict(row))
        tmpl = dict(mon[0])
        tmpl.update({E["id"]: LATE_ID, E["code"]: f"{E['code_prefix']}-{LATE_ID}", E["name_col"]: f"New {tmpl[E['type_col']].title()} 99",
                     "status": E["status"][0], E[ "tier_col"]: 4, E["start_col"]: dates[1].date().isoformat(),
                     "last_modified": _ts(cutoffs[1] + timedelta(hours=3))})
        cur[LATE_ID] = tmpl
        day3.append(dict(tmpl))
        for row in day3:
            row[drift] = r.choice(E["drift_values"])
        out["entity"][3] = day3
        m.add(3, E["table"], f"New column '{drift}' appears in the extract (schema drift); {E['label']} {LATE_ID} is created",
              len(day3), "Bronze absorbs the column and logs SCHEMA DRIFT; Silver ignores it until column_map is updated")

    # ------------------------------------------------------------------ events (ops database, incremental)
    targets = {row[0]: row[2] for row in p["ref"]["rows"]}
    weights = []
    for e in ents:
        w = 1.0 + (e[E["tier_col"]] if isinstance(e[E["tier_col"]], int) else 1) * 0.4 + (2.5 if e[E["id"]] in mon_ids else 0)
        weights.append(w)
    ids = [e[E["id"]] for e in ents]
    seq = [V["id_start"]]

    def new_event(opened, ent_id=None, now=None):
        t = r.choices(V["types"], weights=[30, 30, 25, 15])[0]
        vid = seq[0]; seq[0] += 1
        lo, hi = V["amount_range"]; qlo, qhi = V["qty_range"]
        amt = round(lo + (hi - lo) * (r.random() ** 2.2), 2)
        qty = round(qlo + (qhi - qlo) * (r.random() ** 2), 2)
        prio = 1 if t == V["urgent_type"] else r.choice([2, 3, 3, 4])
        ev = {V["id"]: vid, E["id"]: ent_id or r.choices(ids, weights=weights)[0], V["type_col"]: t, "priority": prio,
              "status": V["status"][0], V["open_ts"]: _ts(opened), V["close_ts"]: None, V["amount_col"]: amt,
              V["qty_col"]: qty, "description": r.choice(V["descriptions"]), "last_modified": _ts(opened)}
        return ev

    def close(ev, now):
        t = ev[V["type_col"]]
        tgt = targets.get(t, 48)
        hrs = max(0.3, r.lognormvariate(math.log(tgt * 0.7), 0.6))
        opened = datetime.strptime(ev[V["open_ts"]], "%Y-%m-%d %H:%M:%S")
        closed = opened + timedelta(hours=hrs)
        if closed > now:
            ev["status"] = V["status"][1]
            ev["last_modified"] = _ts(opened + (now - opened) / 2)
            return
        if r.random() < 0.06:
            ev["status"] = V["status"][3]
            ev["last_modified"] = _ts(closed)
            return
        ev["status"] = V["status"][2]
        ev[V["close_ts"]] = _ts(closed)
        ev["last_modified"] = _ts(closed)

    events = {}
    hist_start = cutoffs[0] - timedelta(days=540)
    for _ in range(1200):
        opened = hist_start + timedelta(minutes=r.randint(0, int((cutoffs[0] - hist_start).total_seconds() // 60) - 1))
        ev = new_event(opened)
        close(ev, cutoffs[0])
        events[ev[V["id"]]] = ev
    evlist = list(events.values())
    neg = r.sample(evlist, 12)
    for ev in neg:
        ev[V["amount_col"]] = -abs(ev[V["amount_col"]])
    closed_ones = [ev for ev in evlist if ev[V["close_ts"]]]
    backw = r.sample(closed_ones, 20)
    for ev in backw:
        o = datetime.strptime(ev[V["open_ts"]], "%Y-%m-%d %H:%M:%S")
        ev[V["close_ts"]] = _ts(o - timedelta(hours=r.randint(2, 30)))
    urg = [ev for ev in evlist if ev[V["type_col"]] == V["urgent_type"]]
    for ev in r.sample(urg, min(8, len(urg))):
        ev["priority"] = 2
    out["event"][1] = [dict(ev) for ev in evlist]
    m.add(1, V["table"], f"20 {V['plural']} {V['close_verb']} before they were {V['open_verb']}", 20,
          "Quarantined by EVT-03 (business rule)")
    m.add(1, V["table"], f"12 negative {V['amount_col']} values; 8 {V['urgent_type']} rows not priority 1", 20,
          "Loaded with DQ warnings EVT-04 / EVT-05")
    for k in range(2, days + 1):
        lo_t, hi_t = cutoffs[k - 2], cutoffs[k - 1]
        batch = []
        for _ in range(70):
            opened = lo_t + timedelta(minutes=r.randint(0, 24 * 60 - 1))
            ev = new_event(opened)
            close(ev, hi_t)
            events[ev[V["id"]]] = ev
            batch.append(dict(ev))
        openers = [ev for ev in events.values() if ev["status"] in V["status"][:2]
                   and datetime.strptime(ev[V["open_ts"]], "%Y-%m-%d %H:%M:%S") < lo_t]
        for ev in r.sample(openers, min(30, len(openers))):
            close(ev, hi_t)
            if ev["status"] in V["status"][2:]:
                batch.append(dict(ev))
        if k == 2:
            for _ in range(6):
                ev = new_event(lo_t + timedelta(hours=r.randint(1, 20)), ent_id=LATE_ID)
                events[ev[V["id"]]] = ev
                batch.append(dict(ev))
            m.add(2, V["table"], f"6 {V['plural']} reference {E['label']} {LATE_ID}, which the source hasn't published yet", 6,
                  "Quarantined by EVT-02 (referential) with _retry_eligible = true; auto-resolved on day 3")
        out["event"][k] = batch

    # ------------------------------------------------------------------ readings (file drop, 15-min, insert-only)
    rng = R["metrics"]
    off = {eid: [r.gauss(0, mt["sd"] * 0.4) for mt in rng] for eid in mon_ids}
    frozen_id, breach_id = mon_ids[1], mon_ids[0]
    for k, d in enumerate(dates, start=1):
        files = []
        for part in range(4):
            rows = []
            t0 = d + timedelta(hours=6 * part)
            start_t = t0 - timedelta(hours=1) if (part == 0 and k > 1) else t0   # overlap = re-sent readings
            t = start_t
            while t < t0 + timedelta(hours=6):
                hour = t.hour + t.minute / 60
                diurnal = math.sin((hour - 9) / 24 * 2 * math.pi)
                for eid in mon_ids:
                    vals = []
                    for j, mt in enumerate(rng):
                        v = mt["base"] + off[eid][j] + diurnal * mt["sd"] * 0.8 + r.gauss(0, mt["sd"] * 0.35)
                        v = max(mt["min"], min(mt["max"], v))
                        vals.append(v)
                    if k == 2 and eid == frozen_id:
                        vals[0] = rng[0]["base"] + 1.3
                    if k == min(3, days) and eid == breach_id and 12 <= t.hour < 18:
                        span = (t - (d + timedelta(hours=12))).total_seconds() / (6 * 3600)
                        thr = rng[0]["threshold"]
                        vals[0] = thr + abs(thr - rng[0]["base"]) * (0.05 + 0.6 * span)
                    if r.random() < 0.004:
                        vals[0] = R["sentinel"]
                    dec = lambda x, mt: round(x, 1) if mt["sd"] >= 1 else round(x, 3)
                    rows.append([eid, t.strftime("%d/%m/%Y %H:%M")] + [dec(vals[j], rng[j]) for j in range(len(rng))])
                t += timedelta(minutes=15)
            files.append(rows)
        out["reading"][k] = files
    m.add(1, R["table"], f"About 0.4% of readings carry the sensor fault value {R['sentinel']}", None,
          "Quarantined by RDG-01, never reaches Gold")
    m.add(2, R["table"], f"Each day's first file re-sends the previous hour; {E['label']} {frozen_id} reports a frozen "
          f"{rng[0]['label']} all day", None, "Insert-only MERGE ignores the re-sent rows (idempotent); RTI flags the frozen sensor")
    if days >= 3:
        m.add(3, R["table"], f"{E['label'].capitalize()} {breach_id} ramps above the {rng[0]['threshold']} {rng[0]['unit']} "
              f"threshold between 12:00 and 18:00", None, "breach_readings > 0 in Gold; risk score goes up")

    # ------------------------------------------------------------------ party (SaaS API JSON snapshot, PII)
    people = {}
    for i in range(Pa["count"]):
        f, l = r.choice(FIRST), r.choice(LAST)
        pid = f"{Pa['id_prefix']}{200001 + i}"
        bd = date(r.randint(1940, 2005), r.randint(1, 12), r.randint(1, 28))
        email = f"{f.lower()}.{l.lower()}{r.randint(1, 99)}@{r.choice(MAIL)}"
        pc = str(r.randint(2000, 7999))
        people[pid] = {"Id": pid, "FirstName": f, "LastName": l, "Email": email,
                       "MobilePhone": f"04{r.randint(10, 99)} {r.randint(100, 999)} {r.randint(100, 999)}",
                       "Birthdate": bd.isoformat(), "MailingPostalCode": pc,
                       "_reg": r.choice(p["regions"]), "_seg": r.choices(Pa["segments"], weights=[45, 30, 15, 10])[0],
                       "_ent": r.choice(ids), "_status": Pa["status"][0] if r.random() < 0.9 else Pa["status"][1],
                       "_sens": "".join(str(r.randint(0, 9)) for _ in range(9)),
                       "LastModifiedDate": (d0 - timedelta(days=r.randint(1, 400))).strftime("%Y-%m-%dT%H:%M:%SZ")}
    pl = list(people.values())
    for x in pl[:9]:
        x["Email"] = x["Email"].replace("@", " at ")
    for x in pl[9:15]:
        x["MailingPostalCode"] = r.choice(["NSW", "200", "20000"])
    for x in pl[15:18]:
        x["_seg"] = "Unknown"
    m.add(1, Pa["table"], "9 invalid emails, 6 invalid postcodes, 3 unknown segments", 18, "DQ warnings PTY-02/03/04")

    def sf(x):
        return {"attributes": {"type": Pa["sf_type"], "url": f"/services/data/v61.0/sobjects/{Pa['sf_type']}/{x['Id']}"},
                "Id": x["Id"], "FirstName": x["FirstName"], "LastName": x["LastName"], "Email": x["Email"],
                "MobilePhone": x["MobilePhone"], "Birthdate": x["Birthdate"], "MailingPostalCode": x["MailingPostalCode"],
                _sf(reg): x["_reg"], _sf(Pa["segment_col"]): x["_seg"], _sf(E["id"]): x["_ent"], "Status__c": x["_status"],
                _sf(Pa["sensitive_col"]): x["_sens"], "LastModifiedDate": x["LastModifiedDate"]}
    gone = []
    for k in range(1, days + 1):
        recs = []
        if k == 2:
            for x in r.sample(pl[20:], 15):
                old = sf(x)
                x["_seg"] = r.choice(Pa["segments"])
                x["LastModifiedDate"] = (cutoffs[0] + timedelta(hours=r.randint(1, 20))).strftime("%Y-%m-%dT%H:%M:%SZ")
                if len(recs) < 10:
                    recs.append(old)                     # API paging re-sends the old version as well
        if k == 3:
            gone = [pl[-1]["Id"], pl[-2]["Id"]]
        recs += [sf(x) for x in pl if x["Id"] not in gone]
        out["party"][k] = {"totalSize": len(recs), "done": True, "records": recs}
    m.add(2, Pa["table"], "10 records returned twice by the API (old and new version)", 10, "De-duplicated - newest wins")
    if days >= 3:
        m.add(3, Pa["table"], f"2 {Pa['plural']} missing from the full snapshot", 2, "Soft-deleted in Silver (_is_deleted = true)")

    # ------------------------------------------------------------------ extra feed (unconfigured until onboarded)
    for k, d in enumerate(dates, start=1):
        back = 30 if k == 1 else 1
        rows = []
        for b in range(back, 0, -1):
            od = (d - timedelta(days=b)).date()
            for rg in p["regions"]:
                vals = [round(max(c["min"], min(c["max"], c["base"] + r.gauss(0, c["sd"]))), 1) for c in X["cols"]]
                rows.append([rg, od.strftime("%d/%m/%Y")] + vals + [r.choices(X["cat_values"], weights=[45, 30, 18, 7])[0]])
        out["extra"][k] = rows
    m.add(1, X["src"], f"An unconfigured feed ({X['label']}) lands every day", None,
          "Ignored until nb_mdf_06_onboard_new_source adds its metadata rows")
    out["manifest"] = m.items
    out["late_id"] = LATE_ID
    return out


def _sf(col):
    return "".join(w.capitalize() for w in col.split("_")) + "__c"


def write_files(p, data, out, include_db=True):
    E, V, R, Pa, X = _kv(p)
    S = p["sys"]
    reg = p["region_col"]
    written = []

    def folder(sys_code, obj, d):
        f = os.path.join(out, "landing", sys_code, obj, f"load_date={d}")
        os.makedirs(f, exist_ok=True)
        return f

    def wcsv(path, header, rows, delim=","):
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, delimiter=delim)
            w.writerow(header)
            w.writerows(rows)
        written.append(path)

    ecols = [E["id"], E["code"], E["name_col"], E["type_col"], reg, "status", E["tier_col"], E["cap_col"], E["start_col"], "last_modified"]
    vcols = [V["id"], E["id"], V["type_col"], "priority", "status", V["open_ts"], V["close_ts"], V["amount_col"], V["qty_col"],
             "description", "last_modified"]
    for k, d in enumerate(data["dates"], start=1):
        ymd = d.replace("-", "")
        if include_db:
            cols = ecols + ([E["drift_col"]] if k >= 3 else [])
            wcsv(os.path.join(folder(S["ops"]["code"], E["src"], d), f"{E['src']}_{ymd}.csv"), cols,
                 [[row.get(c) for c in cols] for row in data["entity"][k]])
            wcsv(os.path.join(folder(S["ops"]["code"], V["src"], d), f"{V['src']}_{ymd}.csv"), vcols,
                 [[row.get(c) for c in vcols] for row in data["event"][k]])
        for i, rows in enumerate(data["reading"][k], start=1):
            wcsv(os.path.join(folder(S["tel"]["code"], R["src"], d), f"{R['src']}_{ymd}_{i}.csv"),
                 [E["id"], "reading_time"] + [mt["col"] for mt in R["metrics"]], rows)
        pth = os.path.join(folder(S["crm"]["code"], Pa["src"], d), f"{Pa['src']}_{ymd}.json")
        with open(pth, "w", encoding="utf-8") as fh:
            json.dump(data["party"][k], fh, indent=1)
        written.append(pth)
        xs = S[X.get("sys", "tel")]["code"]
        wcsv(os.path.join(folder(xs, X["src"], d), f"{X['src']}_{ymd}.txt"),
             [reg, "obs_date"] + [c["col"] for c in X["cols"]] + [X["cat_col"]], data["extra"][k], delim="|")
    with open(os.path.join(out, "dq_injection_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump({"pack": p["id"], "organisation": p["org"], "load_dates": data["dates"], "planted_issues": data["manifest"]},
                  fh, indent=1)
    return written


def build(p, out, days=3, start="2026-11-02", seed=42, include_db=True):
    data = generate(p, days, start, seed)
    files = write_files(p, data, out, include_db)
    print(f"Generated {len(files)} files for '{p['id']}' ({p['org']}) under {out}")
    return data


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pack", required=True, help="pack id, e.g. energy, telco, fsi, retail ... (config/<pack>.json)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--start-date", default="2026-11-02")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-db-extracts", action="store_true")
    ap.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config"))
    a = ap.parse_args(argv)
    with open(os.path.join(a.config, f"{a.pack}.json"), encoding="utf-8") as f:
        p = json.load(f)["pack"]
    build(p, a.out, a.days, a.start_date, a.seed, not a.no_db_extracts)


if __name__ == "__main__":
    main()
