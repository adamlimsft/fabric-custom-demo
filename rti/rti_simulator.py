"""Real-Time Intelligence event simulator for every industry pack in the demo kit.

One generator for all packs. It reads the pack definition (config/<pack>.json -> "pack") and emits flat JSON events:

  * a high-volume reading stream - event_type = reading.table, one event per monitored entity per simulated
    minute: entity id, event_ts, region column and the pack's three reading metrics
  * a low-volume alert stream - event_type = alert.stream, with alert_code (from alert.codes), severity and message

Planted incidents (minute offsets from the start of the run, see incident_plan()):
  RAMP      primary metric climbs above its threshold on one entity, then an alert is raised
  FROZEN    the primary metric stops changing on another entity (stuck sensor)
  BURST     a burst of alerts in one region (the pack's alert.storm scenario)
  SENTINEL  a few readings carry the pack's fault value (reading.sentinel)

Modes:
  file      write JSON lines (<pack>_events.json) for Get data > Local file into the raw table
  eventhub  send to an Eventstream custom endpoint (Event Hub protocol, needs `pip install azure-eventhub`)

  python rti/rti_simulator.py --pack energy --mode file --minutes 240 --out ./rti_out
  python rti/rti_simulator.py --pack retail --mode eventhub --connection-string "Endpoint=sb://..." --speed 60

Locally the entities come from tools/generate_data.py (same ids as the batch sample data). Inside Fabric the notebook
nb_rti_01_stream_simulator embeds everything above the __main__ block and calls entities_from_spark().
File mode needs only the Python standard library.
"""
import argparse, json, math, os, random, sys, time
from datetime import datetime, timedelta, timezone

RUN_MINUTES = 240        # default run length - long enough to show every planted incident


def incident_plan(p):
    """The answer key: what is planted, when, and for how long (minutes from the start)."""
    A, R = p["alert"], p["reading"]
    m1 = R["metrics"][0]
    return [
        {"what": "RAMP", "at": 30, "for": 40, "entity": 0,
         "detail": f"{m1['label']} climbs above {m1['threshold']} {m1['unit']}, then a CRITICAL {A['codes'][0]} alert"},
        {"what": "FROZEN", "at": 90, "for": 75, "entity": 1,
         "detail": f"{m1['label']} stuck at one value (stuck sensor)"},
        {"what": "BURST", "at": 180, "for": 12, "entity": None,
         "detail": f"{A['storm']}: a burst of {A['label']}s in one {p['region_label']}"},
        {"what": "SENTINEL", "at": [20, 120, 200], "for": 1, "entity": 3,
         "detail": f"fault value {R['sentinel']} instead of a real {m1['label']}"},
    ]


def cap(s):
    return s[:1].upper() + s[1:]


def _seed(pack_id, seed):
    return seed * 1000 + sum(ord(c) * (i + 1) for i, c in enumerate(pack_id))


def _iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def _active(inc, minute):
    if isinstance(inc["at"], list):
        return (0, 1) if minute in inc["at"] else None
    if inc["at"] <= minute < inc["at"] + inc["for"]:
        return minute - inc["at"], inc["for"]
    return None


def entities_local(p, seed=42):
    """Monitored entities from the kit's sample data generator (tools/generate_data.py)."""
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.join(here, "..", "tools"))
    import generate_data
    E, reg = p["entity"], p["region_col"]
    mon = generate_data.monitored(p, generate_data.entities(p, seed))
    return [{"id": int(e[E["id"]]), "region": e[reg]} for e in mon]


def entities_fallback(p):
    """Same ids and regions generate_data.entities() gives the first N entities (used when Silver is empty)."""
    regs = p["regions"]
    return [{"id": 100001 + i, "region": regs[i % len(regs)]} for i in range(p["reading"]["monitored"])]


def entities_from_spark(spark, p, log=print):
    """Inside Fabric: entities that already have readings in Silver, with their current region."""
    E, R, reg = p["entity"], p["reading"], p["region_col"]
    sql = (f"SELECT DISTINCT r.{E['id']} AS id, e.{reg} AS region FROM silver.{R['table']} r "
           f"JOIN silver.{E['table']} e ON e.{E['id']} = r.{E['id']} AND e._is_current = true ORDER BY id")
    try:
        rows = [{"id": int(x["id"]), "region": x["region"]} for x in spark.sql(sql).collect()]
    except Exception as ex:   # tables not built yet
        log(f"Silver tables not readable ({type(ex).__name__}) - using fallback ids")
        rows = []
    if not rows:
        rows = entities_fallback(p)
    log(f"{len(rows)} monitored {p['entity']['plural']}: ids {rows[0]['id']}..{rows[-1]['id']}")
    return rows


class Simulator:
    """Generates the stream minute by minute. Deterministic for a given pack, entity list and seed."""

    def __init__(self, p, ents, seed=7):
        if len(ents) < 4:
            raise ValueError("need at least 4 monitored entities")
        self.p, self.ents = p, ents
        self.R, self.A, self.E = p["reading"], p["alert"], p["entity"]
        self.reg = p["region_col"]
        self.rnd = random.Random(_seed(p["id"], seed))
        self.plan = incident_plan(p)
        self.inc = {i["what"]: i for i in self.plan}
        counts = {}
        for e in ents:
            counts[e["region"]] = counts.get(e["region"], 0) + 1
        # burst region: busiest region, but not the region of the RAMP entity so the two stories stay apart
        ranked = sorted(counts, key=lambda r: (-counts[r], r))
        self.burst_region = next((r for r in ranked if r != ents[0]["region"]), ranked[0])
        self.burst_ents = [e for e in ents if e["region"] == self.burst_region]
        self.offset = {e["id"]: self.rnd.uniform(-1, 1) for e in ents}
        self.phase = {e["id"]: self.rnd.uniform(0, 2 * math.pi) for e in ents}
        self.frozen_value = None
        self.alert_seq = 0

    # normal behaviour stays inside 45 % of the gap between base and threshold, so it never breaches
    def _normal(self, k, m, eid, ts):
        span = m["sd"]
        if k == 0:
            span = min(m["sd"], 0.45 * abs(m["threshold"] - m["base"]))
        h = ts.hour + ts.minute / 60.0
        wave = math.sin(2 * math.pi * h / 24.0 + self.phase[eid] + k)
        noise = max(-1.0, min(1.0, self.rnd.gauss(0, 0.5)))
        v = m["base"] + span * (0.4 * self.offset[eid] + 0.4 * wave + 0.2 * noise)
        return max(m["min"], min(m["max"], v))

    def _alert(self, ts, ent, code, severity, message):
        self.alert_seq += 1
        return {"event_type": self.A["stream"], "event_ts": _iso(ts), "alert_id": f"{self.p['prefix']}-{self.alert_seq:06d}",
                self.E["id"]: ent["id"], self.reg: ent["region"], "alert_code": code, "severity": severity,
                "message": message}

    def minute(self, m, ts):
        R, A, rnd = self.R, self.A, self.rnd
        mets = R["metrics"]
        m1 = mets[0]
        thr = m1["threshold"]
        gap = abs(thr - m1["base"])
        out = []
        for i, e in enumerate(self.ents):
            vals = [self._normal(k, met, e["id"], ts) for k, met in enumerate(mets)]
            ramp = _active(self.inc["RAMP"], m) if i == self.inc["RAMP"]["entity"] else None
            if ramp:
                k = min(1.0, (ramp[0] + 1) / 10.0)
                peak = thr + max(0.25 * gap, 0.5 * m1["sd"])
                vals[0] = min(m1["max"], vals[0] + k * (peak - vals[0]) + rnd.uniform(-0.05, 0.1) * gap)
            if i == self.inc["FROZEN"]["entity"]:
                if _active(self.inc["FROZEN"], m):
                    if self.frozen_value is None:
                        self.frozen_value = round(vals[0], 2)
                    vals[0] = self.frozen_value
                else:
                    self.frozen_value = None
            if i == self.inc["SENTINEL"]["entity"] and _active(self.inc["SENTINEL"], m):
                vals[0] = R["sentinel"]
            elif i > 3 and rnd.random() < 0.0004:          # occasional fault value anywhere else
                vals[0] = R["sentinel"]
            ev = {"event_type": R["table"], "event_ts": _iso(ts), self.E["id"]: e["id"], self.reg: e["region"]}
            for met, v in zip(mets, vals):
                ev[met["col"]] = round(v, 2)
            out.append(ev)
        # alerts: the RAMP entity alarms once it has been over the threshold for a while
        ramp = _active(self.inc["RAMP"], m)
        if ramp and ramp[0] == 12:
            e = self.ents[self.inc["RAMP"]["entity"]]
            out.append(self._alert(ts + timedelta(seconds=rnd.randint(5, 50)), e, A["codes"][0], "CRITICAL",
                                   f"{cap(m1['label'])} above {thr} {m1['unit']}"))
        burst = _active(self.inc["BURST"], m)
        if burst:
            for j in range(2):
                e = self.burst_ents[(burst[0] * 2 + j) % len(self.burst_ents)]
                code = A["codes"][(burst[0] + j) % len(A["codes"])]
                out.append(self._alert(ts + timedelta(seconds=rnd.randint(0, 59)), e, code,
                                       rnd.choice(["MAJOR", "MAJOR", "CRITICAL"]), f"{cap(A['storm'])} - {code.lower()}"))
        if rnd.random() < 0.02:                               # background noise
            e = rnd.choice(self.ents)
            code = rnd.choice(A["codes"])
            out.append(self._alert(ts + timedelta(seconds=rnd.randint(0, 59)), e, code, rnd.choice(["MINOR", "WARNING"]),
                                   f"{cap(A['label'])} - {code.lower()}"))
        out.sort(key=lambda x: x["event_ts"])
        return out

    def run(self, start, minutes):
        """Yields (minute_index, [events]) in time order."""
        for m in range(minutes):
            yield m, self.minute(m, start + timedelta(minutes=m))

    def active(self, m):
        return [i["what"] for i in self.plan if _active(i, m)]

    def answer_key(self, start, minutes=None):
        A = self.p["alert"]
        lines = []
        for inc in self.plan:
            at = inc["at"] if isinstance(inc["at"], list) else [inc["at"]]
            when = ", ".join((start + timedelta(minutes=a)).strftime("%H:%M") for a in at)
            who = (f"{self.E['id']} {self.ents[inc['entity']]['id']}" if inc["entity"] is not None
                   else f"{self.reg} {self.burst_region}")
            late = " (not reached - run longer)" if minutes is not None and at[0] >= minutes else ""
            lines.append(f"{inc['what']:9s} {when} UTC  {who}: {inc['detail']}{late}")
        return lines


def _start_for(mode, minutes, speed):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if mode == "file":
        return now - timedelta(minutes=minutes)
    # simulated clock finishes at "now" when the run ends, so event_ts is never in the future
    return now - timedelta(minutes=minutes * (1.0 - 1.0 / max(speed, 1.0)))


def to_file(p, ents, path, minutes=RUN_MINUTES, start=None, seed=7, log=print):
    start = start or _start_for("file", minutes, 1)
    sim = Simulator(p, ents, seed)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    n = 0
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for _, evs in sim.run(start, minutes):
            for e in evs:
                f.write(json.dumps(e, separators=(",", ":"), ensure_ascii=False) + "\n")
                n += 1
    log(f"{n} events -> {path}")
    for line in sim.answer_key(start, minutes):
        log("  " + line)
    return path, n


def to_eventhub(p, ents, connection_string, minutes=RUN_MINUTES, speed=60.0, start=None, seed=7, log=print):
    """speed = simulated minutes per real minute (60 -> one simulated minute per second, 1 -> real time)."""
    from azure.eventhub import EventHubProducerClient, EventData
    start = start or _start_for("eventhub", minutes, speed)
    sim = Simulator(p, ents, seed)
    for line in sim.answer_key(start, minutes):
        log("  " + line)
    producer = EventHubProducerClient.from_connection_string(connection_string)
    sent = 0
    with producer:
        for m, evs in sim.run(start, minutes):
            t0 = time.time()
            batch = producer.create_batch()
            for e in evs:
                data = EventData(json.dumps(e, ensure_ascii=False))
                try:
                    batch.add(data)
                except ValueError:                  # batch full
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(data)
            if len(batch):
                producer.send_batch(batch)
            sent += len(evs)
            if m % 10 == 0:
                log(f"minute {m:4d}  sent {sent:7d}  incidents: {', '.join(sim.active(m)) or '-'}")
            time.sleep(max(0.0, 60.0 / speed - (time.time() - t0)))
    log(f"done - {sent} events sent")
    return sent


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.join(here, "..", "tools"))
    import kitlib
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pack", required=True, help="pack id, e.g. energy, retail")
    ap.add_argument("--mode", choices=["file", "eventhub"], default="file")
    ap.add_argument("--minutes", type=int, default=RUN_MINUTES)
    ap.add_argument("--start", help="UTC start, e.g. 2026-11-04T00:00:00 (default: ends now)")
    ap.add_argument("--out", default=".", help="folder for file mode")
    ap.add_argument("--connection-string", default=os.environ.get("EVENTSTREAM_CONNECTION_STRING"))
    ap.add_argument("--speed", type=float, default=60.0)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    pack = kitlib.load_pack(a.pack)
    ents = entities_local(pack)
    st = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc) if a.start else None
    if a.mode == "file":
        to_file(pack, ents, os.path.join(a.out, f"{pack['id']}_events.json"), a.minutes, st, a.seed)
    else:
        if not a.connection_string:
            sys.exit("--connection-string is required (Eventstream > custom endpoint > Event Hub > Keys)")
        to_eventhub(pack, ents, a.connection_string, a.minutes, a.speed, st, a.seed)
