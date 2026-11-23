# Real-Time Intelligence (RTI) add-on

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

The batch path (Landing > Bronze > Silver > Gold) stays the main story. This add-on shows where streaming
takes over: the same entities, a reading every minute instead of every 15 minutes, and alerts within seconds.
It works for every pack. Set up once per pack in about 20 minutes, then demo it in 30 to 45.

```
 nb_rti_01_stream_simulator --Event Hub--> Eventstream (custom endpoint)
                                                |
                                                v
                            Eventhouse eh_mdf_rt / KQL database eh_mdf_rt
                            RawEvents (dynamic, 7 days)
                               | update policies (at ingestion)
                               v
                            <Reading> and <Alert> typed tables (365 days)
                               | materialized views + detection functions
                               v
                  Real-Time Dashboard  -->  Activator (Teams, email, Power Automate)
```

## What's in this folder

| File | What it is |
|---|---|
| `rti_simulator.py` | One event simulator for all packs (file or Event Hub mode) |
| `<pack>.kql` | All KQL for one pack, in five sections, in run order |
| `README.md` | This file |

The notebook `notebooks/nb_rti_01_stream_simulator.ipynb` embeds the simulator. The `.kql` files come from
`tools/build_rti.py`, so edit the builder rather than the output. `tools/build_kit.py` runs both.

## The packs

Minute offsets are from the start of a simulator run. The default run is 240 simulated minutes, which
covers every incident. The ramp entity, frozen entity and burst region are printed when the run starts.

<!-- packs:start -->
| Pack | Organisation | Reading stream -> table | Alert stream -> table | Primary metric (threshold) | Planted incidents (minute from start) |
|---|---|---|---|---|---|
| `energy` | Southern Grid Transmission | `tx_telemetry` -> `TxTelemetry` | `breaker_trip` -> `BreakerTrip` | `oil_temp_c` > 95 °C | 20 fault value 999.9; 30 oil temperature ramp + PROTECTION_TRIP; 90 frozen sensor; 180 storm cell |
| `telco` | Southern Cross Telecom | `cell_kpi` -> `CellKpi` | `network_alarm` -> `NetworkAlarm` | `prb_util_pct` > 90 % | 20 fault value 999.9; 30 PRB utilisation ramp + LINK_DOWN; 90 frozen sensor; 180 stadium surge |
| `fsi` | Harbour Mutual Bank | `txn_monitor` -> `TxnMonitor` | `fraud_signal` -> `FraudSignal` | `decline_pct` > 30 % | 20 fault value 999.9; 30 declined transaction rate ramp + CARD_TESTING; 90 frozen sensor; 180 card testing burst |
| `manufacturing` | Kestrel Components | `machine_telemetry` -> `MachineTelemetry` | `machine_alarm` -> `MachineAlarm` | `vibration_mm_s` > 7.1 mm/s | 20 fault value 999.9; 30 vibration ramp + OVER_TEMP; 90 frozen sensor; 180 line stoppage |
| `healthcare` | Coastal Health Network | `ward_occupancy` -> `WardOccupancy` | `bed_alert` -> `BedAlert` | `occupancy_pct` > 95 % | 20 fault value 999.9; 30 bed occupancy ramp + ED_OVERCAPACITY; 90 frozen sensor; 180 ED surge |
| `ites` | Bluegum Digital Services | `service_metric` -> `ServiceMetric` | `sla_alert` -> `SlaAlert` | `p95_response_ms` > 2500 ms | 20 fault value -1; 30 p95 response time ramp + SLA_RESPONSE_BREACH; 90 frozen sensor; 180 failed deployment |
| `government` | Riverbend Regional Authority | `sensor_reading` -> `SensorReading` | `sensor_alert` -> `SensorAlert` | `water_level_cm` > 150 cm | 20 fault value 999.9; 30 water level ramp + FLOOD_LEVEL; 90 frozen sensor; 180 storm front |
| `retail` | Wattle & Co | `store_trading` -> `StoreTrading` | `store_alert` -> `StoreAlert` | `out_of_stock_pct` > 8 % | 20 fault value 999.9; 30 out-of-stock lines ramp + OUT_OF_STOCK_SPIKE; 90 frozen sensor; 180 half-price promo rush |
| `digital` | Lyrebird Apps | `usage_metric` -> `UsageMetric` | `platform_alert` -> `PlatformAlert` | `p95_latency_ms` > 800 ms | 20 fault value -1; 30 p95 API latency ramp + API_5XX_SPIKE; 90 frozen sensor; 180 bad release |
<!-- packs:end -->

What each incident looks like:

- **Ramp.** One entity's primary metric climbs past the threshold over 10 minutes, stays there for about
  half an hour, then a CRITICAL alert with the pack's first alert code is raised.
- **Frozen sensor.** Another entity reports the same primary value for 75 minutes. Nothing breaches, which is
  the point: the reading looks fine, but the sensor is dead.
- **Alert burst.** Two alerts a minute for 12 minutes in one region (the pack's storm scenario).
- **Fault values.** A handful of readings carry the pack's sentinel value. The detection functions
  keep them out of the breach and anomaly checks and list them separately.

Normal readings never cross the threshold, so every breach you see is planted.

## Setup

### 1. Eventhouse and tables

1. In the workspace, create an Eventhouse called `eh_mdf_rt`. It comes with a KQL database of the same name.
   Use that.
2. Open a KQL queryset on the database and paste in `rti/<pack>.kql`.
3. Run sections 1, 2 and 3 in order. Blocks are separated by blank lines. Put the cursor in a block and
   press Shift+Enter, one block at a time. Every command is safe to run again.
4. Check it worked: `.show table <Reading> policy update` should show the parse function.

Section 1 creates `RawEvents` (one dynamic column, kept for 7 days), the two typed tables and the update
policies that route `event_type` into them. Section 2 creates the materialized views. Section 3 creates the
detection functions.

### 2. Eventstream

1. Create an Eventstream called `es_mdf_rt`.
2. Source: **Custom endpoint**. Once it's published, open the source, choose the **Event Hub** protocol and
   copy *Connection string-primary key* from the Keys tab.
3. Destination: **Eventhouse**, ingestion mode *Direct ingestion*, database `eh_mdf_rt`, existing table
   `RawEvents`, format JSON, existing mapping `RawEventsMapping`.
4. Optional: a second destination to **Activator**, filtered on the alert `event_type`.
5. Publish.

Store the connection string in Key Vault if you can. The notebook reads it with `key_vault_url` and
`key_vault_secret`.

### 3. Send events

**From Fabric.** Import `notebooks/nb_rti_01_stream_simulator.ipynb`, attach `lh_mdf` and run it after
`nb_mdf_00` has loaded the pack. Set `connection_string` (or the Key Vault pair). With the defaults
(`minutes = 240`, `speed = 60`) it takes about 4 minutes and reaches every incident. If the batch run has
already built Silver, the stream uses the same entities. If not, it falls back to ids 100001 onwards.

**From a laptop:**

```
python rti/rti_simulator.py --pack energy --mode eventhub \
    --connection-string "Endpoint=sb://..." --speed 60
```

**No endpoint (locked-down tenant or offline).** Generate a file and upload it:

```
python rti/rti_simulator.py --pack energy --mode file --out ./rti_out
```

Then in the KQL database: *Get data > Local file*, table `RawEvents`, format JSON, mapping
`RawEventsMapping`. The update policies fire as they do for streaming. The notebook can do the same with
`mode = "file"`; it writes to `Files/rti/` in the lakehouse.

I don't ship the sample files. A 240-minute run is about 1 MB per pack, and generating one takes a second.

### 4. Real-Time Dashboard

Create a Real-Time Dashboard on `eh_mdf_rt`. Add one tile per query in section 4 of the `.kql` file. The
suggested visual is in the tile comment. Set auto-refresh to 30 seconds.

The tiles and functions anchor on the newest event, not `now()`, so they work on an uploaded file as well
as a live stream. For live operations, call the functions with a short lookback, for example
`TxTelemetryBreaches(30m)`.

### 5. Activator

Section 5 of the `.kql` file lists the suggested rules: metric breach, critical alert, alert burst, stuck
or faulty sensor and telemetry stopped. Create them from a dashboard tile (*Set alert*) or from the
Eventstream Activator destination. Send them to a Teams chat you control for the demo.

### 6. Optional extras

- **Join to Gold.** In the KQL database, add a OneLake shortcut to `lh_mdf` > Tables > gold >
  `dim_<entity>` and keep the name. The function `<Reading>BreachesWithContext()` then adds the entity's
  code, name, type and tier from Gold.
- **OneLake availability.** Turn it on for the typed tables, so the lakehouse, Warehouse and Direct Lake
  can read the streaming data without another copy.

## What to show

1. **Same entity, two speeds.** `silver.<reading table>` arrives every 15 minutes through the batch
   framework. The typed table in the Eventhouse fills every minute and is queryable within seconds.
2. **Bronze to Silver with no code.** Events land in `RawEvents`; update policies split and type them at
   ingestion. That's the KQL equivalent of the Bronze to Silver notebook, with no orchestration.
3. **Current state.** The `<Reading>Latest` view answers "where is everything right now".
4. **Detection.** Threshold breach, frozen sensor (standard deviation over 30 minutes), alert burst per region
   per 10 minutes, and `series_decompose_anomalies` against each entity's own baseline.
5. **Act on it.** Activator turns a condition into a Teams message, an email or a Power Automate flow.

## Rebuild

```
python tools/build_rti.py           # all packs
python tools/build_rti.py retail    # one pack
```

This rewrites `rti/<pack>.kql` and the pack table above. `tools/build_kit.py` runs it too, and refreshes
the simulator copy inside the notebook.

## Notes

- Run the setup script once before the session so the tables, functions and update policies are in place.
- Thresholds, alert codes and incident timings are teaching examples, not operating limits.
