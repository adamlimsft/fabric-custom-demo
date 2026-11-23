# Adding your own industry (or customer) pack

The nine packs are just nine JSON files. Everything else – metadata rows, sample data, the source database script, MLV SQL, the warehouse scripts, the KQL, the dataflow, the AI guide and the copies embedded in the notebooks – is generated from them. A new pack takes me about 30 minutes.

## The shape every pack follows

| Part | Source system | Example (energy) | Silver write mode |
|---|---|---|---|
| `entity` – the thing you operate | `ops` database | assets | SCD2 (history) |
| `event` – what happens to it | `ops` database | work orders | SCD1, can arrive before its entity |
| `reading` – 15-minute measurements | `tel` file drop | transformer telemetry | insert-only |
| `party` – people or organisations | `crm` SaaS API (JSON) | landholders (PII) | SCD1, full snapshot, soft delete |
| `extra` – a feed nobody configured yet | `tel` (pipe-delimited text) | BoM weather | onboarded live with `nb_mdf_06` |

Keeping the shape the same is what lets one set of notebooks, one pipeline and one set of docs work for every industry. If a customer needs a different shape, that's the point where you write real metadata by hand – the framework itself doesn't care.

## Steps

1. Copy the closest pack: `config/retail.json` → `config/mining.json`.
2. Edit **only the `pack` section**. Keep the keys, change the values:
   - `id` (lower case, no spaces), `industry`, `org` (make it fictional), `short`, `prefix` (two letters, unique)
   - `region_col` and `regions` – the column you'll slice by and use for RLS
   - `sys` – the three source systems and what Fabric option suits each (`fabric` is shown to the audience)
   - `entity`, `event`, `reading`, `party`, `extra` – table and column names, value lists, ranges
   - `reading.metrics[0]` is the one that matters: it has the `threshold` used for breaches, risk score and alerts
   - `ref` – the business-owned target table used by the dataflow demo
   - `impact` – the illustrative dollar figure on the scorecard: `col` (ends in `_aud`), `label`, and three rates: `open_cost` per open event, `breach_cost` per 15-min threshold breach (capped at 20), and `cap_rate` × the entity's `cap_col` × its tier (use 0 if `cap_col` isn't money-like). `rule` is the one-line explanation shown in Power BI and the AI guide
   - `ai` – the audience, example customers and demo prompts for the AI guide
   - delete the other sections (`source_system`, `ingestion_object`, …) – they get regenerated
3. Rebuild and check it (Python 3):
   ```bash
   python tools/build_kit.py
   python tools/local_check.py mining
   ```
   `local_check.py` generates the data, runs three days through a small Python copy of the framework and runs every Gold query in SQLite. It should end with `ALL OK`.
4. Add `"mining"` to `PACK_ORDER` in `tools/kitlib.py` if you want it listed in a fixed place (optional).
5. Import the regenerated `nb_mdf_00` and `nb_mdf_01` into Fabric (or upload `config/mining.json` to `Files/config/` – it overrides the embedded copy) and run with `industry = "mining"`.

## Rules of thumb

- Column names: lower case, underscores, no reserved words (`date`, `order`, `user`, `group`).
- `entity.status`: four values, the first is the "normal" one. `event.status`: four values – first two are open, third is closed, fourth is cancelled.
- `entity.count` around 120–240 and `party.count` around 300 keep the repo small and the demo quick.
- Use real-sounding Australian places, but never real customers, people or identifiers.
