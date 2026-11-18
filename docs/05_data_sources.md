# MIBEL-Ops — Data Source Study (free APIs)

> Version 0.1 · 2026-11-02 · Focus: **intraday market, 15-minute resolution**
> Inclusion criterion: **free** access. Free registrations or tokens are accepted; paid plans are not.
>
> ⚠️ Items marked **[VERIFY]** could not be confirmed from public documentation. They are resolved in exploration task **SP-01** (see §6) before the final connectors are written.

---

## 1. Context: which prices exist every 15 minutes

Since **1 October 2025** all MIBEL markets (day-ahead and intraday) trade in **15-minute periods** (96 per day).

| Market | When it trades | Resulting price | Role in MIBEL-Ops |
|---|---|---|---|
| **Day-Ahead** | Single auction at 12:00 on D−1 | 96 quarter-hourly prices for D, published ~13:00 on D−1 | Very strong **input variable**: already known when the intraday forecast is made |
| **Intraday auctions (IDA1, IDA2, IDA3)** | 15:00 D−1 · 22:00 D−1 · 10:00 D | Price per quarter-hour in each session | Input variable and secondary target |
| **Continuous intraday (MIC / SIDC)** | Continuous from D−1 until ~1 h before delivery | Maximum, minimum and **volume-weighted average** of each quarter-hour | **Proposed target variable** |
| **PVPC** | Computed daily by REE | Regulated consumer price | Kept as a captured variable and secondary target |

**Proposed target variable:** volume-weighted average price of the continuous intraday market for each quarter-hour, in €/MWh. The baseline to beat is the **day-ahead price for that same quarter-hour**. An intraday model only adds value if it forecasts better than "intraday will equal day-ahead".

---

## 2. Source catalogue

### 2.1 OMIE — public files ⭐ (source of the target variable)
| Aspect | Detail |
|---|---|
| Access | Direct file download from the [file access portal](https://www.omie.es/es/file-access-list). **No registration or token.** |
| Data | `marginalpdbc`: Spanish day-ahead prices · `marginalpibc`: intraday auction prices · `precios_pibcic`: max/min/weighted average per period of the **continuous intraday** · `precios_pibcic_ronda`: weighted average per round |
| Resolution | 15 min (since 01/10/2025). Hourly before that. |
| History | The last 6 years, with daily files and monthly bundles |
| Format | `;`-delimited text, times in Central European Time (CET). Format described in the [file model](https://www.omie.es/sites/default/files/2025-03/formato_ficheros_inf_pub_136.pdf) |
| Latency | Day-ahead and IDA: minutes after each session. Continuous: **[VERIFY]** whether it is published throughout the day or only at the close |
| Risks | File formats may change; data before Oct-2025 is hourly (history must be harmonised) |
| Use | Target (continuous) · features (day-ahead, IDA) · evaluation |

### 2.2 ESIOS (REE) — the system operator's official API ⭐ (real-time system state)
| Aspect | Detail |
|---|---|
| Access | REST API `api.esios.ree.es`. **Free personal token** requested from REE by email (`x-api-key` header). Python wrapper available: `python-esios`. |
| Data | Hundreds of indicators: actual and forecast demand, real-time generation by technology (wind, solar PV, solar thermal, nuclear, hydro, combined cycle, cogeneration…), **wind and solar forecasts**, international exchanges, PVPC (indicator 1001), market prices, imbalance prices and balancing services |
| Resolution | 5–10 min for real-time demand and generation; 15 min / 1 h depending on the indicator |
| History | Several years. Some users report issues with long ranges, so the backfill must be chunked |
| Latency | Practically real time (minutes) |
| Risks | The exact ID of each indicator must be located **[VERIFY]**, especially whether there is a near-real-time continuous-price indicator; usage limits are not published |
| Use | **Main real-time source** for demand, generation, renewable forecasts and PVPC |

### 2.3 REData / APIdatos (REE) — token-free public API (fallback)
| Aspect | Detail |
|---|---|
| Access | `https://apidatos.ree.es/{lang}/datos/{category}/{widget}`. **No token.** |
| Data | Balance, demand, generation (structure, renewable/non-renewable), exchanges (France, Portugal…), markets (`precios-mercados-tiempo-real`, secondary regulation) |
| Resolution | `time_trunc` = hour / day / month / year. Most widgets are hourly or daily, **but `demanda/demanda-tiempo-real` returns 5-minute demand** (real, forecast, scheduled) — see §7 |
| Use | Token-free fallback, cross-validation and hourly backfill. Token-free source of **real-time demand** until the ESIOS token arrives. |

### 2.4 ENTSO-E Transparency Platform — European API (context and cross-validation)
| Aspect | Detail |
|---|---|
| Access | REST API with `securityToken`. **Free**: register and request access by email (reply in about 3 working days). Python wrapper: `entsoe-py`. |
| Data | Day-ahead prices for **Spain, Portugal and France**, actual and forecast load, actual generation by type, wind and solar forecasts (day-ahead and intraday), cross-border flows |
| Resolution | 15 min / 1 h depending on zone and document **[VERIFY for ES]** |
| Use | **Price and flows with France** (coupling through the interconnection affects the Spanish price), cross-validation of ESIOS and alternative forecasts |

### 2.5 Open-Meteo ⭐ (weather)
| Aspect | Detail |
|---|---|
| Access | No key for **non-commercial** use (CC BY 4.0 licence; the source must be credited if published) |
| Forecast | Hourly: wind at **10, 80, 120 and 180 m** (no 100 m), direct and diffuse radiation, cloud cover, temperature. `minutely_15`: wind at 10/80 m and radiation; for Spain it is **interpolated** from the hourly series (native 15-min data only exists for Central Europe and North America) |
| Leakage-free history | **Historical Forecast API** and **Previous Runs API**: return forecasts as they were issued, which is the correct way to train on what was known at each moment |
| Observation | **Satellite Radiation API** (MSG satellites): observed radiation every 15 min for Europe and Africa, updated hourly |
| Use | Forecasts at the 4 strategic nodes. **Change from the whitepaper:** wind is taken at **80 and 120 m** and interpolated to 100 m (hub height) |

### 2.6 MIBGAS — Iberian gas market
| Aspect | Detail |
|---|---|
| Access | Public files downloadable from the website. No token. |
| Data | Daily gas reference price (PVB), indices and volumes |
| Resolution | Daily |
| Use | Cost ceiling of combined cycles. Capturing it once a day is enough. |

### 2.7 AEMET OpenData (optional)
| Aspect | Detail |
|---|---|
| Access | **Free API key** with registration |
| Data | Observations from official stations and weather warnings |
| Use | Optional: validate Open-Meteo against real observations; enable on a second ingestor |

### 2.8 Calendar
Python library `holidays` (national and regional holidays). Works offline, no API.

### 2.9 Known gaps (no reliable free source)
| Data | Situation | Proposal |
|---|---|---|
| CO₂ price (EUA) | No official free API (EEX and ICE are paid) | Out of v1; scraping public sources while respecting their terms could be studied |
| TTF gas price (European) | Paid | MIBGAS as substitute |

---

## 3. Summary matrix

| Source | Token | Useful resolution | Latency | History | Priority | Role |
|---|---|---|---|---|---|---|
| OMIE files | No | 15 min | Minutes to hours [VERIFY] | 6 years | **M** | Target + DA/IDA prices |
| ESIOS | Free (email) | 5–15 min | Minutes | Years | **M** | Real-time system |
| Open-Meteo | No | 1 h (15 min interpolated) | Hourly | Years (with historical forecasts) | **M** | Weather |
| MIBGAS | No | Daily | Daily | Years | **M** | Gas |
| REData | No | 1 h | Minutes | Years | S | Fallback |
| ENTSO-E | Free (registration) | 15 min / 1 h | Minutes to hours | Years | S | France + validation |
| Open-Meteo Satellite | No | 15 min | Hourly | Recent | C | Observed radiation |
| AEMET | Free (registration) | 10 min / 1 h | Hourly | Years | C | Weather observations |

---

## 4. Variables for the intraday model

| Group | Variable | Source | Available at instant *t* |
|---|---|---|---|
| Target | Volume-weighted average continuous price for quarter *q* | OMIE | After *q* closes [VERIFY] |
| Prices | Day-ahead price for *q* | OMIE | ✅ since ~13:00 on D−1 |
| Prices | IDA1/2/3 price for *q* | OMIE | ✅ after each session |
| Prices | Latest published continuous price | OMIE / ESIOS | ✅ with delay [VERIFY] |
| System | Actual and forecast demand | ESIOS | ✅ real time |
| System | Actual generation by technology (including hydro) | ESIOS | ✅ real time |
| System | **Renewable deviation** = actual generation − day-ahead forecast | ESIOS | ✅ (key intraday variable) |
| System | Thermal gap = demand − (wind + solar + nuclear + hydro) | Computed | ✅ |
| System | Exchange and price with France | ENTSO-E / ESIOS | ✅ |
| Weather | Wind 80/120 m → 100 m, radiation, cloud cover, temperature (4 nodes) | Open-Meteo | ✅ latest forecast |
| Gas | MIBGAS daily price | MIBGAS | ✅ latest day |
| Calendar | Quarter-hour of day, day of week, holiday | `holidays` | ✅ |
| Regulated | PVPC | ESIOS / REData | ✅ (D is published on D−1) |

> **Golden rule (point-in-time):** every data point is stored with its `available_at`, i.e. the moment it became known. When building the features for *t*, only data with `available_at ≤ t` is used. This prevents *leakage*, which is much easier to introduce intraday than day-ahead.

---

## 5. Assignment to ingestors

| Ingestor | Sources | Reason |
|---|---|---|
| **Pi 3B+ (v1)** | ESIOS, OMIE, Open-Meteo, MIBGAS, REData (fallback) | All are lightweight HTTP calls; the volume fits comfortably on the Pi |
| Future ingestor 1 | ENTSO-E + Open-Meteo Satellite | Different information: European context and observed radiation |
| Future ingestor 2 | AEMET | Weather observations |

Each source lives as an **independent connector** with its own contract and its own topic `mibel.raw.<source>`. A new ingestor with a new source only adds its connector and its contract, without touching the rest.

## 6. Exploration tasks (SP-01, release R0)
1. Request the ESIOS and ENTSO-E tokens (they take days; do it as soon as possible).
2. Measure the **real publication latency** of the continuous price on OMIE and look for a real-time equivalent in ESIOS.
3. Locate the ESIOS indicator IDs: demand, generation by technology, wind and solar forecasts, PVPC, exchanges.
4. Check ENTSO-E resolution for the Spanish bidding zone.
5. Test the 1-year backfill: OMIE (hourly → 15-min change in Oct-2025), chunked ESIOS and Open-Meteo Historical Forecast.
6. Measure connector RAM and CPU on the Pi 3B+.

## 7. First probe findings (2026-11-10)
Single manual probes made while preparing [wip/ingestion/01_data_sources_exploration.ipynb](../wip/ingestion/01_data_sources_exploration.ipynb). They must be confirmed over several days in SP-01.

| Source | Finding | Impact |
|---|---|---|
| OMIE | Download URL: `https://www.omie.es/es/file-download?parents=<prefix>&filename=<file>`. IDA files are `marginalpibc_YYYYMMDDSS.1` (`SS` = 01–03); IDA3 covers periods 49–96 | Connector naming settled |
| OMIE | The continuous intraday file `precios_pibcic_YYYYMMDD.1` for 08/11/2026 was **emitted at 02:47 on 09/11** (Latin-1, decimal comma) | **The continuous price is not available in real time from OMIE**: the "last known continuous price" feature lags by ≥ 1 day unless ESIOS offers a real-time equivalent. Affects nowcast features (ADR-04) — to be reviewed after SP-01 |
| REData | `demanda/demanda-tiempo-real` returns 5-min values without a token; `demanda/evolucion` and `generacion/estructura-generacion` require `time_trunc=day` | Real-time demand available before the ESIOS token |
| Open-Meteo | Wind speed defaults to km/h; `wind_speed_unit=ms` is required. Historical Forecast, Previous Runs and Satellite APIs respond without a key | Connector must force the unit |
| MIBGAS | One Excel file per year: `https://www.mibgas.es/en/file-access/MIBGAS_Data_{YEAR}.xlsx?path=AGNO_{YEAR}/XLS` | The sheet with the daily reference price is still [VERIFY] |

### 7.1 First full notebook run (2026-11-11, delivery day 2026-11-09, ESIOS and AEMET keys available)

| Source | Finding | Impact |
|---|---|---|
| OMIE | Second sample: continuous file for 09/11 **emitted at 02:59 on 10/11**. PT columns contain `0.00` when there were no trades (8 of 96 periods) | Confirms next-day publication. `0.00` must be treated as missing, not as a price (new data-gate rule needed) |
| OMIE | Continuous − day-ahead spread on 09/11: mean −27.9, std 47.2, range −151.8 … +42.6 €/MWh | The day-ahead baseline leaves a lot of variance to explain → room for a useful model |
| ESIOS | **1727 "Reference price in the continuous intraday market"**: hourly, last value at 13:41 UTC was the previous day's 22:00 UTC | **No real-time continuous price from ESIOS either.** "Last continuous price" cannot be an intraday feature; it is critical for the nowcast in DG-P06 → **decision pending** |
| ESIOS | 1293 real demand (5 min) · 1295 real-time solar PV (5 min, ~10 min lag) · 541 wind forecast (15 min, up to end of D+1) · **1776/1778/1780 intraday H+3 forecasts of demand, wind and PV** (hourly, ~1.3 h ahead) · 1775/1777/1779 D+1 forecasts (hourly) | Good real-time coverage for both segments. 542 / 10034 (solar forecast) return no data: use 1779/1780 instead |
| ESIOS | **600 day-ahead price includes France** (and PT, DE, BE) at 15 min; 1001 PVPC comes per territory (filter Península) | French price available without ENTSO-E → ENTSO-E priority can drop |
| ESIOS | The API returns only the **latest version** of each forecast (no history of revisions) | Forecast vintages cannot be backfilled: the ingestor must **snapshot them in real time** from the start (point-in-time) |
| REData | Daily generation contains small **negative values** (e.g. coal −94 MWh: auxiliary consumption) | DG-E11 "≥ 0" would wrongly reject them; the rule needs a per-technology tolerance |
| Open-Meteo | Forecast, `minutely_15` and Satellite APIs OK; **Historical Forecast and Previous Runs APIs returned HTTP 503** twice | Needs retry with backoff; re-test before relying on them for the backfill |
| MIBGAS | Daily index sheet: `MIBGAS Indexes` (`Delivery day`, `MIBGAS-ES Index`, `MIBGAS PVB Average Price Index Day-Ahead`) | Resolves the MIBGAS [VERIFY] |
| AEMET | Works with the key (hourly station observations); the nearest station to the Galicia node has no wind (`vv` empty) | Pick the nearest station **with** wind data |

### 7.2 Second run with notebook v2 (2026-11-11, general strike day in Spain)

| Topic | Finding | Impact |
|---|---|---|
| Availability | 25 of 26 datasets OK (ENTSO-E skipped, no token). ESIOS real-time demand and PV arrived with ~8 min lag despite the strike | Sources are usable for a first ingestor |
| Open-Meteo | Historical Forecast and Previous Runs answered after **retries with exponential backoff** (4 retries, factor 2) | Retries are mandatory in the connectors |
| Baselines (09/11) | MAE vs continuous VWAP: **day-ahead 36.7 €/MWh**, **latest IDA 23.3 €/MWh** | The latest IDA price is a much stronger reference than day-ahead → day-ahead stays the official baseline; the latest IDA is reported as a stretch reference (A-26) |
| Data gates | All current DG-E rules pass on 5,845 normalised observations, except DG-E11 (3 negative daily generation values). A tolerance of −1 % of the observed weekly maximum still flags 2 → the tolerance should be based on **installed capacity** | Proposed DG-E11 change; proposed new DG-E16 (continuous no-trade periods = missing) |
| Contract | Long format `source, variable, node, event_time, available_at, value, unit, message_id`: ~214 bytes per JSON message, 1.9 MB in memory for 5,845 rows | Message size is small enough for the Pi and Kafka |
| AEMET | Galicia node now uses the nearest station **with** wind (Lugo, ~35 km) | Station choice must be part of the configuration |

### 7.3 First run of the ingestor node (2026-11-11, development desktop PC, `test` mode)
Code: [nodes/ingestor/](../nodes/ingestor/README.md) · design: ADR-05 in [06_technical_decisions.md](06_technical_decisions.md).

| Finding | Detail | Action |
|---|---|---|
| All enabled datasets work | 18/18 jobs `ok`, 0 observations rejected by DG-E (production profile) | — |
| OMIE missing files | A file not yet published returns **HTTP 404 with an HTML page** | Treated as "not published yet", not as an error |
| ESIOS indicator 600 | Returns every zone (~180 kB per call) | One dataset maps `España → ES` and `Francia → FR`: half the requests |
| REData installed capacity | `generacion/potencia-instalada` returns **HTTP 500** (with and without geo filters) | Dataset disabled; DG-E11 checks only the sign until it works [VERIFY] |
| MIBGAS cost | The yearly Excel (3.3 MB) takes ~1.0 s of CPU to parse on the development PC (70 % of a full pass) | Cadence lowered to every 12 h |
| AEMET stations | Station ids fixed in configuration: Borja 9311X, Lugo 1518A, Mérida 4410X, Fuentes de Andalucía 5656 | One small call per station instead of the national dump |
| Change detection | A second pass only sends new or revised values (e.g. 1 of 36 demand values) | Less traffic and CPU on the Pi |
| Volume | ~110 bytes per observation in batches | — |

### 7.4 ENTSO-E with token (2026-11-18, development desktop PC)
| Finding | Detail | Action |
|---|---|---|
| Exploration notebook | 27 of 27 datasets OK, including ENTSO-E flows (A11) and actual generation per type (A75) | — |
| Ingestor connector | `flow_fr_es`, `flow_es_fr`, `flow_pt_es`: 3/3 jobs `ok`, 273 observations at 15 min, 0 rejected | Enabled by `enabled: auto` as soon as `ENTSOE_TOKEN` exists |
| Curve compression | Documents use curve type A03: repeated values are omitted and must be forward-filled (the notebook's raw parser shows fewer rows) | Done in the connector (`A03_forward_fill` lineage step) |
| Latency | FR→ES flows arrived ~4 h behind ES→FR and PT→ES in the same request window | `available_at` = first seen in live mode, so features only see what had arrived |
| Response time | ~8 s per request, negligible CPU | Hourly cadence is enough |
| Actual generation per type (A75) | Works, but ESIOS already provides real-time wind and PV | Not added to the ingestor for now |

## Sources consulted
- [OMIE · File access](https://www.omie.es/es/file-access-list)
- [OMIE · Public information file model](https://www.omie.es/sites/default/files/2025-03/formato_ficheros_inf_pub_136.pdf)
- [OMIE · Press release on the go-live of the 15-min MTU in the day-ahead market](https://www.omie.es/sites/default/files/2025-10/2501001_go_live_mtu15_md_es_vf.pdf)
- [OMIE · Instruction 2/2025](https://www.omie.es/sites/default/files/2025-09/instruccion_2_2025_es.pdf)
- [REE · APIdatos (REData)](https://www.ree.es/es/datos/apidatos)
- [python-esios (PyPI)](https://pypi.org/project/python-esios/)
- [Energía Chi-cuadrado · Getting ESIOS data through its API (Spanish)](https://www.energychisquared.com/post/c%C3%B3mo-conseguir-datos-de-esios-con-su-api/)
- [ENTSO-E · REST API guide](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide_prod_backup_06_11_2024.html)
- [Open-Meteo · Documentation](https://open-meteo.com/en/docs) · [Satellite Radiation API](https://open-meteo.com/en/docs/satellite-radiation-api) · [Discussion on 15-min historical data](https://github.com/open-meteo/open-meteo/discussions/1311)
