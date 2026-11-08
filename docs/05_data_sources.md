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
| Resolution | `time_trunc` = hour / day / month / year. **Nothing finer than 1 hour.** |
| Use | Token-free fallback, cross-validation and hourly backfill. **Not suitable as the main 15-min source.** |

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
