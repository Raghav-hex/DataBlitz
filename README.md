# DataBlitz 🌍⚡

**AI-Powered Global API Digest** — weekly deep dives where AI queries live government APIs worldwide, crunching economic trends, health stats, climate data, and more into actionable stories you won't find elsewhere.

## Overview

DataBlitz ingests open government data from **4 countries** (USA, UK, India, Brazil) across **4 categories** (Economic, Health, Climate, Social), normalises everything into a canonical schema, and feeds it to an AI narrative engine (Claude via Puter.js) that produces the weekly digest.

```
Government APIs ──► Python Ingestion ──► Cache (SQLite) ──► AI Engine (Puter.js)
                                                                    │
                                              Email ◄── Delivery ◄──┘
                                          Static Site
```

## Data Sources

| Country | Source | Category | Auth |
|---------|--------|----------|------|
| 🇺🇸 USA | FRED (St. Louis Fed) | Economic | API key |
| 🇺🇸 USA | BLS | Economic / Social | Reg. key |
| 🇺🇸 USA | NOAA CDO | Climate | Token |
| 🇺🇸 USA | CDC Open Data | Health | None |
| 🇬🇧 UK | ONS Beta API | Economic / Social | None |
| 🇬🇧 UK | Bank of England | Economic | None |
| 🇬🇧 UK | Met Office Hadley | Climate | None |
| 🇬🇧 UK | NHS England | Health | None |
| 🇮🇳 India | data.gov.in | Economic / Social | API key |
| 🇮🇳 India | World Bank HNP | Health | None |
| 🇮🇳 India | OpenAQ | Climate / Health | None |
| 🇧🇷 Brazil | BCB (Banco Central) | Economic | None |
| 🇧🇷 Brazil | IBGE SIDRA | Economic / Social | None |
| 🇧🇷 Brazil | INPE PRODES | Climate | None (CSV) |
| 🇧🇷 Brazil | PAHO PLISA | Health | None |

## Stack

- **Python 3.12** — ingestion pipeline (httpx async, Pydantic v2, tenacity)
- **Node.js 22 + Puter.js** — AI narrative engine (free Claude access)
- **Cloudflare KV + Pages** — storage and static site hosting
- **Resend** — email newsletter delivery (3k free/month)
- **n8n** — weekly orchestration / scheduling
- **Sentry** — error monitoring
- **Render** — free-tier cron host for Python pipeline
- **SQLite** — local stale-cache layer (48h TTL)

## Project Structure

```
datablitz/
├── ingestion/              # Python data ingestion pipeline
│   ├── schemas.py          # Canonical Pydantic v2 models
│   ├── base.py             # Abstract BaseSource + retry logic
│   ├── cache.py            # Async SQLite stale-cache layer
│   ├── config.py           # pydantic-settings env config
│   └── sources/
│       ├── usa/            # FRED, BLS, NOAA, CDC
│       ├── uk/             # ONS, BoE, Met Office, NHS
│       ├── india/          # data.gov.in, World Bank, OpenAQ
│       └── brazil/         # BCB, IBGE, INPE, PAHO
├── ai_engine/              # Node.js + Puter.js narrative generator
├── delivery/               # Email (Resend) + static site output
├── tests/                  # pytest test suite
├── scripts/                # Manual run / debug scripts
└── data/cache/             # SQLite cache (gitignored)
```

## Setup

```bash
# 1. Clone
git clone https://github.com/Raghav-hex/DataBlitz.git
cd DataBlitz

# 2. Python env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Environment
cp .env.example .env
# Fill in: FRED_API_KEY, BLS_API_KEY, NOAA_CDO_TOKEN, DATA_GOV_IN_KEY

# 4. Test
pytest
```

## API Keys Needed

| Key | Where to get |
|-----|-------------|
| `FRED_API_KEY` | https://fred.stlouisfed.org/docs/api/api_key.html (free) |
| `BLS_API_KEY` | https://data.bls.gov/registrationEngine/ (free) |
| `NOAA_CDO_TOKEN` | https://www.ncdc.noaa.gov/cdo-web/token (free) |
| `DATA_GOV_IN_KEY` | https://data.gov.in/user/register (free) |

All other sources are open — no key required.

## Development

```bash
# Run a single country fetch (dry-run, no AI)
python scripts/fetch_country.py --country usa

# Run full pipeline
python scripts/run_pipeline.py

# Tests with coverage
pytest --cov=ingestion
```

## License

MIT
