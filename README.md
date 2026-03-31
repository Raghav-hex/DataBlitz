# DataBlitz ⚡

Weekly AI insights from live government APIs across USA, UK, India, and Brazil.
Browse stories, pick what's interesting, go deeper.

## Stack (all forever-free)

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | Python 3.12 + httpx | Fetch & validate 15 gov APIs |
| Cache | SQLite | 48h stale fallback |
| AI | Puter.js → Claude | Generate narratives |
| Schedule | GitHub Actions | Weekly cron (Sunday 04:00 UTC) |
| Storage | Cloudflare KV | Store digest + narrative JSON |
| Frontend | Cloudflare Pages | Insights browser UI |
| Edge API | Cloudflare Worker | Serve KV data to frontend |

## Data Sources

**USA** — FRED (economic), BLS (labor, no key needed), NOAA (climate), CDC Open Data (health)
**UK** — ONS Beta API, Bank of England, Met Office Hadley, NHS England
**India** — World Bank HNP, OpenAQ (air quality)
**Brazil** — BCB Banco Central, IBGE SIDRA, PAHO/World Bank health

## Setup

```bash
git clone https://github.com/Raghav-hex/DataBlitz.git
cd DataBlitz
pip install -r requirements.txt
cp .env.example .env   # fill in API keys
```

### API Keys needed

| Key | Where to get | Required? |
|-----|-------------|-----------|
| FRED_API_KEY | fred.stlouisfed.org/docs/api/api_key.html | Yes |
| NOAA_CDO_TOKEN | ncdc.noaa.gov/cdo-web/token | Yes |
| PUTER_AUTH_TOKEN | puter.com -> browser console: localStorage.getItem('puter.auth.token') | Yes (AI) |
| BLS_API_KEY | — | No (v1 works without) |
| DATA_GOV_IN_KEY | data.gov.in/user/register | No (World Bank covers India) |

### GitHub Actions Secrets

Add in Settings -> Secrets -> Actions:
FRED_API_KEY, NOAA_CDO_TOKEN, PUTER_AUTH_TOKEN, CF_ACCOUNT_ID, CF_KV_NAMESPACE_ID, CF_API_TOKEN

### Cloudflare Setup

```bash
npm install -g wrangler && wrangler login

# Create KV namespace — paste the returned id into wrangler.toml
wrangler kv:namespace create "DATABLITZ_KV"

# Deploy edge API Worker
wrangler deploy

# Deploy frontend: connect repo in Cloudflare Pages dashboard
# Build command: (none)   Output dir: frontend
```

## Dev

```bash
python scripts/fetch_country.py --country usa   # single country dry-run
python -m ingestion.pipeline --output ./data/digest_latest.json
node ai_engine/index.js                          # needs PUTER_AUTH_TOKEN
# Then open frontend/index.html in browser — loads local narrative_latest.json
```

## Tests

```bash
pytest   # 43 tests
```
