# flights-BRS

Google Flights scraper for **Bristol Airport (BRS)** — finds same-day return "day trip" flights across all airlines.

Part of the [Day Trip Flight Finder](https://flights-web.adam-661.workers.dev) system.

## How it works

1. **Scrapes Google Flights** using the [fast-flights](https://github.com/AWeirdDev/flights) library, which decodes Google's protobuf response format
2. **Searches every BRS destination** (37 routes) for each day of the month, both outbound and return
3. **Stores results in SQLite** with smart caching — only refreshes stale data on subsequent runs
4. **Syncs to Cloudflare D1** so the web frontend can query results instantly
5. **Runs nightly via GitHub Actions** at 3 AM UTC

## Architecture

```
GitHub Actions (cron 3AM UTC)
  │
  ├─ refresher.py          → orchestrates the refresh cycle
  │   ├─ refresh_worker.py → priority queue, search execution
  │   ├─ google_flights.py → patched fast-flights with consent cookie bypass
  │   ├─ rate_limiter.py   → jittered delays, exponential backoff
  │   └─ cache_db.py       → SQLite storage layer
  │
  └─ sync_to_d1.py         → pushes SQLite → Cloudflare D1
```

## Anti-detection

- **3–6 second random delay** between requests (never fixed intervals)
- **10–20 second pause** when switching between destinations
- **30–60 second cooldown** every 50 requests
- **Exponential backoff** on errors (60s → 120s → 240s → 480s)
- **Cookie rotation** across 3 Google consent cookie sets
- **Chrome TLS fingerprint rotation** across 4 versions
- **Interleaved search order** — shuffles destinations per date to avoid patterns
- **Auto-abort** after 5 consecutive errors

## Staleness tiers

Data is refreshed based on how soon the flight is:

| Days until flight | Max cache age |
|---|---|
| 0–3 days | 6 hours |
| 4–7 days | 12 hours |
| 8–14 days | 24 hours |
| 15–30 days | 48 hours |
| 30+ days | 72 hours |

## Destinations (37)

Alicante, Athens, Barcelona, Belfast, Belfast City, Corfu, Dubrovnik, Dublin, Edinburgh, Faro, Fuerteventura, Funchal, Geneva, Glasgow, Gran Canaria, Grenoble, Hurghada, Innsbruck, Krakow, Lanzarote, Lisbon, Malaga, Malta, Marrakech, Newcastle, Palma, Paphos, Paris CDG, Prague, Rhodes, Rome, Salzburg, Sharm el Sheikh, Split, Tenerife, Tirana, Verona

## Local usage

```bash
# Install dependencies
pip install -r requirements.txt

# Refresh a specific month
python refresher.py --month 2026-04

# Refresh specific destinations only
python refresher.py --month 2026-04 --destinations AGP,CDG,FAO

# Sync local cache to Cloudflare D1
export CLOUDFLARE_API_TOKEN=your_token
export CLOUDFLARE_ACCOUNT_ID=your_account_id
export CLOUDFLARE_D1_DATABASE_ID=your_db_id
python sync_to_d1.py
```

## GitHub Actions

The workflow runs automatically at 3 AM UTC daily. To trigger manually:

1. Go to **Actions** → **Nightly Flight Refresh**
2. Click **Run workflow**
3. Optionally specify a month (e.g. `2026-05`)

### Required secrets

| Secret | Description |
|---|---|
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token with D1 edit permission |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare account ID |
| `CLOUDFLARE_D1_DATABASE_ID` | D1 database UUID |

## Scaling to other airports

This repo is a template. To add another UK airport:

1. Fork/copy this repo as `flights-LGW` (or whichever airport)
2. Add the airport's destinations to `destinations.py`
3. Set the `AIRPORT` env var or update `config.py`
4. Add the same Cloudflare secrets
5. All airports share the same D1 database — the web frontend queries across all of them

## Related

- **[flights-web](https://github.com/achetnik/flights-web)** — Cloudflare Workers web frontend
- **[fast-flights](https://github.com/AWeirdDev/flights)** — The Google Flights scraping library used
