# Tenders B2B Directory

GeM contractor search — scrape, Postgres, API, Android.

```
apps/mobile/          Expo React Native app
services/api/         FastAPI + Modal deploy + ingest
jobs/scrape/          Modal GeM scrape + PDF extract
scripts/              Operator commands
docs/                 Architecture
```

## Prerequisites

| Account | Use |
|---------|-----|
| [Modal](https://modal.com) | Scrape workers + hosted API |
| Postgres ([Neon](https://neon.tech) etc.) | Search data |
| Modal secret `gem-contractor-directory-secrets` | `DATABASE_URL`, `GEM_COOKIE` (scrape), optional Azure |
| [Expo](https://expo.dev) | EAS Android builds |

## 1. Database schema

```bash
cp .env.example .env
# Edit DATABASE_URL in .env
bash scripts/setup-schema.sh
```

## 2. Data

**Ingest existing parquet on Modal volume:**

```bash
bash scripts/ingest-parquet.sh YOUR_RUN_ID.parquet
```

**Scrape then ingest:**

```bash
# GEM_COOKIE on Modal secret first
bash scripts/run-scrape.sh
bash scripts/merge-directory-parquet.sh YOUR_RUN_ID
bash scripts/ingest-parquet.sh GEM_CONTRACTOR_DIRECTORY_YOUR_RUN_ID.parquet
```

Stop a stuck scrape: `bash scripts/stop-ephemeral-scrape.sh`

## 3. API

```bash
bash scripts/deploy-api.sh
export API_BASE_URL='https://...--gem-contractor-directory-api-api.modal.run'
bash scripts/verify-api.sh
```

Local API:

```bash
cd services/api && pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 4. Mobile app

```bash
cd apps/mobile
npm install
export EXPO_PUBLIC_API_BASE_URL="$API_BASE_URL"
npm run android
npm run typecheck
```

Production build: `npm run build:production` (requires EAS).

## Verify ingest

```sql
SELECT count(*) AS total_rows,
       count(*) FILTER (WHERE is_reject IS NOT TRUE) AS accepted_rows
FROM contractor_contracts;
```
