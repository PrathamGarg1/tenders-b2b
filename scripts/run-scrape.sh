#!/usr/bin/env bash
# Production scrape tuned for continuous PDF throughput (listing feeds PDF after every page).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

START_DATE="${START_DATE:-2026-01-01}"
END_DATE="${END_DATE:-}"
RUN_ID="${RUN_ID:-full_2026_$(date -u +%Y%m%dT%H%M%SZ)}"
MAX_CONTRACTS="${MAX_CONTRACTS:-0}"

# --- Nonpreemptible (premium) workers ---
export GEM_DISCOVERY_NONPREEMPTIBLE=1
export GEM_PROCESS_NONPREEMPTIBLE=1

# --- PDF fleet (slow step): maximize ---
export GEM_MAX_PROCESS_CONTAINERS=70
export GEM_CONCURRENT_INPUTS=128
export GEM_PROCESS_MEMORY_MIB=1536

# --- Parallel calendar days (don't starve PDF pool) ---
export GEM_DAY_PIPELINE_MAX_CONTAINERS=18
export GEM_DISCOVERY_MAX_CONTAINERS=16

# --- Listing: fast + cap busy days ---
export GEM_FAST_LIST=1
export GEM_MAX_LIST_PAGES=0
export GEM_MAX_CONTRACTS_PER_DAY=2000
export GEM_DISCOVERY_MEMORY_MIB=2048
export GEM_ORCH_MEMORY_MIB=16384

# --- Continuous PDF: small map waves + flush after every list page ---
export GEM_DISCOVER_PDF_BATCH_SIZE=40
export GEM_FLUSH_PDF_AFTER_EVERY_LIST_PAGE=1

export GEM_SUPERFAST_DATE_CHUNK_DAYS=14

echo "=== GeM directory scrape (PDF-continuous tuning) ==="
echo "  START_DATE=$START_DATE  END_DATE=${END_DATE:-today}  RUN_ID=$RUN_ID"
echo "  PDF workers: GEM_MAX_PROCESS_CONTAINERS=$GEM_MAX_PROCESS_CONTAINERS x GEM_CONCURRENT_INPUTS=$GEM_CONCURRENT_INPUTS"
echo "  Parallel days: GEM_DAY_PIPELINE_MAX_CONTAINERS=$GEM_DAY_PIPELINE_MAX_CONTAINERS"
echo "  PDF batch: GEM_DISCOVER_PDF_BATCH_SIZE=$GEM_DISCOVER_PDF_BATCH_SIZE flush_each_page=$GEM_FLUSH_PDF_AFTER_EVERY_LIST_PAGE"
echo "  Per-day list cap: GEM_MAX_CONTRACTS_PER_DAY=$GEM_MAX_CONTRACTS_PER_DAY"
echo "  Nonpreemptible: discovery=$GEM_DISCOVERY_NONPREEMPTIBLE pdf=$GEM_PROCESS_NONPREEMPTIBLE"
echo

# Stop prior detached scrape if still running (same app name)
OLD_EPHEMERAL=$(modal app list 2>/dev/null | awk '/gem_contracts_full.*ephemeral/{print $1}' | head -1 || true)
if [[ -n "$OLD_EPHEMERAL" ]]; then
  echo "Stopping previous ephemeral scrape: $OLD_EPHEMERAL"
  modal app stop "$OLD_EPHEMERAL" 2>/dev/null || true
  sleep 3
fi

# Deploy with same env so max_containers / day-pipeline caps apply to function decorators.
GEM_DISCOVERY_MAX_CONTAINERS="$GEM_DISCOVERY_MAX_CONTAINERS" \
GEM_MAX_PROCESS_CONTAINERS="$GEM_MAX_PROCESS_CONTAINERS" \
GEM_DAY_PIPELINE_MAX_CONTAINERS="$GEM_DAY_PIPELINE_MAX_CONTAINERS" \
GEM_CONCURRENT_INPUTS="$GEM_CONCURRENT_INPUTS" \
modal deploy jobs/scrape/modal_app.py

modal run --detach --timestamps jobs/scrape/modal_app.py::superfast_directory_extract \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --run-id "$RUN_ID" \
  $([[ "$MAX_CONTRACTS" -gt 0 ]] && echo "--max-contracts $MAX_CONTRACTS")

echo
echo "Spawned detached RUN_ID=$RUN_ID"
echo "  Parquet: /data/directory_parquet/GEM_CONTRACTOR_DIRECTORY_${RUN_ID}.parquet"
echo "  Live contacts: /data/logs/seller_contacts_live_${RUN_ID}.jsonl"
echo "  Logs: modal app logs gem_contracts_full_analytics_v1 --timestamps"
