#!/usr/bin/env bash
# Merge directory_records JSON → one Parquet on Modal volume.
# Uses app gem_contractor_parquet_ingest_v1 (NOT gem_contracts_full_analytics_v1).
# Does not stop Azure backup running on the scrape app.
set -euo pipefail

RUN_ID="${1:-full_2026_20260518T053357Z}"
WAIT="${WAIT:-0}"

echo "=== Merge directory_records → directory_parquet ==="
echo "Run ID: $RUN_ID"
echo "Source: directory_records/$RUN_ID/ (all months under it)"
echo "Output: directory_parquet/GEM_CONTRACTOR_DIRECTORY_${RUN_ID}.parquet"
echo "App:    gem_contractor_parquet_ingest_v1 (no region lock — schedules faster)"
echo "Tip:    Stop stuck run first: modal app stop gem_contractor_parquet_ingest_v1"
echo ""

cd "$(dirname "$0")/../jobs/scrape"

if [[ "$WAIT" == "1" ]]; then
  modal run --timestamps modal_parquet_only.py::merge_directory_parquet_entry \
    --run-id "$RUN_ID" --wait
else
  modal run --detach --timestamps modal_parquet_only.py::merge_directory_parquet_entry \
    --run-id "$RUN_ID"
  echo ""
  echo "Merge running on Modal. Azure backup on gem_contracts_full_analytics_v1 is untouched."
  echo "When merge logs show status=complete, run:"
  echo "  bash scripts/ingest-parquet.sh GEM_CONTRACTOR_DIRECTORY_${RUN_ID}.parquet"
fi
