#!/usr/bin/env bash
# Ingest directory parquet from Modal volume into Postgres (FTS-only by default).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PARQUET_PATH="${1:-}"
REQUIRE_EMB="${INGEST_REQUIRE_EMBEDDINGS:-0}"
ARGS=()
if [[ -n "$PARQUET_PATH" ]]; then
  ARGS+=(--parquet-path "$PARQUET_PATH")
fi
if [[ "$REQUIRE_EMB" == "1" ]]; then
  ARGS+=(--require-embeddings)
fi
modal run services/api/modal_backend.py::ingest "${ARGS[@]}"
