#!/usr/bin/env bash
# Apply contractor_contracts schema using DATABASE_URL from .env or environment.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "Set DATABASE_URL in .env or environment" >&2
  exit 1
fi
psql "$DATABASE_URL" -f services/api/sql/contractor_contracts.sql
echo "Schema applied."
