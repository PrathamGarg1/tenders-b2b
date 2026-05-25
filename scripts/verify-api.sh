#!/usr/bin/env bash
set -euo pipefail
API_BASE_URL="${API_BASE_URL:-${EXPO_PUBLIC_API_BASE_URL:-http://localhost:8000}}"
API_BASE_URL="${API_BASE_URL%/}"
echo "Checking $API_BASE_URL"
curl -fsS "$API_BASE_URL/health" | head -c 500
echo
curl -fsS "$API_BASE_URL/search?q=chair&limit=3&mode=fts" | head -c 800
echo
echo "OK"
