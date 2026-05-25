#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
modal deploy services/api/modal_backend.py
echo "Deployed. Set EXPO_PUBLIC_API_BASE_URL to the printed https URL."
