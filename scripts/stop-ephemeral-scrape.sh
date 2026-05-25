#!/usr/bin/env bash
# Stop detached ephemeral scrape apps (keeps deployed apps running).
set -euo pipefail
echo "Ephemeral GeM scrape apps (detached):"
modal app list 2>&1 | grep -E "gem_contract|ephemeral" || true
echo
echo "To stop one: modal app stop <APP_ID>"
echo "Example old superfast run: modal app stop ap-8aGFSpF263Y4EftiRqsnl3"
