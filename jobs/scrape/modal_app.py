"""
Large-scale GeM published-contracts pipeline: list scrape → PDF → pdfplumber extraction → JSONL + Parquet + optional Azure Blob.

Deploy / run:
  modal deploy modal_gem_contracts_full_extract.py
  modal run modal_gem_contracts_full_extract.py::main

Configure workers:
  - Set env GEM_COOKIE on the Modal App / Secret (browser session cookie for gem.gov.in),
    or edit _DEFAULT_COOKIE below for local testing only.
  - List pagination cap (smoke vs full): MAX_LIST_PAGES_PER_DAY defaults to 50. For a full day
    until GeM returns "no record", set env GEM_MAX_LIST_PAGES=0 (or omit cap in PIPELINE).
  - Long date ranges: GEM_DATE_CHUNK_DAYS (0=auto: 7d chunks when range>60d so PDF workers start sooner).
  - Parquet merge: GEM_PARQUET_MERGE_BATCH (default 10000 JSON files per write batch) for huge runs.
  - **Live seller contact stream** (directory): ``GEM_LIVE_CONTACTS_LOG=1`` (default) appends one JSON object per line to ``/data/logs/seller_contacts_live_<RUN_ID>.jsonl`` on each PDF extract (same commit as per-contract JSON). Set ``GEM_LIVE_CONTACTS_LOG=0`` to disable.
  - Directory default: **parallel calendar days** — each day lists GeM then immediately runs PDF/JSON for that day (``directory_day_list_then_pdf``); PDF work does not wait for other days. Opt out: ``GEM_PIPELINE_CHUNK_FIRST=1`` or set ``MAX_DISCOVERED_CONTRACTS`` / ``--max-contracts`` (global cap uses chunk-first coordinator). ``GEM_DAY_PIPELINE_MAX_CONTAINERS`` caps parallel day orchestrators (default ~¼ of list/PDF limits) so PDF workers are not starved when workspace concurrency is tight. ``GEM_DAY_PIPELINE_MEMORY_EXTRA`` bumps RAM for list+PDF day workers.
  - Discovery resilience: checkpoints under /data/discovery_ckpt/<RUN_ID>/ (append-only ``<date>.contracts.jsonl`` + ``<date>.state.json`` + optional ``<date>.complete.json``). Legacy /data/logs/discovery_checkpoints/ is read once and copied forward. GEM_DISCOVERY_CHECKPOINT_BASE overrides the primary folder name. GEM_DISCOVERY_CHECKPOINT=0 to disable; GEM_DISCOVERY_FRESH=1 clears that day (primary + legacy). GEM_DISCOVERY_MEMORY_MIB (default 2048). GEM_DISCOVERY_CHECKPOINT_EVERY_PAGES (default 1). Successful days keep jsonl/state (no delete); ``.complete.json`` skips re-scrape. GEM_DISCOVERY_BUFFER_CONTAINERS / GEM_DISCOVERY_SCALEDOWN_WINDOW (Modal scale guide). GEM_DISCOVERY_MEMORY_SNAPSHOT=1 re-enables snapshots (default off). GEM_DISCOVERY_NONPREEMPTIBLE defaults to 1 (Modal ~3× list pricing); set 0/false/no to allow preemption. SUPERFAST coordinator: GEM_ORCH_MEMORY_MIB (default 16384 MiB) or legacy GEM_V2_ORCH_MEMORY_MIB. SUPERFAST chunk: GEM_SUPERFAST_DATE_CHUNK_DAYS, else GEM_V2_DATE_CHUNK_DAYS, else GEM_DATE_CHUNK_DAYS (default 21). PDF: GEM_PROCESS_MEMORY_MIB (default 1024), retries=2.
  - Optional faster listing for small runs: GEM_FAST_LIST=1 (shorter delays between list pages).
  - Optional Azure: set AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_CONTAINER (default
    container name `gem-contracts`) on the Modal environment so uploads no-op until set.
"""

from __future__ import annotations

import asyncio
import calendar
import datetime as dt
import gzip
import json
import os
import random
import re
import shutil
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import modal

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRAPE_LIB = Path(__file__).resolve().parent / "lib"

# Default cookie placeholder; set env GEM_COOKIE on Modal workers (secret) or edit here.
_DEFAULT_COOKIE = "GeM=YOUR_COOKIE_HERE; TS01dc9e29=YOUR_TS_COOKIE_HERE;"

data_volume = modal.Volume.from_name("gem_contracts_analytics_v1", create_if_missing=True)
runtime_secret = modal.Secret.from_name("gem-contractor-directory-secrets")

MODAL_MOUNT_IGNORE = [
    "**/.venv/**",
    "**/__pycache__/**",
    "**/.pytest_cache/**",
    "**/.mypy_cache/**",
    "**/.ruff_cache/**",
    "**/.git/**",
    "**/.cursor/**",
    "**/.expo/**",
    "**/node_modules/**",
    "apps/mobile/**",
    "scripts/**",
    "docs/**",
    "*.parquet",
    "*.pdf",
    ".env",
]

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "httpx",
        "selectolax",
        "pandas",
        "pdfplumber",
        "PyMuPDF",
        "beautifulsoup4",
        "pyarrow",
        "azure-storage-blob",
        "openai",
        "psycopg[binary]",
    )
    .add_local_dir(
        REPO_ROOT,
        remote_path="/repo",
        ignore=MODAL_MOUNT_IGNORE,
    )
)

app = modal.App(name="gem_contracts_full_analytics_v1")

CONFIG = {
    "LIST_URL": "https://gem.gov.in/view_contracts/contract_details",
    "PDF_API_URL": "https://gem.gov.in/view_contracts/sbtCaptcha",
    "VOLUME_DIR": "/data",
}

MAX_CONTAINERS = max(1, int(os.environ.get("GEM_MAX_PROCESS_CONTAINERS", "120")))
DISCOVERY_MAX_CONTAINERS = max(1, int(os.environ.get("GEM_DISCOVERY_MAX_CONTAINERS", "32")))
_PROCESS_CONTRACT_MAX_INPUTS = max(
    1, min(128, int(os.environ.get("GEM_CONCURRENT_INPUTS", "64")))
)

# Listing workers: default RAM above Modal's 128 MiB default (large HTML list pages + Python heap).
_DISCOVERY_MEMORY_MIB = max(512, int(os.environ.get("GEM_DISCOVERY_MEMORY_MIB", "2048")))
# Default ON: fewer Modal preemptions on discover (Modal nonpreemptible ~3× list pricing).
# Opt out: GEM_DISCOVERY_NONPREEMPTIBLE=0|false|no (read when this module loads at deploy/run).
_np_env = os.environ.get("GEM_DISCOVERY_NONPREEMPTIBLE", "1").strip().lower()
_DISCOVERY_NONPREEMPTIBLE = _np_env not in ("0", "false", "no")
_proc_np_env = os.environ.get(
    "GEM_PROCESS_NONPREEMPTIBLE",
    os.environ.get("GEM_DISCOVERY_NONPREEMPTIBLE", "1"),
).strip().lower()
_PROCESS_NONPREEMPTIBLE = _proc_np_env not in ("0", "false", "no")
# Listing: fewer cold starts between .map() inputs (Modal scaling guide).
_DISCOVERY_BUFFER_CONTAINERS = max(
    0, int(os.environ.get("GEM_DISCOVERY_BUFFER_CONTAINERS", "4"))
)
_DISCOVERY_SCALEDOWN_WINDOW = max(
    60, int(os.environ.get("GEM_DISCOVERY_SCALEDOWN_WINDOW", "600"))
)
_DISCOVERY_MEMORY_SNAPSHOT = os.environ.get("GEM_DISCOVERY_MEMORY_SNAPSHOT", "").lower() in (
    "1",
    "true",
    "yes",
)
_DISCOVERY_FN_RETRIES = max(1, min(10, int(os.environ.get("GEM_DISCOVERY_RETRIES", "3"))))
_PROCESS_FN_RETRIES = max(1, min(10, int(os.environ.get("GEM_PROCESS_RETRIES", "2"))))

_ORCH_MEMORY_MIB = max(
    2048,
    int(
        os.environ.get(
            "GEM_ORCH_MEMORY_MIB",
            os.environ.get("GEM_V2_ORCH_MEMORY_MIB", "16384"),
        )
    ),
)
_PROCESS_MEMORY_MIB = max(512, int(os.environ.get("GEM_PROCESS_MEMORY_MIB", "1024")))
# One worker per calendar day: list that day, then process_contract.map for that day (PDFs start
# without waiting for other days). Cap day orchestrators below raw list/PDF limits: each day worker
# occupies a container while discover + process_contract.map run — if this equals 70 while the
# workspace also caps ~70 total, PDF workers never get slots.
_dpm_raw = os.environ.get("GEM_DAY_PIPELINE_MAX_CONTAINERS", "").strip()
if _dpm_raw:
    _DAY_PIPELINE_MAX_CONTAINERS = max(1, min(120, int(_dpm_raw)))
else:
    _cap_both = min(DISCOVERY_MAX_CONTAINERS, MAX_CONTAINERS)
    _DAY_PIPELINE_MAX_CONTAINERS = max(1, min(_cap_both, min(48, max(4, _cap_both // 4))))
_DAY_PIPELINE_MEMORY_MIB = min(
    8192,
    max(_DISCOVERY_MEMORY_MIB, _PROCESS_MEMORY_MIB)
    + max(0, int(os.environ.get("GEM_DAY_PIPELINE_MEMORY_EXTRA", "512"))),
)

_DISCOVERY_FN_DECORATOR_KW: dict[str, object] = {
    "image": image,
    "region": "ap-south",
    "timeout": 86400,
    "max_containers": DISCOVERY_MAX_CONTAINERS,
    "memory": _DISCOVERY_MEMORY_MIB,
    "retries": _DISCOVERY_FN_RETRIES,
    "nonpreemptible": _DISCOVERY_NONPREEMPTIBLE,
    "enable_memory_snapshot": _DISCOVERY_MEMORY_SNAPSHOT,
    "volumes": {CONFIG["VOLUME_DIR"]: data_volume},
    "secrets": [runtime_secret],
    "scaledown_window": _DISCOVERY_SCALEDOWN_WINDOW,
}
if _DISCOVERY_BUFFER_CONTAINERS > 0:
    _DISCOVERY_FN_DECORATOR_KW["buffer_containers"] = min(
        _DISCOVERY_BUFFER_CONTAINERS, DISCOVERY_MAX_CONTAINERS
    )

_DAY_PIPELINE_FN_DECORATOR_KW: dict[str, object] = {
    "image": image,
    "region": "ap-south",
    "timeout": 86400,
    "max_containers": _DAY_PIPELINE_MAX_CONTAINERS,
    "memory": _DAY_PIPELINE_MEMORY_MIB,
    "retries": _PROCESS_FN_RETRIES,
    "nonpreemptible": _PROCESS_NONPREEMPTIBLE,
    "enable_memory_snapshot": True,
    "volumes": {CONFIG["VOLUME_DIR"]: data_volume},
    "secrets": [runtime_secret],
}

# --- Tunables (edit before deploy / run) ---
# Production default is the contractor directory from 2026-01-01 through today.
# For smoke runs, override with GEM_START_DATE/GEM_END_DATE or GEM_MAX_CONTRACTS.
PIPELINE = {
    "YEAR": 2026,
    "MONTH": 1,
    "START_DAY": 1,
    "END_DAY": None,
    "DATE_RANGE_START": "2026-01-01",
    "DATE_RANGE_END": None,
    "MAX_CONTAINERS": MAX_CONTAINERS,
    # Cap list API pages per calendar day (each page = one POST to contract_details).
    # None = scrape until GeM returns "no record". Use 50 for quick output checks.
    "MAX_LIST_PAGES_PER_DAY": None,
    "MAX_EMPTY_LIST_PAGES_PER_DAY": 10,
    "LIST_PAGE_DELAY_MIN": 1.0,
    "LIST_PAGE_DELAY_MAX": 2.5,
    "LIST_HTTP_TIMEOUT": 180.0,
    "HTTP_RETRIES": 5,
    "PDF_HTTP_RETRIES": 5,
    "HTTP_RETRY_BASE_SLEEP": 2.0,
    "CONTRACT_PARALLELISM": 8,
    "CONTRACT_BATCH_SIZE": 16,
    "SAVE_RAW_PDFS_TO_VOLUME": False,
    "UPLOAD_AZURE_IF_CONFIGURED": False,
    "RUN_NOTES": "directory_full_2026_to_today",
    "RUN_ID": None,
    "USE_CONTRACT_FANOUT": True,
    "DIRECTORY_MODE": True,
    "DIRECTORY_MAX_PDF_PAGES": 4,
    "DIRECTORY_SLOW_FALLBACK": True,
    "DETACHED_SPAWN_MAP": False,
    "BACKFILL_2025_TO_TODAY": False,
    "CONCURRENT_INPUTS_PER_CONTAINER": 32,
    "MAX_DISCOVERED_CONTRACTS": None,
    # Per calendar day: stop listing after this many contracts (PDF starts in batches before cap).
    "MAX_DISCOVERED_CONTRACTS_PER_DAY": 2000,
    # Chunk size for each process_contract.map wave (smaller = steadier PDF load).
    "DISCOVER_PDF_BATCH_SIZE": 40,
    # After every list page, flush any buffered contracts to PDF (keeps PDF fleet busy).
    "FLUSH_PDF_AFTER_EVERY_LIST_PAGE": True,
    # If set (int), caps calendar days per discover wave; else GEM_DATE_CHUNK_DAYS env or auto.
    "DATE_CHUNK_DAYS": None,
}


def load_pipeline_config() -> dict:
    """Merge PIPELINE with environment overrides (Modal secrets / `modal run -e`)."""
    cfg = dict(PIPELINE)
    if os.environ.get("GEM_YEAR"):
        cfg["YEAR"] = int(os.environ["GEM_YEAR"])
    if os.environ.get("GEM_MONTH"):
        cfg["MONTH"] = int(os.environ["GEM_MONTH"])
    if os.environ.get("GEM_START_DAY"):
        cfg["START_DAY"] = int(os.environ["GEM_START_DAY"])
    end = os.environ.get("GEM_END_DAY")
    if end is not None and str(end).strip() != "":
        cfg["END_DAY"] = int(end)
    elif "GEM_FULL_MONTH" in os.environ and os.environ["GEM_FULL_MONTH"].lower() in (
        "1",
        "true",
        "yes",
    ):
        cfg["END_DAY"] = None
    if os.environ.get("GEM_START_DATE"):
        cfg["DATE_RANGE_START"] = os.environ["GEM_START_DATE"]
    if "GEM_END_DATE" in os.environ:
        end_date = os.environ["GEM_END_DATE"].strip()
        cfg["DATE_RANGE_END"] = end_date or None
    if os.environ.get("GEM_RUN_NOTES"):
        cfg["RUN_NOTES"] = os.environ["GEM_RUN_NOTES"]
    if os.environ.get("GEM_RUN_ID"):
        cfg["RUN_ID"] = os.environ["GEM_RUN_ID"]
    if os.environ.get("GEM_SAVE_PDFS", "").lower() in ("0", "false", "no"):
        cfg["SAVE_RAW_PDFS_TO_VOLUME"] = False
    if os.environ.get("GEM_SAVE_PDFS", "").lower() in ("1", "true", "yes"):
        cfg["SAVE_RAW_PDFS_TO_VOLUME"] = True
    if os.environ.get("GEM_AZURE_UPLOAD", "").lower() in ("0", "false", "no"):
        cfg["UPLOAD_AZURE_IF_CONFIGURED"] = False
    if os.environ.get("GEM_CONTRACT_PARALLELISM"):
        cfg["CONTRACT_PARALLELISM"] = max(1, int(os.environ["GEM_CONTRACT_PARALLELISM"]))
    if os.environ.get("GEM_CONTRACT_BATCH_SIZE"):
        cfg["CONTRACT_BATCH_SIZE"] = max(1, int(os.environ["GEM_CONTRACT_BATCH_SIZE"]))
    if os.environ.get("GEM_PDF_HTTP_RETRIES"):
        cfg["PDF_HTTP_RETRIES"] = max(1, int(os.environ["GEM_PDF_HTTP_RETRIES"]))
    if os.environ.get("GEM_HTTP_RETRIES"):
        cfg["HTTP_RETRIES"] = max(1, int(os.environ["GEM_HTTP_RETRIES"]))
    if os.environ.get("GEM_LIST_HTTP_TIMEOUT"):
        cfg["LIST_HTTP_TIMEOUT"] = max(30.0, float(os.environ["GEM_LIST_HTTP_TIMEOUT"]))
    if os.environ.get("GEM_USE_CONTRACT_FANOUT", "").lower() in ("0", "false", "no"):
        cfg["USE_CONTRACT_FANOUT"] = False
    if os.environ.get("GEM_DIRECTORY_MODE", "").lower() in ("1", "true", "yes"):
        cfg["DIRECTORY_MODE"] = True
    if os.environ.get("GEM_DIRECTORY_MAX_PDF_PAGES"):
        cfg["DIRECTORY_MAX_PDF_PAGES"] = max(1, int(os.environ["GEM_DIRECTORY_MAX_PDF_PAGES"]))
    if os.environ.get("GEM_DIRECTORY_SLOW_FALLBACK", "").lower() in ("0", "false", "no"):
        cfg["DIRECTORY_SLOW_FALLBACK"] = False
    if os.environ.get("GEM_DETACHED_SPAWN_MAP", "").lower() in ("1", "true", "yes"):
        cfg["DETACHED_SPAWN_MAP"] = True
    if os.environ.get("GEM_BACKFILL_2025_TO_TODAY", "").lower() in ("1", "true", "yes"):
        cfg["BACKFILL_2025_TO_TODAY"] = True
    if os.environ.get("GEM_CONCURRENT_INPUTS"):
        cfg["CONCURRENT_INPUTS_PER_CONTAINER"] = max(
            1, int(os.environ["GEM_CONCURRENT_INPUTS"])
        )
    if os.environ.get("GEM_MAX_CONTRACTS"):
        mx = str(os.environ["GEM_MAX_CONTRACTS"]).strip().lower()
        if mx in ("", "0", "none", "unlimited", "inf"):
            cfg["MAX_DISCOVERED_CONTRACTS"] = None
        else:
            cfg["MAX_DISCOVERED_CONTRACTS"] = max(1, int(mx))
    if os.environ.get("GEM_MAX_CONTRACTS_PER_DAY"):
        mx_day = str(os.environ["GEM_MAX_CONTRACTS_PER_DAY"]).strip().lower()
        if mx_day in ("", "0", "none", "unlimited", "inf"):
            cfg["MAX_DISCOVERED_CONTRACTS_PER_DAY"] = None
        else:
            cfg["MAX_DISCOVERED_CONTRACTS_PER_DAY"] = max(1, int(mx_day))
    if os.environ.get("GEM_DISCOVER_PDF_BATCH_SIZE"):
        cfg["DISCOVER_PDF_BATCH_SIZE"] = max(1, int(os.environ["GEM_DISCOVER_PDF_BATCH_SIZE"]))

    mlp = os.environ.get("GEM_MAX_LIST_PAGES")
    if mlp is not None:
        mlp = str(mlp).strip().lower()
        if mlp in ("", "0", "none", "unlimited", "inf"):
            cfg["MAX_LIST_PAGES_PER_DAY"] = None
        else:
            cfg["MAX_LIST_PAGES_PER_DAY"] = int(mlp)
    if os.environ.get("GEM_MAX_EMPTY_LIST_PAGES"):
        cfg["MAX_EMPTY_LIST_PAGES_PER_DAY"] = max(1, int(os.environ["GEM_MAX_EMPTY_LIST_PAGES"]))

    if os.environ.get("GEM_DATE_CHUNK_DAYS"):
        raw_dc = os.environ["GEM_DATE_CHUNK_DAYS"].strip().lower()
        if raw_dc in ("", "0", "none", "full", "all"):
            cfg["DATE_CHUNK_DAYS"] = None
        else:
            try:
                cfg["DATE_CHUNK_DAYS"] = max(1, int(raw_dc))
            except ValueError:
                pass

    if os.environ.get("GEM_FAST_LIST", "").lower() in ("1", "true", "yes"):
        cfg["LIST_PAGE_DELAY_MIN"] = 0.35
        cfg["LIST_PAGE_DELAY_MAX"] = 0.85

    return cfg


def _merge_cfg_overrides(cfg: dict, cfg_overrides: dict) -> dict:
    if not cfg_overrides:
        return cfg
    merged = dict(cfg)
    for k, v in cfg_overrides.items():
        if k in merged:
            merged[k] = v
    return merged


def _worker_save_pdfs_default(worker_preset: bool) -> bool:
    """GEM_SAVE_PDFS on the worker overrides; if unset, use worker preset.

    Default **off**: raw PDFs are not stored on the volume; contacts/product still
    come from in-memory PDF parse in ``process_contract``. Set GEM_SAVE_PDFS=1/true
    to also write ``/data/pdfs/<RUN_ID>/``.
    """
    raw = os.environ.get("GEM_SAVE_PDFS")
    if raw is None or str(raw).strip() == "":
        return worker_preset
    return str(raw).strip().lower() not in ("0", "false", "no")


def _resolved_max_discovered_contracts() -> int:
    """0 = unlimited. Resolved on the machine running ``modal run`` and passed into the remote worker."""
    raw = os.environ.get("GEM_MAX_CONTRACTS", "").strip().lower()
    if raw in ("", "0", "none", "unlimited", "inf"):
        return 0
    try:
        return max(1, int(raw))
    except ValueError:
        return 0


def _resolved_superfast_date_chunk_days() -> int:
    """Read local shell / `modal run` env so chunk size reaches the remote worker (secrets alone do not)."""
    for key in ("GEM_SUPERFAST_DATE_CHUNK_DAYS", "GEM_V2_DATE_CHUNK_DAYS", "GEM_DATE_CHUNK_DAYS"):
        raw = os.environ.get(key, "").strip()
        if not raw:
            continue
        try:
            return max(1, min(60, int(raw)))
        except ValueError:
            continue
    return 21


def _decode_cfg_overrides(cfg_overrides_json: str = "") -> dict:
    if not cfg_overrides_json:
        return {}
    try:
        val = json.loads(cfg_overrides_json)
    except json.JSONDecodeError:
        return {}
    return val if isinstance(val, dict) else {}


def _collect_local_gem_overrides() -> dict:
    """Capture local GEM_* env and map to PIPELINE keys for remote workers."""
    out: dict = {}
    if os.environ.get("GEM_YEAR"):
        out["YEAR"] = int(os.environ["GEM_YEAR"])
    if os.environ.get("GEM_MONTH"):
        out["MONTH"] = int(os.environ["GEM_MONTH"])
    if os.environ.get("GEM_START_DAY"):
        out["START_DAY"] = int(os.environ["GEM_START_DAY"])
    end = os.environ.get("GEM_END_DAY")
    if end is not None and str(end).strip() != "":
        out["END_DAY"] = int(end)
    elif "GEM_FULL_MONTH" in os.environ and os.environ["GEM_FULL_MONTH"].lower() in (
        "1",
        "true",
        "yes",
    ):
        out["END_DAY"] = None
    if os.environ.get("GEM_START_DATE"):
        out["DATE_RANGE_START"] = os.environ["GEM_START_DATE"]
    if "GEM_END_DATE" in os.environ:
        end_date = os.environ["GEM_END_DATE"].strip()
        out["DATE_RANGE_END"] = end_date or None
    if os.environ.get("GEM_RUN_NOTES"):
        out["RUN_NOTES"] = os.environ["GEM_RUN_NOTES"]
    if os.environ.get("GEM_RUN_ID"):
        out["RUN_ID"] = os.environ["GEM_RUN_ID"]
    if os.environ.get("GEM_SAVE_PDFS", "").lower() in ("0", "false", "no"):
        out["SAVE_RAW_PDFS_TO_VOLUME"] = False
    if os.environ.get("GEM_SAVE_PDFS", "").lower() in ("1", "true", "yes"):
        out["SAVE_RAW_PDFS_TO_VOLUME"] = True
    if os.environ.get("GEM_AZURE_UPLOAD", "").lower() in ("0", "false", "no"):
        out["UPLOAD_AZURE_IF_CONFIGURED"] = False
    if os.environ.get("GEM_CONTRACT_PARALLELISM"):
        out["CONTRACT_PARALLELISM"] = max(1, int(os.environ["GEM_CONTRACT_PARALLELISM"]))
    if os.environ.get("GEM_CONTRACT_BATCH_SIZE"):
        out["CONTRACT_BATCH_SIZE"] = max(1, int(os.environ["GEM_CONTRACT_BATCH_SIZE"]))
    if os.environ.get("GEM_PDF_HTTP_RETRIES"):
        out["PDF_HTTP_RETRIES"] = max(1, int(os.environ["GEM_PDF_HTTP_RETRIES"]))
    if os.environ.get("GEM_HTTP_RETRIES"):
        out["HTTP_RETRIES"] = max(1, int(os.environ["GEM_HTTP_RETRIES"]))
    if os.environ.get("GEM_LIST_HTTP_TIMEOUT"):
        out["LIST_HTTP_TIMEOUT"] = max(30.0, float(os.environ["GEM_LIST_HTTP_TIMEOUT"]))
    if os.environ.get("GEM_USE_CONTRACT_FANOUT", "").lower() in ("0", "false", "no"):
        out["USE_CONTRACT_FANOUT"] = False
    if os.environ.get("GEM_DIRECTORY_MODE", "").lower() in ("1", "true", "yes"):
        out["DIRECTORY_MODE"] = True
    if os.environ.get("GEM_DIRECTORY_MAX_PDF_PAGES"):
        out["DIRECTORY_MAX_PDF_PAGES"] = max(1, int(os.environ["GEM_DIRECTORY_MAX_PDF_PAGES"]))
    if os.environ.get("GEM_DIRECTORY_SLOW_FALLBACK", "").lower() in ("0", "false", "no"):
        out["DIRECTORY_SLOW_FALLBACK"] = False
    if os.environ.get("GEM_DETACHED_SPAWN_MAP", "").lower() in ("1", "true", "yes"):
        out["DETACHED_SPAWN_MAP"] = True
    if os.environ.get("GEM_BACKFILL_2025_TO_TODAY", "").lower() in ("1", "true", "yes"):
        out["BACKFILL_2025_TO_TODAY"] = True
    if os.environ.get("GEM_CONCURRENT_INPUTS"):
        out["CONCURRENT_INPUTS_PER_CONTAINER"] = max(
            1, int(os.environ["GEM_CONCURRENT_INPUTS"])
        )
    if os.environ.get("GEM_MAX_CONTRACTS"):
        mx = str(os.environ["GEM_MAX_CONTRACTS"]).strip().lower()
        if mx in ("", "0", "none", "unlimited", "inf"):
            out["MAX_DISCOVERED_CONTRACTS"] = None
        else:
            out["MAX_DISCOVERED_CONTRACTS"] = max(1, int(mx))
    mlp = os.environ.get("GEM_MAX_LIST_PAGES")
    if mlp is not None:
        mlp_s = str(mlp).strip().lower()
        if mlp_s in ("", "0", "none", "unlimited", "inf"):
            out["MAX_LIST_PAGES_PER_DAY"] = None
        else:
            out["MAX_LIST_PAGES_PER_DAY"] = int(mlp_s)
    if os.environ.get("GEM_MAX_EMPTY_LIST_PAGES"):
        out["MAX_EMPTY_LIST_PAGES_PER_DAY"] = max(1, int(os.environ["GEM_MAX_EMPTY_LIST_PAGES"]))
    if os.environ.get("GEM_FAST_LIST", "").lower() in ("1", "true", "yes"):
        out["LIST_PAGE_DELAY_MIN"] = 0.35
        out["LIST_PAGE_DELAY_MAX"] = 0.85
    if os.environ.get("GEM_DATE_CHUNK_DAYS"):
        raw_dc = os.environ["GEM_DATE_CHUNK_DAYS"].strip().lower()
        if raw_dc not in ("", "0", "none", "full", "all"):
            try:
                out["DATE_CHUNK_DAYS"] = max(1, int(raw_dc))
            except ValueError:
                pass
    return out


def _setup_path() -> None:
    for p in ("/repo/jobs/scrape/lib", "/repo"):
        if p not in sys.path:
            sys.path.insert(0, p)


def get_headers() -> dict[str, str]:
    cookie = os.environ.get("GEM_COOKIE", _DEFAULT_COOKIE)
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": cookie,
        "Origin": "https://gem.gov.in",
        "Referer": "https://gem.gov.in/view_contracts",
    }


def _ym_parts(date_dd_mm_yyyy: str) -> tuple[str, str]:
    parts = date_dd_mm_yyyy.split("-")
    if len(parts) == 3:
        return parts[2], parts[1]
    return "", ""


async def _sleep_backoff(attempt: int, cfg: dict) -> None:
    base = cfg["HTTP_RETRY_BASE_SLEEP"]
    delay = min(base * (2**attempt), 120.0) + random.uniform(0, 1.5)
    await asyncio.sleep(delay)


def _discovery_run_id(cfg: dict) -> str:
    return str(cfg.get("RUN_ID") or os.environ.get("GEM_RUN_ID") or "discovery_no_run_id")


def _discovery_checkpoint_stem(target_date: str) -> str:
    return target_date.replace("/", "-")


def _discovery_checkpoint_primary_dir(run_id: str) -> Path:
    raw = (os.environ.get("GEM_DISCOVERY_CHECKPOINT_BASE") or "discovery_ckpt").strip().strip("/")
    if not raw or ".." in raw or raw.startswith("."):
        raw = "discovery_ckpt"
    safe_rid = run_id.replace("/", "_").replace("..", "_")
    return Path(CONFIG["VOLUME_DIR"]) / raw / safe_rid


def _discovery_checkpoint_legacy_dir(run_id: str) -> Path:
    safe_rid = run_id.replace("/", "_").replace("..", "_")
    return Path(CONFIG["VOLUME_DIR"]) / "logs" / "discovery_checkpoints" / safe_rid


def _discovery_checkpoint_locations(run_id: str, target_date: str) -> tuple[Path, Path, str]:
    """Primary checkpoint dir (new), legacy dir (old runs), and per-day stem."""
    return (
        _discovery_checkpoint_primary_dir(run_id),
        _discovery_checkpoint_legacy_dir(run_id),
        _discovery_checkpoint_stem(target_date),
    )


def _discovery_migrate_legacy_checkpoints_if_needed(primary: Path, legacy: Path, stem: str) -> None:
    """If this day was scraped under the old path, copy stem files into primary once."""
    src_state = _discovery_ck_state_path(legacy, stem)
    dst_state = _discovery_ck_state_path(primary, stem)
    if dst_state.is_file() or not src_state.is_file():
        return
    primary.mkdir(parents=True, exist_ok=True)
    for suffix in (
        f"{stem}.state.json",
        f"{stem}.contracts.jsonl",
        f"{stem}.json",
        f"{stem}.complete.json",
    ):
        lp = legacy / suffix
        pp = primary / suffix
        if lp.is_file() and not pp.is_file():
            shutil.copy2(lp, pp)


def _discovery_ck_legacy_json(ck_dir: Path, stem: str) -> Path:
    return ck_dir / f"{stem}.json"


def _discovery_ck_complete_path(ck_dir: Path, stem: str) -> Path:
    return ck_dir / f"{stem}.complete.json"


def _discovery_ck_state_path(ck_dir: Path, stem: str) -> Path:
    return ck_dir / f"{stem}.state.json"


def _discovery_ck_jsonl_path(ck_dir: Path, stem: str) -> Path:
    return ck_dir / f"{stem}.contracts.jsonl"


def _discovery_checkpoint_enabled() -> bool:
    return os.environ.get("GEM_DISCOVERY_CHECKPOINT", "1").lower() not in ("0", "false", "no")


def _per_day_contract_cap(cfg: dict) -> int | None:
    """Max contracts to list per calendar day (does not disable parallel day pipeline)."""
    cap = cfg.get("MAX_DISCOVERED_CONTRACTS_PER_DAY")
    if cap is not None:
        return max(1, int(cap))
    cap = cfg.get("MAX_DISCOVERED_CONTRACTS")
    if cap is not None:
        return max(1, int(cap))
    return None


def _discovery_load_legacy_v1_json(path: Path) -> dict[str, object] | None:
    """Single-file checkpoint (v1): rewrites full `out` each flush — slow at scale; kept for resume only."""
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict) or int(data.get("v", 0)) != 1:
        return None
    raw_out = data.get("out")
    if not isinstance(raw_out, list):
        return None
    try:
        next_page = int(data.get("next_page", 1))
        empty_pages = int(data.get("empty_pages", 0))
    except (TypeError, ValueError):
        return None
    if next_page < 1:
        return None
    out: list[dict] = []
    for row in raw_out:
        if isinstance(row, dict) and row.get("contract_no"):
            out.append(dict(row))
    return {"next_page": next_page, "empty_pages": empty_pages, "out": out}


def _discovery_load_contracts_jsonl(path: Path) -> tuple[list[dict], set[str]]:
    out: list[dict] = []
    seen: set[str] = set()
    if not path.is_file():
        return out, seen
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict) and row.get("contract_no"):
                    cno = str(row["contract_no"])
                    if cno in seen:
                        continue
                    seen.add(cno)
                    out.append(dict(row))
    except OSError:
        pass
    return out, seen


def _discovery_clear_checkpoints(ck_dir: Path, stem: str) -> None:
    for p in (
        _discovery_ck_state_path(ck_dir, stem),
        _discovery_ck_jsonl_path(ck_dir, stem),
        _discovery_ck_legacy_json(ck_dir, stem),
        _discovery_ck_complete_path(ck_dir, stem),
        ck_dir / f"{stem}.state.part.tmp",
        ck_dir / f"{stem}.partial.tmp",
    ):
        try:
            if p.is_file():
                p.unlink()
        except OSError:
            pass


async def _discovery_persist_checkpoint_v2(
    ck_dir: Path,
    stem: str,
    *,
    next_page: int,
    empty_pages: int,
    out: list[dict],
    persisted_upto: int,
) -> int:
    """Append only new tail of `out` to jsonl + small state file. Returns len(out) after persist."""
    jsonl_p = _discovery_ck_jsonl_path(ck_dir, stem)
    state_p = _discovery_ck_state_path(ck_dir, stem)
    ck_dir.mkdir(parents=True, exist_ok=True)
    n = len(out)
    if persisted_upto < n:
        chunk = out[persisted_upto:n]
        lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in chunk)
        if lines:
            with open(jsonl_p, "a", encoding="utf-8") as fa:
                fa.write(lines + "\n")
    payload = {"v": 2, "next_page": next_page, "empty_pages": empty_pages}
    stmp = ck_dir / f"{stem}.state.part.tmp"
    stmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    stmp.replace(state_p)
    await data_volume.commit.aio()
    return n


async def _discovery_mark_day_complete(
    ck_dir: Path,
    stem: str,
    *,
    next_page: int,
    empty_pages: int,
    out: list[dict],
    persisted_upto: int,
) -> int:
    """Final flush + ``.complete.json`` so a restarted worker skips re-listing this day."""
    nu = await _discovery_persist_checkpoint_v2(
        ck_dir,
        stem,
        next_page=next_page,
        empty_pages=empty_pages,
        out=out,
        persisted_upto=persisted_upto,
    )
    cp = _discovery_ck_complete_path(ck_dir, stem)
    cp.write_text(
        json.dumps(
            {
                "v": 1,
                "next_page": next_page,
                "empty_pages": empty_pages,
                "contracts": len(out),
                "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    await data_volume.commit.aio()
    return nu


async def _discovery_migrate_v1_file_to_v2(ck_dir: Path, stem: str, legacy_path: Path) -> None:
    """One-time migration from monolithic v1 json to v2 layout after resume."""
    loaded = _discovery_load_legacy_v1_json(legacy_path)
    if not loaded:
        return
    out = loaded["out"]  # type: ignore[assignment]
    next_page = int(loaded["next_page"])  # type: ignore[arg-type]
    empty_pages = int(loaded["empty_pages"])  # type: ignore[arg-type]
    ck_dir.mkdir(parents=True, exist_ok=True)
    jsonl_p = _discovery_ck_jsonl_path(ck_dir, stem)
    if jsonl_p.is_file():
        jsonl_p.unlink()
    with open(jsonl_p, "w", encoding="utf-8") as fj:
        for row in out:
            if isinstance(row, dict) and row.get("contract_no"):
                fj.write(json.dumps(row, ensure_ascii=False) + "\n")
    stmp = ck_dir / f"{stem}.state.part.tmp"
    stmp.write_text(
        json.dumps({"v": 2, "next_page": next_page, "empty_pages": empty_pages}, ensure_ascii=False),
        encoding="utf-8",
    )
    stmp.replace(_discovery_ck_state_path(ck_dir, stem))
    try:
        legacy_path.unlink()
    except OSError:
        pass
    await data_volume.commit.aio()


async def download_pdf_bytes(client, contract_no: str, cfg: dict) -> bytes | None:
    from bs4 import BeautifulSoup

    last_err: Exception | None = None
    for attempt in range(cfg.get("PDF_HTTP_RETRIES", cfg["HTTP_RETRIES"])):
        try:
            payload = {"oid": contract_no}
            resp = await client.post(CONFIG["PDF_API_URL"], data=payload)
            if resp.status_code in (429, 500, 502, 503, 504):
                await _sleep_backoff(attempt, cfg)
                continue
            if resp.status_code != 200 or '"status":"1"' not in resp.text:
                return None
            json_data = resp.json()
            soup = BeautifulSoup(json_data["code"], "html.parser")
            link_tag = soup.find("a")
            if not link_tag:
                return None
            pdf_resp = await client.get(link_tag["href"], follow_redirects=True)
            if pdf_resp.status_code in (429, 500, 502, 503, 504):
                await _sleep_backoff(attempt, cfg)
                continue
            if pdf_resp.status_code != 200:
                return None
            return pdf_resp.content
        except Exception as e:
            last_err = e
            await _sleep_backoff(attempt, cfg)
    if last_err:
        print(f"  PDF download failed for {contract_no}: {last_err}")
    return None


@app.function(
    image=image,
    region="ap-south",
    timeout=86400,
    max_containers=MAX_CONTAINERS,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
async def scrape_single_day(target_date: str, cfg_overrides_json: str = "") -> dict:
    import httpx
    from selectolax.lexbor import LexborHTMLParser

    _setup_path()
    from gem_azure import azure_configured, upload_pdf
    from gem_pdf_extract import extract_contract_record
    from gem_run_logging import DayRunStats, redact_sample_record, write_day_summary

    cfg = _merge_cfg_overrides(load_pipeline_config(), _decode_cfg_overrides(cfg_overrides_json))
    log_dir = Path(CONFIG["VOLUME_DIR"]) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    os.makedirs(f"{CONFIG['VOLUME_DIR']}/jsonl", exist_ok=True)
    os.makedirs(f"{CONFIG['VOLUME_DIR']}/jsonl_rejects", exist_ok=True)
    os.makedirs(f"{CONFIG['VOLUME_DIR']}/pdfs/{target_date}", exist_ok=True)

    year_s, month_s = _ym_parts(target_date)
    seen_contracts: set[str] = set()
    page = 1
    empty_pages = 0
    stats = DayRunStats(target_date=target_date)
    cap = cfg.get("MAX_LIST_PAGES_PER_DAY")
    stats.max_list_pages_cap = int(cap) if cap is not None else None

    out_gz = f"{CONFIG['VOLUME_DIR']}/jsonl/gem_{target_date}.jsonl.gz"
    out_reject_gz = f"{CONFIG['VOLUME_DIR']}/jsonl_rejects/gem_rejects_{target_date}.jsonl.gz"

    cap_msg = (
        f"max_list_pages={cap}"
        if cap is not None
        else "max_list_pages=unlimited"
    )
    print(
        f"[START] {target_date} | cfg YEAR={cfg['YEAR']} MONTH={cfg['MONTH']} "
        f"delays={cfg['LIST_PAGE_DELAY_MIN']}-{cfg['LIST_PAGE_DELAY_MAX']} "
        f"{cap_msg} notes={cfg.get('RUN_NOTES', '')}"
    )

    async with httpx.AsyncClient(headers=get_headers(), timeout=90.0) as client:
        with gzip.open(out_gz, "wt", encoding="utf-8") as gz, gzip.open(
            out_reject_gz, "wt", encoding="utf-8"
        ) as reject_gz:
            while True:
                mlp = cfg.get("MAX_LIST_PAGES_PER_DAY")
                if mlp is not None and page > int(mlp):
                    stats.stopped_reason = "max_list_pages_cap"
                    stats.errors.append(f"stopped_at_list_page_cap_{int(mlp)}")
                    print(
                        f"  {target_date}: MAX_LIST_PAGES_PER_DAY={mlp} reached "
                        f"(next list page would be {page}), stopping."
                    )
                    break

                payload = {
                    "fromDate": target_date,
                    "toDate": target_date,
                    "page": page,
                    "department": "",
                    "bno": "",
                    "buyer_category": "",
                }

                try:
                    await asyncio.sleep(
                        random.uniform(
                            cfg["LIST_PAGE_DELAY_MIN"],
                            cfg["LIST_PAGE_DELAY_MAX"],
                        )
                    )

                    resp = None
                    for attempt in range(cfg["HTTP_RETRIES"]):
                        resp = await client.post(CONFIG["LIST_URL"], data=payload)
                        if resp.status_code in (429, 500, 502, 503, 504):
                            await _sleep_backoff(attempt, cfg)
                            continue
                        break
                    if resp is None:
                        stats.list_http_errors += 1
                        stats.errors.append("list_response_none")
                        break

                    if resp.status_code != 200:
                        stats.list_http_errors += 1
                        stats.errors.append(f"list_http_{resp.status_code}")
                        page += 1
                        continue

                    if "no record found" in resp.text.lower():
                        stats.stopped_reason = "gem_no_more_pages"
                        print(f"  {target_date}: GeM returned no more records at list page {page}.")
                        break

                    tree = LexborHTMLParser(resp.text)
                    blocks = tree.css(".border.block")

                    if not blocks:
                        empty_pages += 1
                        max_empty = int(cfg.get("MAX_EMPTY_LIST_PAGES_PER_DAY", 10))
                        if empty_pages >= max_empty:
                            stats.stopped_reason = "max_empty_list_pages"
                            stats.errors.append(f"stopped_after_{max_empty}_empty_list_pages")
                            print(
                                f"  {target_date}: {empty_pages} consecutive list pages had zero "
                                "blocks, stopping."
                            )
                            break
                        page += 1
                        continue
                    empty_pages = 0

                    stats.pages_fetched += 1

                    page_items: list[dict[str, str]] = []
                    for element in blocks:
                        if not cfg.get("DIRECTORY_MODE") and "Bid/RA" not in element.text():
                            continue
                        c_node = element.css_first(".ajxtag_order_number")
                        contract_no = c_node.text(strip=True) if c_node else "N/A"
                        if not contract_no or contract_no == "N/A" or contract_no in seen_contracts:
                            continue
                        item_name = "Various Items"
                        price = "0"
                        table = element.css_first("table.table-striped")
                        if table:
                            rows = table.css("tr")[1:]
                            if rows:
                                cols = rows[0].css("td")
                                if len(cols) >= 5:
                                    item_name = cols[0].text(strip=True)
                                    price = re.sub(r"[^\d.]", "", cols[4].text(strip=True))
                        page_items.append(
                            {
                                "contract_no": contract_no,
                                "primary_item": item_name,
                                "list_price": price or None,
                            }
                        )

                    sem = asyncio.Semaphore(int(cfg.get("CONTRACT_PARALLELISM", 8)))

                    async def process_one(item: dict[str, str]) -> tuple[dict, bytes | None]:
                        async with sem:
                            cno = item["contract_no"]
                            pdf_bytes = await download_pdf_bytes(client, cno, cfg)
                            meta_in = {
                                "contract_no": cno,
                                "list_date": target_date,
                                "primary_item": item.get("primary_item"),
                                "list_price": item.get("list_price"),
                            }
                            rec = extract_contract_record(pdf_bytes or b"", meta_in)
                            return rec, pdf_bytes

                    batch_size = int(cfg.get("CONTRACT_BATCH_SIZE", 16))
                    for i in range(0, len(page_items), batch_size):
                        chunk = page_items[i : i + batch_size]
                        results = await asyncio.gather(
                            *(process_one(it) for it in chunk), return_exceptions=True
                        )
                        for it, result in zip(chunk, results):
                            contract_no = it["contract_no"]
                            if isinstance(result, Exception):
                                stats.errors.append(f"contract_process_error:{contract_no}:{result}")
                                continue
                            rec, pdf_bytes = result
                            if pdf_bytes:
                                stats.pdf_success += 1
                            else:
                                stats.pdf_failed += 1

                            ex = rec.get("extraction") or {}
                            try:
                                tc = int(ex.get("text_chars") or 0)
                            except (TypeError, ValueError):
                                tc = 0
                            if tc == 0:
                                stats.extraction_zero_text += 1

                            if len(stats.sample_contract_nos) < 5:
                                stats.sample_contract_nos.append(contract_no)
                            if len(stats.sample_records_redacted) < 3:
                                stats.sample_records_redacted.append(redact_sample_record(rec))

                            if cfg["SAVE_RAW_PDFS_TO_VOLUME"] and pdf_bytes:
                                safe = re.sub(r"[^\w.-]+", "_", contract_no)
                                pdf_path = f"{CONFIG['VOLUME_DIR']}/pdfs/{target_date}/{safe}.pdf"
                                with open(pdf_path, "wb") as pf:
                                    pf.write(pdf_bytes)

                            if (
                                cfg["UPLOAD_AZURE_IF_CONFIGURED"]
                                and azure_configured()
                                and pdf_bytes
                                and year_s
                                and month_s
                            ):
                                blob_pdf = f"raw-pdfs/{year_s}/{month_s}/{contract_no}.pdf"
                                try:
                                    upload_pdf(pdf_bytes=pdf_bytes, blob_path=blob_pdf)
                                except Exception as e:
                                    print(f"  Azure PDF upload failed {contract_no}: {e}")

                            if (ex.get("is_reject") is True) or ex.get("reject_reasons"):
                                reject_gz.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            else:
                                gz.write(json.dumps(rec, ensure_ascii=False) + "\n")
                                seen_contracts.add(contract_no)

                    stats.contracts_unique = len(seen_contracts)
                    print(
                        f"  {target_date} page {page}: "
                        f"contracts_unique={stats.contracts_unique} "
                        f"pdf_ok={stats.pdf_success} pdf_fail={stats.pdf_failed}"
                    )
                    page += 1

                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                    stats.errors.append(err)
                    print(f"  Error {target_date} page {page}: {e}")
                    await asyncio.sleep(5)

    stats.contracts_unique = len(seen_contracts)
    summary_path = write_day_summary(log_dir, stats)

    events_path = log_dir / "run_events.jsonl"
    with open(events_path, "a", encoding="utf-8") as ev:
        ev.write(
            json.dumps(
                {
                    "event": "day_complete",
                    "date": target_date,
                    "stats": stats.to_dict(),
                    "jsonl": out_gz,
                    "reject_jsonl": out_reject_gz,
                },
                ensure_ascii=False,
            )
            + "\n"
        )

    await data_volume.commit.aio()

    # Upload day's JSONL gzip to Azure (single blob per day)
    if (
        cfg["UPLOAD_AZURE_IF_CONFIGURED"]
        and azure_configured()
        and year_s
        and month_s
        and os.path.isfile(out_gz)
    ):
        try:
            from gem_azure import upload_bytes

            with open(out_gz, "rb") as rf:
                gz_bytes = rf.read()
            blob_jsonl = (
                f"structured/jsonl/{year_s}/{month_s}/gem_{target_date}.jsonl.gz"
            )
            upload_bytes(
                gz_bytes,
                blob_jsonl,
                content_type="application/gzip",
            )
            if os.path.isfile(out_reject_gz):
                with open(out_reject_gz, "rb") as rrf:
                    reject_bytes = rrf.read()
                blob_reject = (
                    f"structured/jsonl_rejects/{year_s}/{month_s}/"
                    f"gem_rejects_{target_date}.jsonl.gz"
                )
                upload_bytes(
                    reject_bytes,
                    blob_reject,
                    content_type="application/gzip",
                )
        except Exception as e:
            print(f"  Azure JSONL upload failed for {target_date}: {e}")

    return {
        "date": target_date,
        "unique_contracts": len(seen_contracts),
        "jsonl_path": out_gz,
        "reject_jsonl_path": out_reject_gz,
        "summary_path": str(summary_path),
        "stats": stats.to_dict(),
    }


def _date_strings_for_month(
    year: int,
    month: int,
    start_day: int,
    end_day: int | None,
) -> list[str]:
    _, last = calendar.monthrange(year, month)
    hi = end_day if end_day is not None else last
    hi = min(max(hi, 1), last)
    lo = max(1, start_day)
    out: list[str] = []
    for d in range(lo, hi + 1):
        out.append(f"{d:02d}-{month:02d}-{year}")
    return out


def _effective_date_chunk_days(n_dates: int, cfg: dict) -> int:
    """Split long calendar ranges so discovery map payloads stay bounded.

    Smaller chunks mean `process_contract` (the only writer of directory JSON)
    starts sooner: the coordinator waits for *all* days in a chunk to finish
    discovery before it runs `process_contract.map` for that chunk.
    """
    dc = cfg.get("DATE_CHUNK_DAYS")
    if dc is not None and str(dc).strip() != "":
        try:
            iv = int(dc)
            if iv > 0:
                return min(iv, n_dates)
        except (TypeError, ValueError):
            pass
    raw = os.environ.get("GEM_DATE_CHUNK_DAYS", "").strip().lower()
    if raw in ("", "0", "none", "full", "all"):
        chunk = 0
    else:
        try:
            chunk = max(1, int(raw))
        except ValueError:
            chunk = 0
    if chunk:
        return min(chunk, n_dates)
    if n_dates > 60:
        # Default 7 (not 30): otherwise discovery for an entire month can run for
        # hours/days with zero `[PROCESS]` / volume JSON until the chunk completes.
        return min(7, n_dates)
    return n_dates


def _date_strings_from_cfg(cfg: dict) -> list[str]:
    if cfg.get("DATE_RANGE_START"):
        start = dt.date.fromisoformat(str(cfg["DATE_RANGE_START"]))
        raw_end = cfg.get("DATE_RANGE_END")
        end = (
            dt.date.today()
            if raw_end in (None, "", "today")
            else dt.date.fromisoformat(str(raw_end))
        )
    elif cfg.get("BACKFILL_2025_TO_TODAY"):
        start = dt.date(2025, 1, 1)
        end = dt.date.today()
    else:
        return _date_strings_for_month(
            cfg["YEAR"], cfg["MONTH"], cfg["START_DAY"], cfg["END_DAY"]
        )

    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%d-%m-%Y"))
        cur += dt.timedelta(days=1)
    return out


@app.function(
    image=image,
    enable_memory_snapshot=True,
    timeout=86400,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
)
def merge_month_parquet(
    year: int,
    month: int,
    outfile_name: str = None,
) -> str | None:
    """Read all JSONL.GZ shards for the month from the volume and write one Parquet file."""
    import pandas as pd

    _setup_path()
    from gem_record_flatten import flatten_for_parquet

    prefix = f"{year}-{month:02d}"
    jsonl_dir = Path(CONFIG["VOLUME_DIR"]) / "jsonl"
    paths = sorted(jsonl_dir.glob(f"gem_*-{month:02d}-{year}.jsonl.gz"))
    if not paths:
        print(f"No JSONL files matching month {prefix}")
        return None

    rows: list[dict] = []
    for p in paths:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                rows.append(flatten_for_parquet(rec))

    if not rows:
        print("No rows to write.")
        return None

    df = pd.DataFrame(rows)
    out_dir = Path(CONFIG["VOLUME_DIR"]) / "parquet"
    out_dir.mkdir(parents=True, exist_ok=True)
    name = outfile_name or f"GEM_CONTRACTS_{year}_{month:02d}.parquet"
    out_path = out_dir / name
    df.to_parquet(out_path, index=False)
    data_volume.commit()
    print(f"Wrote {len(df)} rows to {out_path}")

    try:
        from gem_azure import azure_configured, upload_bytes

        if azure_configured():
            with open(out_path, "rb") as rf:
                pq_bytes = rf.read()
            upload_bytes(
                pq_bytes,
                f"structured/parquet/{year}/{month:02d}/{name}",
                content_type="application/vnd.apache.parquet",
            )
            print("Uploaded Parquet to Azure Blob.")
    except Exception as e:
        print(f"Azure Parquet upload skipped/failed: {e}")

    return str(out_path)


async def _discover_day_page_batches(
    target_date: str, cfg: dict,
) -> AsyncIterator[list[dict]]:
    import httpx
    from selectolax.lexbor import LexborHTMLParser

    run_id = _discovery_run_id(cfg)
    ck_dir, legacy_dir, ck_stem = _discovery_checkpoint_locations(run_id, target_date)
    _discovery_migrate_legacy_checkpoints_if_needed(ck_dir, legacy_dir, ck_stem)
    ck_enable = _discovery_checkpoint_enabled()
    ck_every = max(1, int(os.environ.get("GEM_DISCOVERY_CHECKPOINT_EVERY_PAGES", "1")))
    last_flushed_next_page = 0
    persisted_upto = 0

    page = 1
    empty_pages = 0
    seen: set[str] = set()
    out: list[dict] = []
    fresh = os.environ.get("GEM_DISCOVERY_FRESH", "").lower() in ("1", "true", "yes")

    async def maybe_flush(next_page: int, *, force: bool = False) -> None:
        nonlocal last_flushed_next_page, persisted_upto
        if not ck_enable:
            return
        if not force and (next_page - last_flushed_next_page) < ck_every:
            return
        last_flushed_next_page = next_page
        persisted_upto = await _discovery_persist_checkpoint_v2(
            ck_dir,
            ck_stem,
            next_page=next_page,
            empty_pages=empty_pages,
            out=out,
            persisted_upto=persisted_upto,
        )

    legacy_json = _discovery_ck_legacy_json(ck_dir, ck_stem)
    state_json = _discovery_ck_state_path(ck_dir, ck_stem)
    complete_json = _discovery_ck_complete_path(ck_dir, ck_stem)

    if ck_enable and fresh:
        _discovery_clear_checkpoints(ck_dir, ck_stem)
        _discovery_clear_checkpoints(legacy_dir, ck_stem)
        print(f"[DISCOVER][{target_date}] GEM_DISCOVERY_FRESH=1 cleared primary+legacy checkpoint")
        await data_volume.commit.aio()
    elif ck_enable and complete_json.is_file():
        jsonl_p = _discovery_ck_jsonl_path(ck_dir, ck_stem)
        out, seen = _discovery_load_contracts_jsonl(jsonl_p)
        print(
            f"[DISCOVER][{target_date}] day already COMPLETE (disk) contracts={len(out)} "
            f"run_id={run_id} path={ck_dir}"
        )
        if out:
            yield out
        return
    elif ck_enable:
        if state_json.is_file():
            try:
                data = json.loads(state_json.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                data = {}
            if isinstance(data, dict) and int(data.get("v", 0)) == 2:
                page = int(data.get("next_page", 1))
                empty_pages = int(data.get("empty_pages", 0))
                jsonl_p = _discovery_ck_jsonl_path(ck_dir, ck_stem)
                out, seen = _discovery_load_contracts_jsonl(jsonl_p)
                persisted_upto = len(out)
                last_flushed_next_page = page
                print(
                    f"[DISCOVER][{target_date}] resume v2 next_page={page} "
                    f"empty_pages={empty_pages} contracts={len(out)} run_id={run_id} ck_dir={ck_dir}"
                )
        elif legacy_json.is_file():
            loaded = _discovery_load_legacy_v1_json(legacy_json)
            if loaded:
                page = int(loaded["next_page"])  # type: ignore[arg-type]
                empty_pages = int(loaded["empty_pages"])  # type: ignore[arg-type]
                out = [dict(x) for x in loaded["out"]]  # type: ignore[misc]
                seen = {str(x["contract_no"]) for x in out if x.get("contract_no")}
                last_flushed_next_page = page
                persisted_upto = len(out)
                print(
                    f"[DISCOVER][{target_date}] resume v1 legacy next_page={page} "
                    f"empty_pages={empty_pages} contracts={len(out)} run_id={run_id} "
                    f"(migrating checkpoint to v2 append-only)"
                )
                await _discovery_migrate_v1_file_to_v2(ck_dir, ck_stem, legacy_json)

    print(
        f"[DISCOVER][{target_date}] start run_id={run_id} "
        f"checkpoint={'on' if ck_enable else 'off'} "
        f"ckpt_v2={'yes' if ck_enable else 'n/a'} ck_dir={ck_dir} "
        f"mem_mib={_DISCOVERY_MEMORY_MIB} nonpreemptible={_DISCOVERY_NONPREEMPTIBLE} "
        f"retries={_DISCOVERY_FN_RETRIES} mem_snapshot={_DISCOVERY_MEMORY_SNAPSHOT}"
    )
    timeout = httpx.Timeout(
        float(cfg.get("LIST_HTTP_TIMEOUT", 180.0)),
        connect=30.0,
        write=30.0,
        pool=30.0,
    )
    exit_reason = "complete"
    async with httpx.AsyncClient(headers=get_headers(), timeout=timeout) as client:
        while True:
            mlp = cfg.get("MAX_LIST_PAGES_PER_DAY")
            if mlp is not None and page > int(mlp):
                print(f"[DISCOVER][{target_date}] reached page cap {mlp}, stopping")
                if ck_enable:
                    persisted_upto = await _discovery_mark_day_complete(
                        ck_dir,
                        ck_stem,
                        next_page=page,
                        empty_pages=empty_pages,
                        out=out,
                        persisted_upto=persisted_upto,
                    )
                exit_reason = "page_cap"
                break
            payload = {
                "fromDate": target_date,
                "toDate": target_date,
                "page": page,
                "department": "",
                "bno": "",
                "buyer_category": "",
            }
            resp = None
            for attempt in range(int(cfg.get("HTTP_RETRIES", 5))):
                try:
                    resp = await client.post(CONFIG["LIST_URL"], data=payload)
                    if resp.status_code in (429, 500, 502, 503, 504):
                        print(
                            f"[DISCOVER][{target_date}] list page {page} "
                            f"http={resp.status_code}, retry {attempt + 1}"
                        )
                        await _sleep_backoff(attempt, cfg)
                        continue
                    break
                except httpx.RequestError as exc:
                    print(
                        f"[DISCOVER][{target_date}] list page {page} request error "
                        f"{type(exc).__name__}, retry {attempt + 1}"
                    )
                    await _sleep_backoff(attempt, cfg)
            if resp is None:
                print(
                    f"[DISCOVER][{target_date}] list page {page} failed after "
                    f"{cfg.get('HTTP_RETRIES', 5)} retries, stopping day"
                )
                await maybe_flush(page, force=True)
                exit_reason = "transport_fail"
                break
            if resp.status_code != 200:
                print(
                    f"[DISCOVER][{target_date}] list page {page} http={resp.status_code}, skipping"
                )
                await maybe_flush(page + 1, force=True)
                page += 1
                continue
            if "no record found" in resp.text.lower():
                print(f"[DISCOVER][{target_date}] no more records at page {page}")
                if ck_enable:
                    persisted_upto = await _discovery_mark_day_complete(
                        ck_dir,
                        ck_stem,
                        next_page=page + 1,
                        empty_pages=empty_pages,
                        out=out,
                        persisted_upto=persisted_upto,
                    )
                break
            tree = LexborHTMLParser(resp.text)
            blocks = tree.css(".border.block")
            del resp
            if not blocks:
                print(f"[DISCOVER][{target_date}] page {page} had zero blocks")
                empty_pages += 1
                max_empty = int(cfg.get("MAX_EMPTY_LIST_PAGES_PER_DAY", 10))
                if empty_pages >= max_empty:
                    print(
                        f"[DISCOVER][{target_date}] {empty_pages} consecutive empty pages, stopping"
                    )
                    await maybe_flush(page, force=True)
                    exit_reason = "empty_streak"
                    break
                await maybe_flush(page + 1, force=True)
                page += 1
                continue
            empty_pages = 0
            pre_count = len(out)
            for element in blocks:
                if not cfg.get("DIRECTORY_MODE") and "Bid/RA" not in element.text():
                    continue
                c_node = element.css_first(".ajxtag_order_number")
                contract_no = c_node.text(strip=True) if c_node else "N/A"
                if not contract_no or contract_no == "N/A" or contract_no in seen:
                    continue
                item_name = "Various Items"
                price = "0"
                table = element.css_first("table.table-striped")
                if table:
                    rows = table.css("tr")[1:]
                    if rows:
                        cols = rows[0].css("td")
                        if len(cols) >= 5:
                            item_name = cols[0].text(strip=True)
                            price = re.sub(r"[^\d.]", "", cols[4].text(strip=True))
                seen.add(contract_no)
                out.append(
                    {
                        "contract_no": contract_no,
                        "list_date": target_date,
                        "primary_item": item_name,
                        "list_price": price or None,
                    }
                )
                cap = _per_day_contract_cap(cfg)
                if cap is not None and len(out) >= int(cap):
                    print(
                        f"[DISCOVER][{target_date}] reached per-day contract cap {cap}, stopping early"
                    )
                    if ck_enable:
                        persisted_upto = await _discovery_mark_day_complete(
                            ck_dir,
                            ck_stem,
                            next_page=page + 1,
                            empty_pages=empty_pages,
                            out=out,
                            persisted_upto=persisted_upto,
                        )
                    page_batch = out[pre_count:]
                    if page_batch:
                        yield page_batch
                    return
            added = len(out) - pre_count
            print(
                f"[DISCOVER][{target_date}] page {page} blocks={len(blocks)} added={added} total={len(out)}"
            )
            if added:
                yield out[pre_count:]
            await maybe_flush(page + 1, force=True)
            page += 1
            await asyncio.sleep(
                random.uniform(
                    cfg["LIST_PAGE_DELAY_MIN"],
                    cfg["LIST_PAGE_DELAY_MAX"],
                )
            )
    if exit_reason == "transport_fail":
        print(
            f"[DISCOVER][{target_date}] incomplete (network) total_contracts={len(out)} "
            f"checkpoint_kept={'yes' if ck_enable else 'n/a'}"
        )
        return
    if exit_reason == "empty_streak":
        print(
            f"[DISCOVER][{target_date}] stopped_empty_streak total_contracts={len(out)} "
            f"checkpoint_kept={'yes' if ck_enable else 'n/a'} (resume same day)"
        )
        return
    print(f"[DISCOVER][{target_date}] complete total_contracts={len(out)}")


@app.function(**_DISCOVERY_FN_DECORATOR_KW)
async def discover_contracts_for_day(
    target_date: str, cfg_overrides_json: str = ""
) -> list[dict]:
    """List all contracts for one day (used by chunk-first coordinator)."""
    cfg = _merge_cfg_overrides(load_pipeline_config(), _decode_cfg_overrides(cfg_overrides_json))
    collected: list[dict] = []
    async for batch in _discover_day_page_batches(target_date, cfg):
        collected.extend(batch)
    return collected


@app.function(
    image=image,
    region="ap-south",
    timeout=86400,
    max_containers=MAX_CONTAINERS,
    memory=_PROCESS_MEMORY_MIB,
    retries=_PROCESS_FN_RETRIES,
    nonpreemptible=_PROCESS_NONPREEMPTIBLE,
    enable_memory_snapshot=True,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
@modal.concurrent(max_inputs=_PROCESS_CONTRACT_MAX_INPUTS)
async def process_contract(
    item: dict, run_id: str, cfg_overrides_json: str = ""
) -> dict:
    import httpx

    _setup_path()
    from gem_azure import azure_configured, upload_pdf
    from gem_pdf_extract import extract_contract_record, extract_directory_record

    cfg = _merge_cfg_overrides(load_pipeline_config(), _decode_cfg_overrides(cfg_overrides_json))
    contract_no = item["contract_no"]
    list_date = item["list_date"]
    year_s, month_s = _ym_parts(list_date)
    print(f"[PROCESS][{run_id}] start contract={contract_no} list_date={list_date}")

    async with httpx.AsyncClient(headers=get_headers(), timeout=90.0) as client:
        pdf_bytes = await download_pdf_bytes(client, contract_no, cfg)
    print(
        f"[PROCESS][{run_id}] downloaded contract={contract_no} pdf_ok={'yes' if pdf_bytes else 'no'}"
    )

    meta_in = {
        "contract_no": contract_no,
        "list_date": list_date,
        "primary_item": item.get("primary_item"),
        "list_price": item.get("list_price"),
    }
    if cfg.get("DIRECTORY_MODE"):
        rec = extract_directory_record(
            pdf_bytes or b"",
            meta_in,
            max_pages=int(cfg.get("DIRECTORY_MAX_PDF_PAGES", 4)),
            allow_slow_fallback=bool(cfg.get("DIRECTORY_SLOW_FALLBACK", True)),
        )
        em = rec.get("seller_email")
        ph = rec.get("seller_phone")
        ne = len(rec.get("seller_emails") or []) if isinstance(rec.get("seller_emails"), list) else 0
        np = len(rec.get("seller_phones") or []) if isinstance(rec.get("seller_phones"), list) else 0
        print(
            f"[PROCESS][{run_id}] directory_fields contract={contract_no} "
            f"seller_email={'yes' if em else 'no'} seller_phone={'yes' if ph else 'no'} "
            f"alt_emails={ne} alt_phones={np} product={(rec.get('product_name') or '')[:48]!r}"
        )
    else:
        rec = extract_contract_record(pdf_bytes or b"", meta_in)
    ex = rec.get("extraction") or {}
    safe = re.sub(r"[^\w.-]+", "_", contract_no)
    records_root = "directory_records" if cfg.get("DIRECTORY_MODE") else "records"
    rejects_root = "directory_rejects" if cfg.get("DIRECTORY_MODE") else "rejects"
    rec_dir = Path(CONFIG["VOLUME_DIR"]) / records_root / run_id / year_s / month_s
    rej_dir = Path(CONFIG["VOLUME_DIR"]) / rejects_root / run_id / year_s / month_s
    rec_dir.mkdir(parents=True, exist_ok=True)
    rej_dir.mkdir(parents=True, exist_ok=True)

    accepted = not ((ex.get("is_reject") is True) or ex.get("reject_reasons"))
    out_path = (rec_dir if accepted else rej_dir) / f"{safe}.json"
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(rec, ensure_ascii=False), encoding="utf-8")
    tmp.replace(out_path)
    print(
        f"[PROCESS][{run_id}] wrote contract={contract_no} accepted={accepted} path={out_path}"
    )

    if cfg.get("DIRECTORY_MODE") and _live_contacts_log_enabled():
        live_row = {
            "t": dt.datetime.now(dt.timezone.utc).isoformat(),
            "contract_no": contract_no,
            "list_date": list_date,
            "seller_name": rec.get("seller_name"),
            "seller_email": rec.get("seller_email"),
            "seller_phone": rec.get("seller_phone"),
            "seller_emails": rec.get("seller_emails"),
            "seller_phones": rec.get("seller_phones"),
            "seller_gstin": rec.get("seller_gstin"),
            "product_name": rec.get("product_name"),
            "contract_value": rec.get("contract_value"),
            "accepted": accepted,
        }
        _append_live_seller_contact_row(run_id, live_row)
        print(
            f"[LIVE][{run_id}] +seller_row → /data/logs/seller_contacts_live_{run_id}.jsonl "
            f"contract={contract_no}"
        )

    if cfg["SAVE_RAW_PDFS_TO_VOLUME"] and pdf_bytes:
        pdir = Path(CONFIG["VOLUME_DIR"]) / "pdfs" / run_id / list_date
        pdir.mkdir(parents=True, exist_ok=True)
        pdf_final = pdir / f"{safe}.pdf"
        pdf_tmp = pdir / f"{safe}.pdf.tmp"
        pdf_tmp.write_bytes(pdf_bytes)
        pdf_tmp.replace(pdf_final)

    if (
        cfg["UPLOAD_AZURE_IF_CONFIGURED"]
        and azure_configured()
        and pdf_bytes
        and year_s
        and month_s
    ):
        blob_pdf = f"raw-pdfs/{year_s}/{month_s}/{contract_no}.pdf"
        try:
            upload_pdf(pdf_bytes=pdf_bytes, blob_path=blob_pdf)
        except Exception:
            pass

    await data_volume.commit.aio()

    return {
        "contract_no": contract_no,
        "list_date": list_date,
        "accepted": accepted,
        "path": str(out_path),
    }


@app.function(**_DAY_PIPELINE_FN_DECORATOR_KW)
async def directory_day_list_then_pdf(
    target_date: str, run_id: str, cfg_overrides_json: str = ""
) -> dict:
    """One calendar day: list GeM page-by-page and run PDF after each batch (no full-day list wait).

    Caps busy days via MAX_DISCOVERED_CONTRACTS_PER_DAY (default 2000). PDF starts after each
    list page (FLUSH_PDF_AFTER_EVERY_LIST_PAGE) and in waves of DISCOVER_PDF_BATCH_SIZE.
    Uses process_contract.map.aio() because listing runs in async context.
    """
    cfg = _merge_cfg_overrides(load_pipeline_config(), _decode_cfg_overrides(cfg_overrides_json))
    batch_size = max(1, int(cfg.get("DISCOVER_PDF_BATCH_SIZE", 40)))
    per_day_cap = _per_day_contract_cap(cfg)
    listed = 0
    accepted = 0
    rejected = 0
    pdf_buffer: list[dict] = []

    print(
        f"[DAYPIPE][{target_date}] stream_list_pdf run_id={run_id} "
        f"pdf_batch_size={batch_size} per_day_cap={per_day_cap} "
        f"flush_each_page={cfg.get('FLUSH_PDF_AFTER_EVERY_LIST_PAGE', True)} "
        f"nonpreemptible_discovery={_DISCOVERY_NONPREEMPTIBLE} "
        f"nonpreemptible_pdf={_PROCESS_NONPREEMPTIBLE}"
    )

    async def flush_pdf(items: list[dict]) -> None:
        nonlocal listed, accepted, rejected
        if not items:
            return
        listed += len(items)
        proc_results: list[dict] = []
        async for result in process_contract.map.aio(
            items,
            kwargs={"run_id": run_id, "cfg_overrides_json": cfg_overrides_json},
            order_outputs=False,
        ):
            if isinstance(result, dict):
                proc_results.append(result)
        batch_acc = sum(1 for r in proc_results if r.get("accepted"))
        batch_rej = sum(1 for r in proc_results if not r.get("accepted"))
        accepted += batch_acc
        rejected += batch_rej
        print(
            f"[DAYPIPE][{target_date}] pdf_batch n={len(items)} "
            f"accepted={batch_acc} rejected={batch_rej} run_id={run_id}"
        )

    flush_each_page = cfg.get("FLUSH_PDF_AFTER_EVERY_LIST_PAGE", True)

    async for page_batch in _discover_day_page_batches(target_date, cfg):
        pdf_buffer.extend(page_batch)
        while len(pdf_buffer) >= batch_size:
            chunk = pdf_buffer[:batch_size]
            pdf_buffer = pdf_buffer[batch_size:]
            await flush_pdf(chunk)
        if flush_each_page and pdf_buffer:
            await flush_pdf(pdf_buffer)
            pdf_buffer = []

    if pdf_buffer:
        await flush_pdf(pdf_buffer)

    print(
        f"[DAYPIPE][{target_date}] pdf_done listed={listed} accepted={accepted} "
        f"rejected={rejected} run_id={run_id}"
    )
    return {
        "target_date": target_date,
        "listed": listed,
        "accepted": accepted,
        "rejected": rejected,
    }


@app.function(
    image=image,
    timeout=86400,
    enable_memory_snapshot=True,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
)
def merge_run_parquet(run_id: str, outfile_name: str = None) -> str | None:
    import pandas as pd

    _setup_path()
    from gem_record_flatten import flatten_for_parquet

    base = Path(CONFIG["VOLUME_DIR"]) / "records" / run_id
    paths = sorted(base.glob("**/*.json"))
    if not paths:
        print(f"No accepted record files for run_id={run_id}")
        return None
    rows: list[dict] = []
    for p in paths:
        rec = json.loads(p.read_text(encoding="utf-8"))
        rows.append(flatten_for_parquet(rec))
    if not rows:
        return None
    df = pd.DataFrame(rows)
    out_dir = Path(CONFIG["VOLUME_DIR"]) / "parquet"
    out_dir.mkdir(parents=True, exist_ok=True)
    name = outfile_name or f"GEM_CONTRACTS_{run_id}.parquet"
    out_path = out_dir / name
    df.to_parquet(out_path, index=False)
    data_volume.commit()
    print(f"Wrote {len(df)} rows to {out_path}")
    return str(out_path)


@app.function(
    image=image,
    timeout=86400,
    enable_memory_snapshot=True,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
)
def merge_run_directory_parquet(
    run_id: str, outfile_name: str = None, json_batch_size: int = 10000
) -> str | None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _setup_path()
    from gem_record_flatten import flatten_directory_for_parquet

    env_bs = os.environ.get("GEM_PARQUET_MERGE_BATCH", "").strip()
    if env_bs:
        try:
            json_batch_size = max(500, int(env_bs))
        except ValueError:
            pass

    base = Path(CONFIG["VOLUME_DIR"]) / "directory_records" / run_id
    paths = sorted(base.glob("**/*.json"))
    if not paths:
        print(f"No accepted directory record files for run_id={run_id}")
        return None
    out_dir = Path(CONFIG["VOLUME_DIR"]) / "directory_parquet"
    out_dir.mkdir(parents=True, exist_ok=True)
    name = outfile_name or f"GEM_CONTRACTOR_DIRECTORY_{run_id}.parquet"
    out_path = out_dir / name
    bs = max(500, int(json_batch_size))
    writer: pq.ParquetWriter | None = None
    total = 0
    for i in range(0, len(paths), bs):
        chunk_paths = paths[i : i + bs]
        rows: list[dict] = []
        for p in chunk_paths:
            rec = json.loads(p.read_text(encoding="utf-8"))
            rows.append(flatten_directory_for_parquet(rec))
        if not rows:
            continue
        table = pa.Table.from_pylist(rows)
        if writer is None:
            writer = pq.ParquetWriter(str(out_path), table.schema, compression="snappy")
        else:
            if table.schema != writer.schema:
                table = table.cast(writer.schema)
        writer.write_table(table)
        total += len(rows)
        if (i // bs) % 10 == 0 and i > 0:
            print(f"  merge_run_directory_parquet: wrote {total} rows so far...")
    if writer is None:
        print("No rows to write after flatten.")
        return None
    writer.close()
    data_volume.commit()
    print(f"Wrote {total} directory rows to {out_path}")
    return str(out_path)


def _live_contacts_log_enabled() -> bool:
    """Append one JSON line per directory extract to ``/data/logs/seller_contacts_live_<RUN_ID>.jsonl``."""
    return os.environ.get("GEM_LIVE_CONTACTS_LOG", "1").strip().lower() not in ("0", "false", "no")


def _append_live_seller_contact_row(run_id: str, row: dict) -> None:
    log_dir = Path(CONFIG["VOLUME_DIR"]) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    p = log_dir / f"seller_contacts_live_{run_id}.jsonl"
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _use_parallel_day_pipeline(cfg: dict) -> bool:
    """List+PDF per calendar day in parallel across days (PDFs do not wait for other days' listing).

    Opt out with GEM_PIPELINE_CHUNK_FIRST=1 or cfg PIPELINE_CHUNK_FIRST (wait for whole discover
    chunk before any PDF). Forced off when MAX_DISCOVERED_CONTRACTS is set (global cap) or
    DETACHED_SPAWN_MAP or non-directory mode.
    """
    if cfg.get("DETACHED_SPAWN_MAP"):
        return False
    if os.environ.get("GEM_PIPELINE_CHUNK_FIRST", "").lower() in ("1", "true", "yes"):
        return False
    if cfg.get("PIPELINE_CHUNK_FIRST"):
        return False
    if cfg.get("MAX_DISCOVERED_CONTRACTS") is not None:
        return False
    if not cfg.get("DIRECTORY_MODE", True):
        return False
    return True


def _append_superfast_checkpoint(run_id: str, record: dict) -> None:
    """Append one JSON line to the volume for chunk-level progress (coordinator only)."""
    log_dir = Path(CONFIG["VOLUME_DIR"]) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"superfast_checkpoints_{run_id}.jsonl"
    row = dict(record)
    row.setdefault("run_id", run_id)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    data_volume.commit()


def _run_extraction_core(cfg_overrides_json: str = "") -> dict:
    """Fast path: contract fan-out pipeline; fallback to day workers."""
    _setup_path()
    from datetime import datetime, timezone

    from gem_run_logging import write_run_manifest

    cfg_overrides = _decode_cfg_overrides(cfg_overrides_json)
    cfg = _merge_cfg_overrides(load_pipeline_config(), cfg_overrides)
    run_id = cfg.get("RUN_ID") or os.environ.get("GEM_RUN_ID") or datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    print(
        f"[RUN][{run_id}] starting pipeline use_fanout={cfg.get('USE_CONTRACT_FANOUT', True)} "
        f"detached={cfg.get('DETACHED_SPAWN_MAP', False)} max_contracts={cfg.get('MAX_DISCOVERED_CONTRACTS')}"
    )

    date_list = _date_strings_from_cfg(cfg)
    print(
        f"[RUN][{run_id}] date_range days={len(date_list)} first={date_list[0] if date_list else 'n/a'} "
        f"last={date_list[-1] if date_list else 'n/a'}"
    )
    log_dir = Path(CONFIG["VOLUME_DIR"]) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    pq_path: str | None = None

    if cfg.get("USE_CONTRACT_FANOUT", True):
        print(
            f"Run {run_id}: contract fan-out over {len(date_list)} day(s), "
            f"detached={cfg.get('DETACHED_SPAWN_MAP', False)} notes={cfg.get('RUN_NOTES','')}"
        )
        day_results: list[dict] = []
        global_seen: set[str] = set()
        hit_cap = False
        parallel_listed_total = 0

        if _use_parallel_day_pipeline(cfg):
            day_workers = _DAY_PIPELINE_MAX_CONTAINERS
            raw_dpm = os.environ.get("GEM_DAY_PIPELINE_MAX_CONTAINERS", "").strip()
            if raw_dpm:
                day_workers = max(1, min(120, int(raw_dpm)))
            print(
                f"[RUN][{run_id}] pipeline_mode=parallel_days "
                f"(each calendar day lists then PDFs immediately; "
                f"max_day_workers={day_workers} pdf_max_containers={MAX_CONTAINERS} "
                f"pdf_concurrent_inputs={_PROCESS_CONTRACT_MAX_INPUTS})"
            )
            print(
                f"[RUN][{run_id}] live_contact_stream=/data/logs/seller_contacts_live_{run_id}.jsonl "
                f"(GEM_LIVE_CONTACTS_LOG=1 default; tail grows one line per PDF extract)"
            )
            pipe_raw = list(
                directory_day_list_then_pdf.map(
                    date_list,
                    kwargs={"run_id": run_id, "cfg_overrides_json": cfg_overrides_json},
                    return_exceptions=True,
                    wrap_returned_exceptions=False,
                )
            )
            total_acc = 0
            total_rej = 0
            for target_date, result in zip(date_list, pipe_raw):
                if isinstance(result, Exception):
                    print(f"[RUN][{run_id}] parallel day failed {target_date}: {result!r}")
                    day_results.append(
                        {
                            "mode": "parallel_day_pipeline",
                            "target_date": target_date,
                            "error": repr(result),
                        }
                    )
                else:
                    r = result
                    listed = int(r.get("listed", 0))
                    parallel_listed_total += listed
                    total_acc += int(r.get("accepted", 0))
                    total_rej += int(r.get("rejected", 0))
                    day_results.append({"mode": "parallel_day_pipeline", **r})
            print(
                f"[RUN][{run_id}] parallel_day_pipeline done calendar_days={len(date_list)} "
                f"listed={parallel_listed_total} accepted={total_acc} rejected={total_rej}"
            )
            _append_superfast_checkpoint(
                run_id,
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "chunk_num": 1,
                    "date_from": date_list[0] if date_list else "",
                    "date_to": date_list[-1] if date_list else "",
                    "chunk_calendar_days": len(date_list),
                    "items_new_in_chunk": parallel_listed_total,
                    "accepted": total_acc,
                    "rejected": total_rej,
                    "cumulative_unique_contracts": parallel_listed_total,
                    "discovery_error_days": sum(1 for d in day_results if d.get("error")),
                    "phase": "after_parallel_day_pipeline",
                },
            )
        else:
            chunk_days = _effective_date_chunk_days(len(date_list), cfg)
            print(
                f"[RUN][{run_id}] calendar chunk_size_days={chunk_days} "
                f"(set GEM_DATE_CHUNK_DAYS to override; 0 = auto) "
                f"[chunk-first: GEM_PIPELINE_CHUNK_FIRST=1 or MAX_DISCOVERED_CONTRACTS set]"
            )

            for chunk_idx in range(0, len(date_list), chunk_days):
                chunk_dates = date_list[chunk_idx : chunk_idx + chunk_days]
                chunk_num = chunk_idx // chunk_days + 1
                print(
                    f"[RUN][{run_id}] discovering chunk {chunk_idx // chunk_days + 1} "
                    f"len={len(chunk_dates)} range={chunk_dates[0]}..{chunk_dates[-1]}"
                )
                print(
                    f"[RUN][{run_id}] phase=A listing | discover_contracts_for_day uses v2 checkpoints "
                    f"(state.json + contracts.jsonl under /data/discovery_ckpt/{run_id}/; "
                    f"legacy /data/logs/discovery_checkpoints/) "
                    f"when enabled; returns ids to the coordinator."
                )
                discovered_raw = list(
                    discover_contracts_for_day.map(
                        chunk_dates,
                        kwargs={"cfg_overrides_json": cfg_overrides_json},
                        return_exceptions=True,
                        wrap_returned_exceptions=False,
                    )
                )
                discovered: list[list[dict]] = []
                discovery_errors: list[dict[str, str]] = []
                for target_date, result in zip(chunk_dates, discovered_raw):
                    if isinstance(result, Exception):
                        print(f"[RUN][{run_id}] discovery failed for {target_date}: {result!r}")
                        discovery_errors.append({"date": target_date, "error": repr(result)})
                        discovered.append([])
                    else:
                        discovered.append(result)
                print(
                    f"[RUN][{run_id}] discovery chunk done sizes={[len(d) for d in discovered]} "
                    f"errors={len(discovery_errors)}"
                )
                items: list[dict] = []
                for day_items in discovered:
                    for it in day_items:
                        cno = it.get("contract_no")
                        if not cno or cno in global_seen:
                            continue
                        global_seen.add(cno)
                        items.append(it)
                        mx = cfg.get("MAX_DISCOVERED_CONTRACTS")
                        if mx is not None and len(global_seen) >= int(mx):
                            hit_cap = True
                            break
                    if hit_cap:
                        break
                print(
                    f"[RUN][{run_id}] cumulative unique contracts={len(global_seen)} "
                    f"new_in_chunk={len(items)}"
                )
                if items:
                    save_pdfs = bool(cfg.get("SAVE_RAW_PDFS_TO_VOLUME"))
                    print(
                        f"[RUN][{run_id}] phase=B pdf_extract+save | starting process_contract.map "
                        f"(n={len(items)}) — per contract: download PDF → extract_directory_record "
                        f"(seller email/phone live HERE, not in listing logs). "
                        f"JSON: /data/directory_records/{run_id}/<YYYY>/<MM>/*.json | "
                        f"PDFs: {'/data/pdfs/' + run_id + '/<list_date>/' if save_pdfs else 'SAVE_RAW_PDFS_TO_VOLUME=off'} "
                        f"| each task ends with volume commit."
                    )
                if not items:
                    day_results.append(
                        {
                            "mode": "fanout_chunk",
                            "chunk_dates": len(chunk_dates),
                            "contracts_discovered": 0,
                            "discovery_errors": len(discovery_errors),
                        }
                    )
                    _append_superfast_checkpoint(
                        run_id,
                        {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "chunk_num": chunk_num,
                            "date_from": chunk_dates[0],
                            "date_to": chunk_dates[-1],
                            "chunk_calendar_days": len(chunk_dates),
                            "items_new_in_chunk": 0,
                            "accepted": 0,
                            "rejected": 0,
                            "cumulative_unique_contracts": len(global_seen),
                            "discovery_error_days": len(discovery_errors),
                            "phase": "after_discovery_no_new_items",
                        },
                    )
                elif cfg.get("DETACHED_SPAWN_MAP", False):
                    print(
                        f"[RUN][{run_id}] submitting detached process_contract.spawn_map "
                        f"n={len(items)}"
                    )
                    process_contract.spawn_map(
                        items,
                        kwargs={"run_id": run_id, "cfg_overrides_json": cfg_overrides_json},
                    )
                    day_results.append(
                        {
                            "mode": "detached_spawn_map",
                            "contracts_submitted": len(items),
                            "chunk_dates": len(chunk_dates),
                        }
                    )
                    print("Detached submission for chunk complete; merge separately if needed.")
                    _append_superfast_checkpoint(
                        run_id,
                        {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "chunk_num": chunk_num,
                            "date_from": chunk_dates[0],
                            "date_to": chunk_dates[-1],
                            "chunk_calendar_days": len(chunk_dates),
                            "items_new_in_chunk": len(items),
                            "accepted": None,
                            "rejected": None,
                            "cumulative_unique_contracts": len(global_seen),
                            "discovery_error_days": len(discovery_errors),
                            "phase": "after_detached_spawn_map",
                        },
                    )
                else:
                    print(f"[RUN][{run_id}] starting process_contract.map inputs={len(items)}")
                    proc_results = list(
                        process_contract.map(
                            items,
                            kwargs={"run_id": run_id, "cfg_overrides_json": cfg_overrides_json},
                            order_outputs=False,
                        )
                    )
                    print(
                        f"[RUN][{run_id}] process_contract.map chunk done outputs={len(proc_results)}"
                    )
                    accepted = sum(
                        1 for r in proc_results if isinstance(r, dict) and r.get("accepted")
                    )
                    rejected = sum(
                        1 for r in proc_results if isinstance(r, dict) and not r.get("accepted")
                    )
                    day_results.append(
                        {
                            "mode": "directory_fanout"
                            if cfg.get("DIRECTORY_MODE")
                            else "contract_fanout",
                            "contracts_discovered": len(items),
                            "accepted": accepted,
                            "rejected": rejected,
                            "chunk_dates": len(chunk_dates),
                        }
                    )
                    _append_superfast_checkpoint(
                        run_id,
                        {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "chunk_num": chunk_num,
                            "date_from": chunk_dates[0],
                            "date_to": chunk_dates[-1],
                            "chunk_calendar_days": len(chunk_dates),
                            "items_new_in_chunk": len(items),
                            "accepted": accepted,
                            "rejected": rejected,
                            "cumulative_unique_contracts": len(global_seen),
                            "discovery_error_days": len(discovery_errors),
                            "phase": "after_process_contract_map",
                        },
                    )
                if hit_cap:
                    print(f"[RUN][{run_id}] MAX_DISCOVERED_CONTRACTS reached; stopping chunks.")
                    break

            print(f"[RUN][{run_id}] all chunks done unique_contracts={len(global_seen)}")

        if not cfg.get("DETACHED_SPAWN_MAP", False) and date_list and (
            global_seen or parallel_listed_total > 0
        ):
            try:
                merge_fn = (
                    merge_run_directory_parquet if cfg.get("DIRECTORY_MODE") else merge_run_parquet
                )
                merge_name = (
                    "merge_run_directory_parquet"
                    if cfg.get("DIRECTORY_MODE")
                    else "merge_run_parquet"
                )
                print(f"[RUN][{run_id}] starting {merge_name}")
                fc = merge_fn.remote(run_id=run_id)
                pq_path = fc.get() if hasattr(fc, "get") else fc
                print(f"[RUN][{run_id}] merge finished parquet={pq_path}")
            except Exception as e:
                print(f"merge failed: {e}")
    else:
        y = cfg["YEAR"]
        m = cfg["MONTH"]
        start = cfg["START_DAY"]
        end = cfg["END_DAY"]
        print(
            f"Run {run_id}: launching {len(date_list)} day worker(s) for "
            f"{start}-{end or 'EOM'} {m:02d}-{y} | notes={cfg.get('RUN_NOTES', '')}"
        )
        print(f"[RUN][{run_id}] starting scrape_single_day.map")
        day_results = list(
            scrape_single_day.map(date_list, kwargs={"cfg_overrides_json": cfg_overrides_json})
        )
        print(f"[RUN][{run_id}] scrape_single_day.map completed outputs={len(day_results)}")
        try:
            print(f"[RUN][{run_id}] starting merge_month_parquet")
            fc = merge_month_parquet.remote(year=y, month=m)
            pq_path = fc.get() if hasattr(fc, "get") else fc
            print(f"[RUN][{run_id}] merge_month_parquet finished parquet={pq_path}")
        except Exception as e:
            print(f"merge_month_parquet failed: {e}")

    manifest_path = write_run_manifest(
        log_dir,
        run_id,
        cfg,
        day_results,
        pq_path,
        notes=str(cfg.get("RUN_NOTES", "")),
    )
    print(f"[RUN][{run_id}] manifest writing complete")
    data_volume.commit()
    print(f"Run manifest: {manifest_path}")
    if cfg.get("DIRECTORY_MODE"):
        print("Done. Directory parquet under /data/directory_parquet on volume gem_contracts_analytics_v1")
    else:
        print("Done. Parquet under /data/parquet on volume gem_contracts_analytics_v1")
    return {
        "run_id": run_id,
        "parquet_path": pq_path,
        "manifest_path": str(manifest_path),
        "day_results": day_results,
    }


@app.function(
    image=image,
    timeout=86400,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
)
def main(cfg_overrides_json: str = "") -> dict:
    return _run_extraction_core(cfg_overrides_json)


def _ingest_directory_parquet_core(
    parquet_path: str,
    batch_size: int = 128,
    *,
    require_embeddings: bool | None = None,
) -> dict:
    """Directory parquet -> Postgres (embeddings optional when FTS-only)."""
    _setup_path()
    sys.path.insert(0, "/repo/services/api")
    from app.config import get_settings
    from app.db import ContractorStore
    from app.ingest import ingest_parquet

    if require_embeddings is None:
        flag = os.environ.get("INGEST_REQUIRE_EMBEDDINGS", "0").strip().lower()
        require_embeddings = flag in ("1", "true", "yes")

    path = Path(parquet_path)
    if not path.is_absolute():
        path = Path(CONFIG["VOLUME_DIR"]) / "directory_parquet" / path
    if not path.exists():
        raise FileNotFoundError(str(path))

    settings = get_settings()
    if require_embeddings:
        settings.require_azure_embeddings()
    report = ingest_parquet(
        path, settings, batch_size=batch_size, require_embeddings=require_embeddings
    )
    validation = ContractorStore(settings).validation_stats()
    out = {
        "parquet_path": str(path),
        "rows_loaded": report.rows_loaded,
        "rows_upserted": report.rows_upserted,
        "missing_embeddings": report.missing_embeddings,
        "missing_product_names": report.missing_product_names,
        "db_validation": validation,
    }
    print(out)
    return out


@app.function(
    image=image,
    timeout=86400,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
def ingest_directory_parquet(parquet_path: str, batch_size: int = 128) -> dict:
    return _ingest_directory_parquet_core(parquet_path, batch_size=batch_size)


@app.local_entrypoint()
def local_main() -> None:
    main.remote(cfg_overrides_json=json.dumps(_collect_local_gem_overrides()))


@app.function(image=image, secrets=[runtime_secret])
def debug_runtime_config() -> dict:
    cookie = os.environ.get("GEM_COOKIE", "")
    info = {
        "gem_cookie_present": bool(cookie),
        "gem_cookie_length": len(cookie),
        "database_url_present": bool(os.environ.get("DATABASE_URL")),
        "azure_endpoint_present": bool(os.environ.get("AZURE_OPENAI_ENDPOINT")),
    }
    print(info)
    return info


@app.function(
    image=image,
    timeout=86400,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
def directory_extract_only_worker(
    start_date: str = "2026-01-01",
    end_date: str = "",
    run_id: str = "",
) -> dict:
    """Scrape directory PDFs through the date range, write per-contract JSON, merge to Parquet only (no DB/embeddings)."""
    generated_run_id = run_id or f"full_2026_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    save_pdfs = _worker_save_pdfs_default(False)
    cfg_overrides = {
        "DATE_RANGE_START": start_date,
        "DATE_RANGE_END": end_date or None,
        "DIRECTORY_MODE": True,
        "SAVE_RAW_PDFS_TO_VOLUME": save_pdfs,
        "UPLOAD_AZURE_IF_CONFIGURED": False,
        "USE_CONTRACT_FANOUT": True,
        "DETACHED_SPAWN_MAP": False,
        "MAX_LIST_PAGES_PER_DAY": None,
        "MAX_EMPTY_LIST_PAGES_PER_DAY": 40,
        "LIST_HTTP_TIMEOUT": 240.0,
        "LIST_PAGE_DELAY_MIN": 1.5,
        "LIST_PAGE_DELAY_MAX": 4.0,
        "HTTP_RETRIES": 10,
        "RUN_ID": generated_run_id,
        "RUN_NOTES": f"directory_parquet_only_{start_date}_to_{end_date or 'today'}",
    }
    print(
        {
            "step": "extract_directory_parquet_only",
            "run_id": generated_run_id,
            "start_date": start_date,
            "end_date": end_date or "today",
            "save_raw_pdfs_to_volume": save_pdfs,
        }
    )
    extraction = _run_extraction_core(cfg_overrides_json=json.dumps(cfg_overrides))
    parquet_path = (extraction or {}).get("parquet_path")
    if not parquet_path:
        raise RuntimeError(f"Extraction did not produce a directory parquet: {extraction}")
    out = {"step": "extract_complete", "extraction": extraction}
    print(out)
    return out


@app.function(
    image=image,
    timeout=86400,
    memory=_ORCH_MEMORY_MIB,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
def superfast_directory_extract_worker(
    start_date: str = "2026-01-01",
    end_date: str = "",
    run_id: str = "",
    date_chunk_days: int = 21,
    max_discovered_contracts: int = 0,
) -> dict:
    """Directory extract with aggressive defaults; omit list delays so GEM_FAST_LIST (env) applies."""
    _v1_date_chunk_days = max(1, min(60, int(date_chunk_days)))
    generated_run_id = run_id or (
        f"superfast_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    # Same as directory_extract: raw PDF files on volume optional (GEM_SAVE_PDFS=1); contacts always from parse.
    save_pdfs = _worker_save_pdfs_default(False)
    cfg_overrides = {
        "DATE_RANGE_START": start_date,
        "DATE_RANGE_END": end_date or None,
        "DIRECTORY_MODE": True,
        "SAVE_RAW_PDFS_TO_VOLUME": save_pdfs,
        "UPLOAD_AZURE_IF_CONFIGURED": False,
        "USE_CONTRACT_FANOUT": True,
        "DETACHED_SPAWN_MAP": False,
        "MAX_LIST_PAGES_PER_DAY": None,
        "MAX_EMPTY_LIST_PAGES_PER_DAY": 72,
        "DIRECTORY_SLOW_FALLBACK": False,
        "LIST_HTTP_TIMEOUT": 240.0,
        "HTTP_RETRIES": 10,
        "RUN_ID": generated_run_id,
        "RUN_NOTES": f"superfast_directory_{start_date}_to_{end_date or 'today'}",
        "DATE_CHUNK_DAYS": _v1_date_chunk_days,
        "MAX_DISCOVERED_CONTRACTS_PER_DAY": int(
            os.environ.get("GEM_MAX_CONTRACTS_PER_DAY", "2000")
        ),
        "DISCOVER_PDF_BATCH_SIZE": max(
            1, int(os.environ.get("GEM_DISCOVER_PDF_BATCH_SIZE", "40"))
        ),
        "FLUSH_PDF_AFTER_EVERY_LIST_PAGE": os.environ.get(
            "GEM_FLUSH_PDF_AFTER_EVERY_LIST_PAGE", "1"
        ).lower()
        not in ("0", "false", "no"),
    }
    if max_discovered_contracts > 0:
        cfg_overrides["MAX_DISCOVERED_CONTRACTS"] = max_discovered_contracts
    print(
        {
            "step": "superfast_directory_extract",
            "run_id": generated_run_id,
            "start_date": start_date,
            "end_date": end_date or "today",
            "date_chunk_days": _v1_date_chunk_days,
            "max_discovered_contracts": max_discovered_contracts or None,
            "save_raw_pdfs_to_volume": save_pdfs,
            "hint": "GEM_ORCH_MEMORY_MIB, GEM_DATE_CHUNK_DAYS / GEM_SUPERFAST_DATE_CHUNK_DAYS / legacy GEM_V2_DATE_CHUNK_DAYS, GEM_FAST_LIST, GEM_DISCOVERY_*",
        }
    )
    extraction = _run_extraction_core(cfg_overrides_json=json.dumps(cfg_overrides))
    parquet_path = (extraction or {}).get("parquet_path")
    if not parquet_path:
        raise RuntimeError(f"Extraction did not produce a directory parquet: {extraction}")
    out = {"step": "superfast_extract_complete", "extraction": extraction}
    print(out)
    return out


@app.function(
    image=image,
    timeout=86400,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
def full_pipeline_worker(
    start_date: str = "2026-01-01",
    end_date: str = "",
    batch_size: int = 128,
    run_id: str = "",
) -> dict:
    generated_run_id = run_id or f"full_2026_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    save_pdfs = _worker_save_pdfs_default(False)
    cfg_overrides = {
        "DATE_RANGE_START": start_date,
        "DATE_RANGE_END": end_date or None,
        "DIRECTORY_MODE": True,
        "SAVE_RAW_PDFS_TO_VOLUME": save_pdfs,
        "UPLOAD_AZURE_IF_CONFIGURED": False,
        "USE_CONTRACT_FANOUT": True,
        "DETACHED_SPAWN_MAP": False,
        "MAX_LIST_PAGES_PER_DAY": None,
        "MAX_EMPTY_LIST_PAGES_PER_DAY": 40,
        "LIST_HTTP_TIMEOUT": 180.0,
        "LIST_PAGE_DELAY_MIN": 2.0,
        "LIST_PAGE_DELAY_MAX": 5.0,
        "HTTP_RETRIES": 8,
        "RUN_ID": generated_run_id,
        "RUN_NOTES": f"full_directory_from_{start_date}_to_{end_date or 'today'}",
    }
    print(
        {
            "step": "extract_directory_parquet",
            "run_id": generated_run_id,
            "start_date": start_date,
            "end_date": end_date or "today",
            "save_pdfs": save_pdfs,
            "azure_blob_upload": False,
        }
    )
    extraction = _run_extraction_core(cfg_overrides_json=json.dumps(cfg_overrides))
    parquet_path = (extraction or {}).get("parquet_path")
    if not parquet_path:
        raise RuntimeError(f"Extraction did not produce a directory parquet: {extraction}")

    print({"step": "embed_and_ingest", "parquet_path": parquet_path})
    ingest = _ingest_directory_parquet_core(parquet_path=parquet_path, batch_size=batch_size)
    result = {"step": "pipeline_complete", "extraction": extraction, "ingest": ingest}
    print(result)
    return result


@app.local_entrypoint()
def directory_extract_only(
    start_date: str = "2026-01-01",
    end_date: str = "",
    run_id: str = "",
) -> None:
    """Detached-friendly entrypoint: directory scrape + volume JSON + single Parquet (no embeddings/DB)."""
    generated_run_id = run_id or f"full_2026_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    call = directory_extract_only_worker.spawn(
        start_date=start_date,
        end_date=end_date,
        run_id=generated_run_id,
    )
    print(
        json.dumps(
            {
                "spawned_directory_extract_only": call.object_id,
                "run_id": generated_run_id,
                "start_date": start_date,
                "end_date": end_date or "today",
                "parquet_volume_path": f"/data/directory_parquet/GEM_CONTRACTOR_DIRECTORY_{generated_run_id}.parquet",
            },
            indent=2,
        )
    )


@app.local_entrypoint()
def superfast_directory_extract(
    start_date: str = "2026-01-01",
    end_date: str = "",
    run_id: str = "",
    date_chunk_days: int = 0,
    max_contracts: int = 0,
) -> None:
    """Detached spawn: SUPERFAST directory extract (see SUPERFAST/README.md)."""
    generated_run_id = run_id or (
        f"superfast_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    chunk = max(1, min(60, int(date_chunk_days))) if date_chunk_days > 0 else _resolved_superfast_date_chunk_days()
    mx = max_contracts if max_contracts > 0 else _resolved_max_discovered_contracts()
    call = superfast_directory_extract_worker.spawn(
        start_date=start_date,
        end_date=end_date,
        run_id=generated_run_id,
        date_chunk_days=chunk,
        max_discovered_contracts=mx,
    )
    print(
        json.dumps(
            {
                "spawned_superfast_directory_extract": call.object_id,
                "run_id": generated_run_id,
                "start_date": start_date,
                "end_date": end_date or "today",
                "date_chunk_days": chunk,
                "max_discovered_contracts": mx or None,
                "parquet_volume_path": f"/data/directory_parquet/GEM_CONTRACTOR_DIRECTORY_{generated_run_id}.parquet",
                "chunk_checkpoints": f"/data/logs/superfast_checkpoints_{generated_run_id}.jsonl",
            },
            indent=2,
        )
    )


_BACKUP_MEMORY_MIB = max(1024, int(os.environ.get("GEM_BACKUP_MEMORY_MIB", "2048")))
_backup_np_env = os.environ.get("GEM_BACKUP_NONPREEMPTIBLE", "1").strip().lower()
_BACKUP_NONPREEMPTIBLE = _backup_np_env not in ("0", "false", "no")


@app.function(
    image=image,
    region="ap-south",
    timeout=86400,
    max_containers=1,
    memory=_BACKUP_MEMORY_MIB,
    retries=2,
    nonpreemptible=_BACKUP_NONPREEMPTIBLE,
    scaledown_window=120,
    volumes={CONFIG["VOLUME_DIR"]: data_volume},
    secrets=[runtime_secret],
)
def backup_volume_tree_to_azure(
    relative_path: str,
    azure_prefix: str = "modal-volume-backup",
    skip_existing: bool = True,
) -> dict:
    """
    Read-only on Modal volume: copies files to Azure Blob (PC can be off).
    Single non-preemptible container; exits when done (scaledown after ~2 min).
    Re-run safe: skip_existing skips blobs already in Azure (resume after preemption).
    """
    _setup_path()
    from gem_azure import azure_configured, upload_bytes

    if not azure_configured():
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING missing on Modal secret "
            "gem-contractor-directory-secrets"
        )

    root = Path(CONFIG["VOLUME_DIR"]) / relative_path.strip("/")
    if not root.exists():
        raise FileNotFoundError(f"Volume path not found: {root}")

    from azure.storage.blob import BlobServiceClient

    conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container_name = os.environ.get("AZURE_STORAGE_CONTAINER", "gem-contracts")
    svc = BlobServiceClient.from_connection_string(conn)
    container = svc.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        pass

    prefix = azure_prefix.strip("/")
    uploaded = 0
    skipped = 0
    failed = 0
    bytes_total = 0

    print(
        f"[azure-backup] start path={relative_path} prefix={prefix} "
        f"nonpreemptible={_BACKUP_NONPREEMPTIBLE} skip_existing={skip_existing}"
    )

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(Path(CONFIG["VOLUME_DIR"])).as_posix()
        blob_path = f"{prefix}/{rel}" if prefix else rel
        try:
            if skip_existing and container.get_blob_client(blob_path).exists():
                skipped += 1
                continue
            data = path.read_bytes()
            content_type = "application/json" if path.suffix == ".json" else "application/octet-stream"
            if path.suffix == ".parquet":
                content_type = "application/octet-stream"
            upload_bytes(data, blob_path, content_type)
            uploaded += 1
            bytes_total += len(data)
            if uploaded % 500 == 0:
                print(f"[azure-backup] uploaded={uploaded} skipped={skipped} last={blob_path}")
        except Exception as exc:
            failed += 1
            print(f"[azure-backup] failed {rel}: {exc}")

    summary = {
        "relative_path": relative_path,
        "azure_prefix": prefix,
        "container": container_name,
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "bytes_total": bytes_total,
        "status": "complete",
    }
    print(json.dumps(summary, indent=2))
    print("[azure-backup] DONE — container exiting (no more billing after scaledown)")
    return summary


@app.local_entrypoint()
def backup_to_azure(
    relative_path: str = "directory_records/full_2026_20260518T053357Z/2026",
    azure_prefix: str = "modal-volume-backup",
    wait: bool = False,
) -> None:
    """
    Upload a volume folder to Azure Blob (read-only on volume).
    Use: modal run --detach modal_app.py::backup_to_azure  (PC can sleep; job keeps running)
    Or:  modal run modal_app.py::backup_to_azure --wait  (blocks until done; shows errors)
    """
    if wait:
        result = backup_volume_tree_to_azure.remote(
            relative_path=relative_path,
            azure_prefix=azure_prefix,
        )
        print(json.dumps(result, indent=2))
        return
    call = backup_volume_tree_to_azure.spawn(
        relative_path=relative_path,
        azure_prefix=azure_prefix,
    )
    print(
        json.dumps(
            {
                "spawned_azure_backup": call.object_id,
                "relative_path": relative_path,
                "azure_prefix": azure_prefix,
                "note": "Job runs on Modal. Use: modal run --detach ... so it is not cancelled when this terminal closes.",
                "logs": "modal app logs gem_contracts_full_analytics_v1 --timestamps",
            },
            indent=2,
        )
    )


@app.local_entrypoint()
def full_pipeline(
    start_date: str = "2026-01-01",
    end_date: str = "",
    batch_size: int = 128,
    run_id: str = "",
) -> None:
    """One command: full directory scrape, parquet merge, embeddings, DB ingest."""
    generated_run_id = run_id or f"full_2026_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    call = full_pipeline_worker.spawn(
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        run_id=generated_run_id,
    )
    print(
        json.dumps(
            {
                "spawned_full_pipeline": call.object_id,
                "run_id": generated_run_id,
                "start_date": start_date,
                "end_date": end_date or "today",
            },
            indent=2,
        )
    )
