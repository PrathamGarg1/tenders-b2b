"""Structured run logs for GEM pipeline (volume-friendly JSON)."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DayRunStats:
    target_date: str
    pages_fetched: int = 0
    list_http_errors: int = 0
    contracts_unique: int = 0
    pdf_success: int = 0
    pdf_failed: int = 0
    extraction_zero_text: int = 0
    max_list_pages_cap: int | None = None
    stopped_reason: str | None = None
    sample_contract_nos: list[str] = field(default_factory=list)
    sample_records_redacted: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    finished_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        ck = os.environ.get("GEM_COOKIE", "").strip()
        d["cookie_configured"] = bool(
            ck and "YOUR_COOKIE" not in ck and "YOUR_TS_COOKIE" not in ck
        )
        return d


def write_day_summary(log_dir: str | Path, stats: DayRunStats) -> Path:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    stats.finished_at = utc_now_iso()
    path = log_dir / f"day_{stats.target_date.replace('/', '-')}_summary.json"
    path.write_text(json.dumps(stats.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def write_run_manifest(
    log_dir: str | Path,
    run_id: str,
    pipeline_config: dict[str, Any],
    day_summaries: list[dict[str, Any]],
    parquet_path: str | None,
    notes: str = "",
) -> Path:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "started_logged_at": utc_now_iso(),
        "pipeline": pipeline_config,
        "day_results": day_summaries,
        "parquet_path": parquet_path,
        "notes": notes,
    }
    path = log_dir / f"run_manifest_{run_id}.json"
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def redact_sample_record(rec: dict[str, Any]) -> dict[str, Any]:
    """Small preview for logs (no full spec blob)."""
    cm = rec.get("contract_meta") or {}
    s = rec.get("seller") or {}
    b = rec.get("buyer") or {}
    ex = rec.get("extraction") or {}
    n_specs = len(rec.get("specifications") or [])
    return {
        "contract_no": cm.get("contract_no"),
        "primary_item": (cm.get("primary_item") or "")[:120],
        "seller_company": (s.get("company_name") or "")[:80],
        "seller_gstin": s.get("gstin"),
        "buyer_org": (b.get("organisation_name") or "")[:80],
        "pdf_pages": ex.get("pages"),
        "pdf_text_chars": ex.get("text_chars"),
        "spec_rows": n_specs,
        "is_reject": ex.get("is_reject"),
        "reject_reasons": ex.get("reject_reasons"),
        "quality_flags": ex.get("quality_flags"),
    }
