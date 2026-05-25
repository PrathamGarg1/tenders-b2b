#!/usr/bin/env python3
"""Validation gate for smoke/full runs before scaling historical backfill."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate GEM extraction quality gate")
    ap.add_argument("--jsonl", type=Path, required=True, help="Path to main jsonl.gz shard")
    ap.add_argument(
        "--rejects-jsonl",
        type=Path,
        default=None,
        help="Optional path to rejects jsonl.gz shard",
    )
    ap.add_argument("--min-rows", type=int, default=20)
    ap.add_argument("--max-reject-rate", type=float, default=0.15)
    ap.add_argument("--max-missing-product-rate", type=float, default=0.15)
    args = ap.parse_args()

    rows = []
    with gzip.open(args.jsonl, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    reject_rows = []
    if args.rejects_jsonl and args.rejects_jsonl.exists():
        with gzip.open(args.rejects_jsonl, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    reject_rows.append(json.loads(line))

    total = len(rows) + len(reject_rows)
    missing_product = sum(
        1 for r in rows if not ((r.get("product") or {}).get("title"))
    )
    reject_rate = (len(reject_rows) / total) if total else 1.0
    missing_product_rate = (missing_product / len(rows)) if rows else 1.0

    verdict = (
        total >= args.min_rows
        and reject_rate <= args.max_reject_rate
        and missing_product_rate <= args.max_missing_product_rate
    )
    report = {
        "total_rows_all": total,
        "accepted_rows": len(rows),
        "reject_rows": len(reject_rows),
        "reject_rate": reject_rate,
        "missing_product_title_rate": missing_product_rate,
        "thresholds": {
            "min_rows": args.min_rows,
            "max_reject_rate": args.max_reject_rate,
            "max_missing_product_rate": args.max_missing_product_rate,
        },
        "pass": verdict,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if not verdict:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
