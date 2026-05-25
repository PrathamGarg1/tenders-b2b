#!/usr/bin/env python3
"""CLI: parquet -> Azure embeddings -> Postgres pgvector upsert."""

from __future__ import annotations

import argparse

from app.config import get_settings
from app.ingest import ingest_parquet


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest contractor directory parquet into pgvector")
    ap.add_argument("parquet_path", help="Path to GEM_CONTRACTOR_DIRECTORY_*.parquet")
    ap.add_argument("--batch-size", type=int, default=128)
    args = ap.parse_args()

    report = ingest_parquet(
        args.parquet_path,
        get_settings(),
        batch_size=args.batch_size,
    )
    print(
        {
            "rows_loaded": report.rows_loaded,
            "rows_upserted": report.rows_upserted,
            "embeddings_generated": report.embeddings_generated,
            "missing_embeddings": report.missing_embeddings,
            "missing_product_names": report.missing_product_names,
        }
    )


if __name__ == "__main__":
    main()
