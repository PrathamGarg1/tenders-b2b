"""Parquet ingestion for contractor directory records."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings
from .db import ContractorStore
from .embeddings import AzureEmbeddingClient, normalize_embedding_text


@dataclass(frozen=True)
class IngestReport:
    rows_loaded: int
    rows_upserted: int
    embeddings_generated: bool
    missing_embeddings: int
    missing_product_names: int


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    return text or None


def _split_pipe(value: Any) -> list[str]:
    text = _clean_str(value)
    if not text:
        return []
    return [p.strip() for p in text.split("|") if p.strip()]


def _normalize_phone(value: Any) -> str | None:
    text = _clean_str(value)
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 10:
        return digits[-10:]
    return None


def parquet_to_records(parquet_path: str | Path, *, limit: int | None = None) -> list[dict[str, Any]]:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    if limit:
        df = df.head(limit)

    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        phone = _normalize_phone(row.get("seller_phone"))
        phones = _split_pipe(row.get("seller_phones")) or ([phone] if phone else [])
        email = _clean_str(row.get("seller_email"))
        emails = _split_pipe(row.get("seller_emails")) or ([email] if email else [])
        quality_flags = _split_pipe(row.get("quality_flags"))
        reject_reasons = _split_pipe(row.get("reject_reasons"))
        product_name = _clean_str(row.get("product_name"))

        records.append(
            {
                "contract_no": _clean_str(row.get("contract_no")),
                "list_date": _clean_str(row.get("list_date")),
                "product_name": product_name,
                "contract_value": _clean_str(row.get("contract_value")),
                "seller_name": _clean_str(row.get("seller_name")),
                "seller_email": email,
                "seller_phone": phone,
                "seller_emails": emails,
                "seller_phones": phones,
                "seller_gstin": _clean_str(row.get("seller_gstin")),
                "seller_address": _clean_str(row.get("seller_address")),
                "source_pdf_sha256": _clean_str(row.get("source_pdf_sha256")),
                "product_embedding": None,
                "embedding": {
                    "provider": None,
                    "model": None,
                    "dimensions": None,
                    "text": None,
                },
                "extraction": {
                    "quality_flags": quality_flags,
                    "reject_reasons": reject_reasons,
                    "is_reject": bool(row.get("is_reject") or False),
                },
            }
        )
    return [rec for rec in records if rec.get("contract_no")]


def attach_embeddings(
    records: list[dict[str, Any]],
    settings: Settings,
    *,
    batch_size: int = 128,
) -> bool:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    if not records:
        return False
    settings.require_azure_embeddings()
    client = AzureEmbeddingClient(settings)
    deployment = settings.azure_openai_embedding_deployment
    for start in range(0, len(records), batch_size):
        batch = records[start : start + batch_size]
        texts = [normalize_embedding_text(rec.get("product_name")) for rec in batch]
        vectors = client.embed_texts(texts)
        if len(vectors) != len(batch):
            raise RuntimeError(
                f"Azure returned {len(vectors)} embeddings for {len(batch)} product names"
            )
        for rec, text, vector in zip(batch, texts, vectors):
            rec["product_embedding"] = vector
            rec["embedding"] = {
                "provider": "azure_openai",
                "model": deployment,
                "dimensions": len(vector),
                "text": text,
            }
    return True


def count_missing_product_names(records: list[dict[str, Any]]) -> int:
    return sum(1 for rec in records if not rec.get("product_name"))


def count_missing_embeddings(records: list[dict[str, Any]]) -> int:
    return sum(1 for rec in records if not rec.get("product_embedding"))


def ingest_parquet(
    parquet_path: str | Path,
    settings: Settings,
    *,
    batch_size: int = 128,
    require_embeddings: bool = False,
) -> IngestReport:
    settings.require_database()
    if require_embeddings:
        settings.require_azure_embeddings()

    records = parquet_to_records(parquet_path)
    missing_product_names = count_missing_product_names(records)
    if missing_product_names:
        raise RuntimeError(
            f"{missing_product_names} record(s) are missing product_name; "
            "refusing to ingest records without product_name"
        )

    embedded = False
    if require_embeddings:
        embedded = attach_embeddings(records, settings, batch_size=batch_size)
        missing_embeddings = count_missing_embeddings(records)
        if missing_embeddings:
            raise RuntimeError(f"{missing_embeddings} record(s) are missing product embeddings")
    else:
        missing_embeddings = count_missing_embeddings(records)

    upserted = ContractorStore(settings).upsert_records(records)
    if upserted != len(records):
        raise RuntimeError(f"Upserted {upserted} of {len(records)} loaded record(s)")

    return IngestReport(
        rows_loaded=len(records),
        rows_upserted=upserted,
        embeddings_generated=embedded,
        missing_embeddings=missing_embeddings,
        missing_product_names=missing_product_names,
    )
