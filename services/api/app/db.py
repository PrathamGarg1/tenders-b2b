"""Postgres/pgvector storage and search helpers."""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from .config import Settings


def _vector_literal(vector: list[float] | None) -> str | None:
    if not vector:
        return None
    return "[" + ",".join(f"{float(v):.8g}" for v in vector) + "]"


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    cleaned = re.sub(r"[^\d.]", "", str(value))
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _text_array(value: Any) -> list[str] | None:
    if not value:
        return None
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    if isinstance(value, str) and "|" in value:
        return [v.strip() for v in value.split("|") if v.strip()]
    return [str(value)]


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def schema_sql(dimensions: int) -> str:
    return f"""
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS contractor_contracts (
    contract_no text PRIMARY KEY,
    list_date text,
    product_name text,
    contract_value numeric,
    seller_name text,
    seller_email text,
    seller_phone text,
    seller_emails text[],
    seller_phones text[],
    seller_gstin text,
    seller_address text,
    source_pdf_sha256 text,
    product_embedding vector({dimensions}),
    quality_flags text[],
    reject_reasons text[],
    is_reject boolean DEFAULT false,
    raw_record jsonb NOT NULL,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS contractor_contracts_product_embedding_idx
ON contractor_contracts USING ivfflat (product_embedding vector_cosine_ops)
WITH (lists = 100);

CREATE INDEX IF NOT EXISTS contractor_contracts_product_name_idx
ON contractor_contracts USING gin (to_tsvector('english', coalesce(product_name, '')));

CREATE INDEX IF NOT EXISTS contractor_contracts_seller_name_idx
ON contractor_contracts USING gin (to_tsvector('english', coalesce(seller_name, '')));

CREATE INDEX IF NOT EXISTS contractor_contracts_list_date_idx
ON contractor_contracts (list_date);

CREATE INDEX IF NOT EXISTS contractor_contracts_seller_gstin_idx
ON contractor_contracts (seller_gstin)
WHERE seller_gstin IS NOT NULL;

CREATE INDEX IF NOT EXISTS contractor_contracts_contract_value_idx
ON contractor_contracts (contract_value DESC)
WHERE contract_value IS NOT NULL;
""".strip()


class ContractorStore:
    def __init__(self, settings: Settings):
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL or POSTGRES_URL is not configured")
        self.settings = settings

    def _connect(self):
        import psycopg

        return psycopg.connect(self.settings.database_url, prepare_threshold=None)

    def ensure_schema(self, dimensions: int | None = None) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(schema_sql(dimensions or self.settings.embedding_dimensions))
            conn.commit()

    def upsert_records(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0

        missing_product_names = sum(1 for rec in records if not rec.get("product_name"))
        if missing_product_names:
            raise RuntimeError(
                f"{missing_product_names} record(s) are missing product_name; "
                "refusing to upsert records without product-name embeddings"
            )
        missing_embeddings = sum(1 for rec in records if not rec.get("product_embedding"))
        has_any_embedding = any(rec.get("product_embedding") for rec in records)
        if missing_embeddings and has_any_embedding:
            raise RuntimeError(
                f"{missing_embeddings} record(s) are missing product_embedding; "
                "run Azure embedding generation before upsert or use FTS-only ingest"
            )

        dimensions = next(
            (
                len(rec["product_embedding"])
                for rec in records
                if isinstance(rec.get("product_embedding"), list) and rec["product_embedding"]
            ),
            self.settings.embedding_dimensions,
        )
        self.ensure_schema(dimensions)

        rows = [self._row_from_record(rec) for rec in records]
        sql = """
INSERT INTO contractor_contracts (
    contract_no, list_date, product_name, contract_value,
    seller_name, seller_email, seller_phone, seller_emails, seller_phones,
    seller_gstin, seller_address, source_pdf_sha256, product_embedding,
    quality_flags, reject_reasons, is_reject, raw_record
) VALUES (
    %(contract_no)s, %(list_date)s, %(product_name)s, %(contract_value)s,
    %(seller_name)s, %(seller_email)s, %(seller_phone)s, %(seller_emails)s, %(seller_phones)s,
    %(seller_gstin)s, %(seller_address)s, %(source_pdf_sha256)s, %(product_embedding)s::vector,
    %(quality_flags)s, %(reject_reasons)s, %(is_reject)s, %(raw_record)s::jsonb
)
ON CONFLICT (contract_no) DO UPDATE SET
    list_date = EXCLUDED.list_date,
    product_name = EXCLUDED.product_name,
    contract_value = EXCLUDED.contract_value,
    seller_name = EXCLUDED.seller_name,
    seller_email = EXCLUDED.seller_email,
    seller_phone = EXCLUDED.seller_phone,
    seller_emails = EXCLUDED.seller_emails,
    seller_phones = EXCLUDED.seller_phones,
    seller_gstin = EXCLUDED.seller_gstin,
    seller_address = EXCLUDED.seller_address,
    source_pdf_sha256 = EXCLUDED.source_pdf_sha256,
    product_embedding = EXCLUDED.product_embedding,
    quality_flags = EXCLUDED.quality_flags,
    reject_reasons = EXCLUDED.reject_reasons,
    is_reject = EXCLUDED.is_reject,
    raw_record = EXCLUDED.raw_record,
    updated_at = now();
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def search_by_vector(self, query_embedding: list[float], limit: int = 20) -> list[dict[str, Any]]:
        sql = """
SELECT
    contract_no, seller_name, seller_email, seller_phone, seller_gstin,
    seller_address, product_name, contract_value, list_date, quality_flags,
    1 - (product_embedding <=> %s::vector) AS score
FROM contractor_contracts
WHERE product_embedding IS NOT NULL AND is_reject IS NOT TRUE
ORDER BY product_embedding <=> %s::vector
LIMIT %s;
"""
        vector = _vector_literal(query_embedding)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (vector, vector, limit))
                return [self._card_from_row(row) for row in cur.fetchall()]

    def search_by_full_text(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Full-text search on product_name + seller_name (GIN-backed tsvector)."""
        sql = """
WITH q AS (
    SELECT plainto_tsquery('english', %s) AS tq
)
SELECT
    c.contract_no,
    c.seller_name,
    c.seller_email,
    c.seller_phone,
    c.seller_gstin,
    c.seller_address,
    c.product_name,
    c.contract_value,
    c.list_date,
    c.quality_flags,
    ts_rank_cd(
        to_tsvector('english', coalesce(c.product_name, '') || ' ' || coalesce(c.seller_name, '')),
        q.tq
    ) AS score
FROM contractor_contracts c
CROSS JOIN q
WHERE c.is_reject IS NOT TRUE
  AND to_tsvector('english', coalesce(c.product_name, '') || ' ' || coalesce(c.seller_name, '')) @@ q.tq
ORDER BY score DESC NULLS LAST, c.contract_no
LIMIT %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (query, limit))
                return [self._card_from_row(row) for row in cur.fetchall()]

    def get_contractor(self, contract_no: str) -> dict[str, Any] | None:
        sql = """
SELECT
    contract_no, seller_name, seller_email, seller_phone, seller_gstin,
    seller_address, product_name, contract_value, list_date, quality_flags,
    NULL::float AS score
FROM contractor_contracts
WHERE contract_no = %s;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (contract_no,))
                row = cur.fetchone()
                return self._card_from_row(row) if row else None

    def validation_stats(self) -> dict[str, int]:
        sql = """
SELECT
    count(*) AS total_rows,
    count(*) FILTER (WHERE is_reject IS NOT TRUE) AS accepted_rows,
    count(*) FILTER (
        WHERE is_reject IS NOT TRUE AND product_embedding IS NOT NULL
    ) AS accepted_rows_with_embedding,
    count(*) FILTER (
        WHERE is_reject IS NOT TRUE AND product_embedding IS NULL
    ) AS accepted_rows_missing_embedding,
    count(*) FILTER (
        WHERE is_reject IS NOT TRUE AND product_name IS NULL
    ) AS accepted_rows_missing_product_name,
    count(*) FILTER (WHERE is_reject IS TRUE) AS rejected_rows
FROM contractor_contracts;
"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
        keys = (
            "total_rows",
            "accepted_rows",
            "accepted_rows_with_embedding",
            "accepted_rows_missing_embedding",
            "accepted_rows_missing_product_name",
            "rejected_rows",
        )
        return {key: int(value or 0) for key, value in zip(keys, row or [])}

    def _row_from_record(self, rec: dict[str, Any]) -> dict[str, Any]:
        ex = rec.get("extraction") or {}
        return {
            "contract_no": rec.get("contract_no"),
            "list_date": rec.get("list_date"),
            "product_name": rec.get("product_name"),
            "contract_value": _decimal_or_none(rec.get("contract_value")),
            "seller_name": rec.get("seller_name"),
            "seller_email": rec.get("seller_email"),
            "seller_phone": rec.get("seller_phone"),
            "seller_emails": _text_array(rec.get("seller_emails")),
            "seller_phones": _text_array(rec.get("seller_phones")),
            "seller_gstin": rec.get("seller_gstin"),
            "seller_address": rec.get("seller_address"),
            "source_pdf_sha256": rec.get("source_pdf_sha256") or ex.get("pdf_sha256"),
            "product_embedding": _vector_literal(rec.get("product_embedding")),
            "quality_flags": _text_array(ex.get("quality_flags") or rec.get("quality_flags")),
            "reject_reasons": _text_array(ex.get("reject_reasons") or rec.get("reject_reasons")),
            "is_reject": bool(ex.get("is_reject") or rec.get("is_reject")),
            "raw_record": json.dumps(rec, ensure_ascii=False),
        }

    def _card_from_row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        return {
            "contract_no": row[0],
            "seller_name": row[1],
            "seller_email": row[2],
            "seller_phone": row[3],
            "seller_gstin": row[4],
            "seller_address": row[5],
            "product_name": row[6],
            "contract_value": _float_or_none(row[7]),
            "list_date": row[8],
            "quality_flags": list(row[9] or []),
            "score": _float_or_none(row[10]),
        }
