"""Flatten nested contract records for Parquet / CSV analytics."""

from __future__ import annotations

import json
from typing import Any


def flatten_for_parquet(rec: dict[str, Any]) -> dict[str, Any]:
    cm = rec.get("contract_meta") or {}
    s = rec.get("seller") or {}
    b = rec.get("buyer") or {}
    c = rec.get("consignee") or {}
    p = rec.get("product") or {}
    ex = rec.get("extraction") or {}
    specs = rec.get("specifications") or []
    stds = rec.get("standards_mentions") or []

    def jsep(xs: list[str] | None) -> str | None:
        if not xs:
            return None
        return "|".join(xs)

    return {
        "contract_no": cm.get("contract_no"),
        "list_date": cm.get("list_date"),
        "primary_item": cm.get("primary_item"),
        "list_price": cm.get("list_price"),
        "seller_company_name": s.get("company_name"),
        "seller_gstin": s.get("gstin"),
        "seller_emails": jsep(s.get("emails")),
        "seller_phones": jsep(s.get("phones")),
        "seller_address": s.get("address"),
        "seller_selling_status": s.get("selling_status"),
        "seller_verification_flags": jsep(s.get("verification_flags")),
        "buyer_organisation_name": b.get("organisation_name"),
        "buyer_designation": b.get("designation"),
        "buyer_department": b.get("department"),
        "buyer_emails": jsep(b.get("emails")),
        "buyer_phones": jsep(b.get("phones")),
        "consignee_address": c.get("address"),
        "consignee_raw": c.get("raw"),
        "product_title": p.get("title"),
        "product_brand": p.get("brand"),
        "product_brand_type": p.get("brand_type"),
        "product_category": p.get("category"),
        "product_category_name": p.get("category_name"),
        "product_quadrant": p.get("quadrant"),
        "product_catalogue_status": p.get("catalogue_status"),
        "product_selling_as": p.get("selling_as"),
        "product_model": p.get("model"),
        "product_hsn_code": p.get("hsn_code"),
        "product_unit_price": p.get("unit_price"),
        "product_total_price": p.get("total_price"),
        "product_quantity": p.get("quantity"),
        "product_uom": p.get("uom"),
        "specifications_json": json.dumps(specs, ensure_ascii=False),
        "standards_mentions": jsep(stds),
        "pdf_pages": ex.get("pages"),
        "pdf_text_chars": ex.get("text_chars"),
        "pdf_sha256": ex.get("pdf_sha256"),
        "parse_version": ex.get("parse_version"),
        "is_reject": ex.get("is_reject"),
        "reject_reasons": jsep(ex.get("reject_reasons")),
        "quality_flags": jsep(ex.get("quality_flags")),
        "source_map_json": json.dumps(ex.get("source_map") or {}, ensure_ascii=False),
    }


def flatten_directory_for_parquet(rec: dict[str, Any]) -> dict[str, Any]:
    ex = rec.get("extraction") or {}
    emb = rec.get("embedding") or {}

    def jsep(xs: list[str] | None) -> str | None:
        if not xs:
            return None
        return "|".join(xs)

    return {
        "contract_no": rec.get("contract_no"),
        "list_date": rec.get("list_date"),
        "product_name": rec.get("product_name"),
        "contract_value": rec.get("contract_value"),
        "seller_name": rec.get("seller_name"),
        "seller_email": rec.get("seller_email"),
        "seller_phone": rec.get("seller_phone"),
        "seller_emails": jsep(rec.get("seller_emails")),
        "seller_phones": jsep(rec.get("seller_phones")),
        "seller_gstin": rec.get("seller_gstin"),
        "seller_address": rec.get("seller_address"),
        "source_pdf_sha256": rec.get("source_pdf_sha256") or ex.get("pdf_sha256"),
        "has_product_embedding": bool(rec.get("product_embedding")),
        "embedding_provider": emb.get("provider"),
        "embedding_model": emb.get("model"),
        "embedding_dimensions": emb.get("dimensions"),
        "embedding_text": emb.get("text"),
        "pdf_pages": ex.get("pages"),
        "pdf_pages_scanned": ex.get("pages_scanned"),
        "pdf_text_chars": ex.get("text_chars"),
        "parse_version": ex.get("parse_version"),
        "extraction_mode": ex.get("mode"),
        "is_reject": ex.get("is_reject"),
        "reject_reasons": jsep(ex.get("reject_reasons")),
        "quality_flags": jsep(ex.get("quality_flags")),
        "source_map_json": json.dumps(ex.get("source_map") or {}, ensure_ascii=False),
    }
