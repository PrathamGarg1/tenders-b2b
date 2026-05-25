"""
Structured contract record shape produced by gem_pdf_extract.extract_contract_record.
Used for JSONL / Parquet analytics pipelines.
"""

from __future__ import annotations

from typing import Any, TypedDict


class ContactBlock(TypedDict, total=False):
    organisation_name: str | None
    designation: str | None
    department: str | None
    emails: list[str]
    phones: list[str]


class SellerBlock(TypedDict, total=False):
    company_name: str | None
    gstin: str | None
    emails: list[str]
    phones: list[str]
    address: str | None
    selling_status: str | None
    verification_flags: list[str]


class ConsigneeBlock(TypedDict, total=False):
    address: str | None
    raw: str | None


class ProductBlock(TypedDict, total=False):
    title: str | None
    brand: str | None
    brand_type: str | None
    category: str | None
    category_name: str | None
    quadrant: str | None
    catalogue_status: str | None
    selling_as: str | None
    model: str | None
    hsn_code: str | None
    unit_price: str | None
    total_price: str | None
    quantity: str | None
    uom: str | None


class ContractMeta(TypedDict, total=False):
    contract_no: str | None
    list_date: str | None
    primary_item: str | None
    list_price: str | None


class SpecRow(TypedDict):
    label: str
    value: str


class ExtractionMeta(TypedDict, total=False):
    pages: int
    text_chars: int
    pdf_sha256: str | None
    parse_version: str
    source_map: dict[str, str]
    quality_flags: list[str]
    is_reject: bool
    reject_reasons: list[str]


class DirectoryEmbeddingMeta(TypedDict, total=False):
    provider: str | None
    model: str | None
    dimensions: int | None
    text: str | None


class DirectoryExtractionMeta(TypedDict, total=False):
    pages: int
    pages_scanned: int
    text_chars: int
    pdf_sha256: str | None
    parse_version: str
    mode: str
    source_map: dict[str, str]
    quality_flags: list[str]
    is_reject: bool
    reject_reasons: list[str]


def empty_record(
    contract_meta: ContractMeta | None = None,
) -> dict[str, Any]:
    meta: ContractMeta = contract_meta or {}
    return {
        "contract_meta": {
            "contract_no": meta.get("contract_no"),
            "list_date": meta.get("list_date"),
            "primary_item": meta.get("primary_item"),
            "list_price": meta.get("list_price"),
        },
        "seller": {
            "company_name": None,
            "gstin": None,
            "emails": [],
            "phones": [],
            "address": None,
            "selling_status": None,
            "verification_flags": [],
        },
        "buyer": {
            "organisation_name": None,
            "designation": None,
            "department": None,
            "emails": [],
            "phones": [],
        },
        "consignee": {"address": None, "raw": None},
        "product": {
            "title": None,
            "brand": None,
            "brand_type": None,
            "category": None,
            "category_name": None,
            "quadrant": None,
            "catalogue_status": None,
            "selling_as": None,
            "model": None,
            "hsn_code": None,
            "unit_price": None,
            "total_price": None,
            "quantity": None,
            "uom": None,
        },
        "specifications": [],
        "standards_mentions": [],
        "extraction": {
            "pages": 0,
            "text_chars": 0,
            "pdf_sha256": None,
            "parse_version": "v2_en_section_scoped",
            "source_map": {},
            "quality_flags": [],
            "is_reject": False,
            "reject_reasons": [],
        },
    }


def empty_directory_record(
    contract_meta: ContractMeta | None = None,
) -> dict[str, Any]:
    """Compact app/search record for the contractor directory."""
    meta: ContractMeta = contract_meta or {}
    return {
        "contract_no": meta.get("contract_no"),
        "list_date": meta.get("list_date"),
        "product_name": meta.get("primary_item"),
        "contract_value": meta.get("list_price"),
        "seller_name": None,
        "seller_email": None,
        "seller_phone": None,
        "seller_emails": [],
        "seller_phones": [],
        "seller_gstin": None,
        "seller_address": None,
        "source_pdf_sha256": None,
        "product_embedding": None,
        "embedding": {
            "provider": None,
            "model": None,
            "dimensions": None,
            "text": None,
        },
        "extraction": {
            "pages": 0,
            "pages_scanned": 0,
            "text_chars": 0,
            "pdf_sha256": None,
            "parse_version": "v1_directory_fast",
            "mode": "directory_fast",
            "source_map": {},
            "quality_flags": [],
            "is_reject": False,
            "reject_reasons": [],
        },
    }
