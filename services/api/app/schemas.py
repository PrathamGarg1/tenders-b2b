"""Pydantic API models for mobile search cards."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ContractorCard(BaseModel):
    contract_no: str
    seller_name: str | None = None
    seller_email: str | None = None
    seller_phone: str | None = None
    seller_gstin: str | None = None
    seller_address: str | None = None
    product_name: str | None = None
    contract_value: float | None = None
    list_date: str | None = None
    score: float | None = None
    quality_flags: list[str] = Field(default_factory=list)


class SearchResponse(BaseModel):
    query: str
    count: int
    results: list[ContractorCard]


class HealthResponse(BaseModel):
    ok: bool
    database_configured: bool
    azure_embeddings_configured: bool


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parquet_path: str
    batch_size: int = Field(default=128, ge=1, le=2048)


class IngestResponse(BaseModel):
    rows_loaded: int
    rows_upserted: int
    embeddings_generated: bool
    missing_embeddings: int
    missing_product_names: int
