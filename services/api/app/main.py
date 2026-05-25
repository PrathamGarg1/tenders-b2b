"""FastAPI app for the contractor directory mobile backend."""

from __future__ import annotations

from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .config import Settings, get_settings
from .db import ContractorStore
from .ingest import ingest_parquet
from .schemas import ContractorCard, HealthResponse, IngestRequest, IngestResponse, SearchResponse
from .search import search_contractors


settings = get_settings()

app = FastAPI(title="GeM Contractor Directory API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def current_settings() -> Settings:
    return get_settings()


def require_admin(
    settings: Settings = Depends(current_settings),
    authorization: str | None = Header(default=None),
) -> None:
    if not settings.admin_token:
        return
    expected = f"Bearer {settings.admin_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Invalid admin token")


@app.get("/health", response_model=HealthResponse)
def health(settings: Settings = Depends(current_settings)) -> HealthResponse:
    return HealthResponse(
        ok=True,
        database_configured=settings.database_configured,
        azure_embeddings_configured=settings.azure_embeddings_configured,
    )


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=50),
    mode: Literal["fts", "vector"] | None = Query(
        default=None,
        description="Override SEARCH_MODE: fts = Postgres full-text; vector = Azure + pgvector.",
    ),
    settings: Settings = Depends(current_settings),
) -> SearchResponse:
    if not settings.database_configured:
        raise HTTPException(status_code=503, detail="Database is not configured")

    if mode is not None:
        effective_mode: Literal["fts", "vector"] = mode
    elif settings.search_mode == "vector":
        effective_mode = "vector"
    else:
        effective_mode = "fts"

    if effective_mode == "vector" and not settings.azure_embeddings_configured:
        raise HTTPException(
            status_code=503,
            detail="Azure embeddings are not configured (required for vector search)",
        )

    try:
        rows = search_contractors(q, settings, limit=limit, mode=mode)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        detail = "vector search failed" if effective_mode == "vector" else "full-text search failed"
        raise HTTPException(status_code=500, detail=detail) from exc
    results = [ContractorCard(**row) for row in rows]
    return SearchResponse(query=q, count=len(results), results=results)


@app.get("/contractors/{contract_no}", response_model=ContractorCard)
def get_contractor(
    contract_no: str,
    settings: Settings = Depends(current_settings),
) -> ContractorCard:
    if not settings.database_configured:
        raise HTTPException(status_code=503, detail="Database is not configured")
    row = ContractorStore(settings).get_contractor(contract_no)
    if not row:
        raise HTTPException(status_code=404, detail="Contractor record not found")
    return ContractorCard(**row)


@app.post("/admin/ingest", response_model=IngestResponse, dependencies=[Depends(require_admin)])
def ingest(
    req: IngestRequest,
    settings: Settings = Depends(current_settings),
) -> IngestResponse:
    if not settings.database_configured:
        raise HTTPException(status_code=503, detail="Database is not configured")
    if not settings.azure_embeddings_configured:
        raise HTTPException(status_code=503, detail="Azure embeddings are not configured")
    try:
        report = ingest_parquet(req.parquet_path, settings, batch_size=req.batch_size)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return IngestResponse(
        rows_loaded=report.rows_loaded,
        rows_upserted=report.rows_upserted,
        embeddings_generated=report.embeddings_generated,
        missing_embeddings=report.missing_embeddings,
        missing_product_names=report.missing_product_names,
    )
