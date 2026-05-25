"""Search orchestration for contractor cards."""

from __future__ import annotations

from typing import Literal

from .config import Settings
from .db import ContractorStore
from .embeddings import AzureEmbeddingClient


def _search_contractors_vector(query: str, settings: Settings, *, limit: int) -> list[dict]:
    settings.require_azure_embeddings()
    store = ContractorStore(settings)
    embedding = AzureEmbeddingClient(settings).embed_query(query)
    return store.search_by_vector(embedding, limit=limit)


def search_contractors(
    query: str,
    settings: Settings,
    *,
    limit: int = 20,
    mode: Literal["fts", "vector"] | None = None,
) -> list[dict]:
    clean_query = query.strip()
    if not clean_query:
        return []

    settings.require_database()
    if mode is not None:
        effective: Literal["fts", "vector"] = mode
    elif settings.search_mode == "vector":
        effective = "vector"
    else:
        effective = "fts"

    if effective == "vector":
        return _search_contractors_vector(clean_query, settings, limit=limit)

    store = ContractorStore(settings)
    return store.search_by_full_text(clean_query, limit=limit)
