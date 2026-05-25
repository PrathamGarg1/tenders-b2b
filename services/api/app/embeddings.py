"""Azure OpenAI embedding helpers."""

from __future__ import annotations

import re

from .config import Settings


def normalize_embedding_text(value: str | None) -> str:
    text = re.sub(r"\s+", " ", value or "").strip().lower()
    return text or "contractor product"


class AzureEmbeddingClient:
    def __init__(self, settings: Settings):
        if not settings.azure_embeddings_configured:
            raise RuntimeError("Azure OpenAI embeddings are not configured")
        from openai import AzureOpenAI

        self._deployment = settings.azure_openai_embedding_deployment
        self._client = AzureOpenAI(
            api_key=settings.azure_openai_api_key,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.azure_openai_api_version,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        response = self._client.embeddings.create(
            model=self._deployment,
            input=[normalize_embedding_text(t) for t in texts],
        )
        return [list(item.embedding) for item in response.data]

    def embed_query(self, query: str) -> list[float]:
        return self.embed_texts([query])[0]
