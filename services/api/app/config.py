"""Runtime configuration for the contractor directory API."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """API settings. ``search_mode`` defaults to ``fts`` (see ``SEARCH_MODE`` env)."""

    database_url: str | None
    azure_openai_endpoint: str | None
    azure_openai_api_key: str | None
    azure_openai_embedding_deployment: str | None
    azure_openai_api_version: str
    admin_token: str | None
    embedding_dimensions: int
    cors_origins: list[str]
    search_mode: str

    @property
    def database_configured(self) -> bool:
        return bool(self.database_url)

    @property
    def azure_embeddings_configured(self) -> bool:
        return bool(
            self.azure_openai_endpoint
            and self.azure_openai_api_key
            and self.azure_openai_embedding_deployment
        )

    def require_database(self) -> None:
        if not self.database_configured:
            raise RuntimeError("DATABASE_URL or POSTGRES_URL is not configured")

    def require_azure_embeddings(self) -> None:
        if not self.azure_embeddings_configured:
            raise RuntimeError("Azure OpenAI embeddings are not configured")


def get_settings() -> Settings:
    origins_raw = os.environ.get("CORS_ORIGINS", "*")
    origins = [o.strip() for o in origins_raw.split(",") if o.strip()] or ["*"]
    mode_raw = (os.environ.get("SEARCH_MODE") or "fts").strip().lower()
    search_mode = mode_raw if mode_raw in ("fts", "vector") else "fts"
    return Settings(
        database_url=os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL"),
        azure_openai_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
        azure_openai_api_key=os.environ.get("AZURE_OPENAI_API_KEY"),
        azure_openai_embedding_deployment=(
            os.environ.get("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
            or os.environ.get("AZURE_OPENAI_EMBEDDING_MODEL")
        ),
        azure_openai_api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        admin_token=os.environ.get("ADMIN_TOKEN"),
        embedding_dimensions=int(os.environ.get("EMBEDDING_DIMENSIONS", "1536")),
        cors_origins=origins,
        search_mode=search_mode,
    )
