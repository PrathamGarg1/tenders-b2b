"""Modal deployment entrypoint for the contractor directory API."""

from __future__ import annotations

from pathlib import Path

import modal

API_ROOT = Path(__file__).resolve().parent
REPO_ROOT = API_ROOT.parent.parent
VOLUME_DIR = "/data"
RUNTIME_SECRET_NAME = "gem-contractor-directory-secrets"

data_volume = modal.Volume.from_name("gem_contracts_analytics_v1", create_if_missing=True)
runtime_secret = modal.Secret.from_name(RUNTIME_SECRET_NAME)

MODAL_MOUNT_IGNORE = [
    "**/.venv/**",
    "**/__pycache__/**",
    "**/.pytest_cache/**",
    "**/.mypy_cache/**",
    "**/.ruff_cache/**",
    "**/.git/**",
    "**/.cursor/**",
    "**/.expo/**",
    "**/node_modules/**",
    "apps/mobile/**",
    "jobs/scrape/**",
    "scripts/**",
    "docs/**",
    "*.parquet",
    "*.pdf",
    ".env",
]

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "fastapi",
        "uvicorn",
        "pandas",
        "pyarrow",
        "openai",
        "psycopg[binary]",
        "pydantic",
    )
    .add_local_dir(REPO_ROOT, remote_path="/repo", ignore=MODAL_MOUNT_IGNORE)
)

app = modal.App(name="gem-contractor-directory-api")


@app.function(
    image=image,
    timeout=86400,
    min_containers=0,
    max_containers=10,
    secrets=[runtime_secret],
)
@modal.asgi_app()
def api():
    import sys

    sys.path.insert(0, "/repo/services/api")
    from app.main import app as fastapi_app

    return fastapi_app


@app.function(
    image=image,
    timeout=86400,
    max_containers=1,
    memory=4096,
    retries=2,
    nonpreemptible=True,
    scaledown_window=120,
    volumes={VOLUME_DIR: data_volume},
    secrets=[runtime_secret],
)
def ingest_directory_parquet(
    parquet_path: str = "",
    *,
    batch_size: int = 128,
    ingest_all: bool = True,
    require_embeddings: bool | None = None,
) -> dict:
    """Modal job: directory parquet -> Postgres. Single non-preemptible worker; exits when done."""
    import os
    import sys
    from pathlib import Path

    print(f"[ingest] nonpreemptible=True parquet_path={parquet_path or '(all GEM_CONTRACTOR_DIRECTORY_*.parquet)'}")

    sys.path.insert(0, "/repo/services/api")
    from app.config import get_settings
    from app.db import ContractorStore
    from app.ingest import ingest_parquet

    if require_embeddings is None:
        flag = os.environ.get("INGEST_REQUIRE_EMBEDDINGS", "0").strip().lower()
        require_embeddings = flag in ("1", "true", "yes")

    base = Path(VOLUME_DIR) / "directory_parquet"
    if parquet_path:
        path = Path(parquet_path)
        paths = [path if path.is_absolute() else base / path]
    else:
        paths = sorted(base.glob("GEM_CONTRACTOR_DIRECTORY_*.parquet"))
        if not ingest_all and paths:
            paths = [paths[-1]]

    if not paths:
        raise FileNotFoundError(f"No directory parquet files found under {base}")

    settings = get_settings()
    settings.require_database()
    if require_embeddings:
        settings.require_azure_embeddings()

    totals = {
        "files": 0,
        "rows_loaded": 0,
        "rows_upserted": 0,
        "missing_embeddings": 0,
        "missing_product_names": 0,
        "require_embeddings": require_embeddings,
        "parquet_paths": [],
    }
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(str(path))
        report = ingest_parquet(
            path,
            settings,
            batch_size=batch_size,
            require_embeddings=require_embeddings,
        )
        totals["files"] += 1
        totals["rows_loaded"] += report.rows_loaded
        totals["rows_upserted"] += report.rows_upserted
        totals["missing_embeddings"] += report.missing_embeddings
        totals["missing_product_names"] += report.missing_product_names
        totals["parquet_paths"].append(str(path))
        print(
            {
                "parquet_path": str(path),
                "rows_loaded": report.rows_loaded,
                "rows_upserted": report.rows_upserted,
                "require_embeddings": require_embeddings,
            }
        )

    totals["db_validation"] = ContractorStore(settings).validation_stats()
    totals["status"] = "complete"
    print({"db_validation": totals["db_validation"]})
    print("[ingest] DONE — container exiting")
    return totals


@app.local_entrypoint()
def ingest(
    parquet_path: str = "",
    batch_size: int = 128,
    ingest_all: bool = True,
    require_embeddings: bool = False,
) -> None:
    print(
        ingest_directory_parquet.remote(
            parquet_path=parquet_path,
            batch_size=batch_size,
            ingest_all=ingest_all,
            require_embeddings=require_embeddings,
        )
    )
