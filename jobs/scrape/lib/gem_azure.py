"""Upload structured records and raw PDFs to Azure Blob Storage."""

from __future__ import annotations

import gzip
import io
import json
import os
from typing import Any


def azure_configured() -> bool:
    return bool(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))


def _container_name() -> str:
    return os.environ.get("AZURE_STORAGE_CONTAINER", "gem-contracts")


def _client():
    from azure.storage.blob import BlobServiceClient

    conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set")
    return BlobServiceClient.from_connection_string(conn)


def upload_pdf(
    *,
    pdf_bytes: bytes,
    blob_path: str,
    content_type: str = "application/pdf",
) -> None:
    if not azure_configured():
        return
    from azure.storage.blob import ContentSettings

    svc = _client()
    container = svc.get_container_client(_container_name())
    blob = container.get_blob_client(blob_path)
    blob.upload_blob(
        pdf_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )


def upload_json_gz_lines(records: list[dict[str, Any]], blob_path: str) -> None:
    if not azure_configured() or not records:
        return
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for rec in records:
            line = json.dumps(rec, ensure_ascii=False) + "\n"
            gz.write(line.encode("utf-8"))
    buf.seek(0)
    from azure.storage.blob import ContentSettings

    svc = _client()
    container = svc.get_container_client(_container_name())
    blob = container.get_blob_client(blob_path)
    blob.upload_blob(
        buf.getvalue(),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/gzip"),
    )


def upload_bytes(data: bytes, blob_path: str, content_type: str) -> None:
    if not azure_configured():
        return
    from azure.storage.blob import ContentSettings

    svc = _client()
    container = svc.get_container_client(_container_name())
    blob = container.get_blob_client(blob_path)
    blob.upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )
