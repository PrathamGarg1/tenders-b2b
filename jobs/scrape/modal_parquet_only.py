"""
Merge directory_records JSON → directory_parquet on the shared Modal volume.

Uses a SEPARATE app name so modal run does not stop gem_contracts_full_analytics_v1
(Azure backup or other jobs on that app keep running).

  modal run --detach modal_parquet_only.py::merge_directory_parquet \\
    --run-id full_2026_20260518T053357Z

Then ingest (different app: gem-contractor-directory-api):

  bash scripts/ingest-parquet.sh GEM_CONTRACTOR_DIRECTORY_full_2026_20260518T053357Z.parquet
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import modal

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRAPE_LIB = Path(__file__).resolve().parent / "lib"

data_volume = modal.Volume.from_name("gem_contracts_analytics_v1", create_if_missing=True)
runtime_secret = modal.Secret.from_name("gem-contractor-directory-secrets")

VOLUME_DIR = "/data"

MODAL_MOUNT_IGNORE = [
    "**/.venv/**",
    "**/__pycache__/**",
    "**/.git/**",
    "**/.cursor/**",
    "**/.expo/**",
    "**/node_modules/**",
    "apps/mobile/**",
    "scripts/**",
    "docs/**",
    "*.parquet",
    "*.pdf",
    ".env",
]

image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("pandas", "pyarrow")
    .add_local_dir(REPO_ROOT, remote_path="/repo", ignore=MODAL_MOUNT_IGNORE)
)

# Different name from gem_contracts_full_analytics_v1 — will not replace/stop that app.
app = modal.App(name="gem_contractor_parquet_ingest_v1")

def _setup_path() -> None:
    for p in ("/repo/jobs/scrape/lib", "/repo"):
        if p not in sys.path:
            sys.path.insert(0, p)


@app.function(
    image=image,
    timeout=86400,
    max_containers=1,
    memory=2048,
    retries=1,
    nonpreemptible=True,
    scaledown_window=120,
    volumes={VOLUME_DIR: data_volume},
    secrets=[runtime_secret],
)
def merge_directory_parquet(
    run_id: str,
    outfile_name: str = "",
    json_batch_size: int = 2000,
) -> dict:
    """Combine directory_records/<run_id>/**/*.json → directory_parquet/*.parquet (volume only)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    t0 = time.time()
    _setup_path()
    from gem_record_flatten import flatten_directory_for_parquet

    data_volume.reload()

    env_bs = os.environ.get("GEM_PARQUET_MERGE_BATCH", "").strip()
    if env_bs:
        try:
            json_batch_size = max(200, int(env_bs))
        except ValueError:
            pass

    base = Path(VOLUME_DIR) / "directory_records" / run_id.strip("/")
    if not base.exists():
        msg = f"Missing directory_records/{run_id}"
        print(msg)
        return {"status": "empty", "run_id": run_id, "message": msg}

    out_dir = Path(VOLUME_DIR) / "directory_parquet"
    out_dir.mkdir(parents=True, exist_ok=True)
    name = outfile_name or f"GEM_CONTRACTOR_DIRECTORY_{run_id}.parquet"
    out_path = out_dir / name
    bs = max(200, int(json_batch_size))

    print(
        f"[merge] start run_id={run_id} out={out_path} batch_size={bs} "
        f"nonpreemptible=True (indexing json on volume next — can take 5–15 min)"
    )

    print("[merge] indexing .json paths on volume...")
    paths: list[Path] = []
    for n, p in enumerate(base.rglob("*.json"), 1):
        paths.append(p)
        if n % 25000 == 0:
            print(f"[merge] indexed {n} paths ({int(time.time() - t0)}s elapsed)")
    paths.sort()
    if not paths:
        msg = f"No JSON under directory_records/{run_id}"
        print(msg)
        return {"status": "empty", "run_id": run_id, "message": msg}

    print(
        f"[merge] found {len(paths)} json files in {int(time.time() - t0)}s — writing parquet..."
    )

    writer: pq.ParquetWriter | None = None
    total = 0
    num_batches = (len(paths) + bs - 1) // bs
    for batch_idx, i in enumerate(range(0, len(paths), bs), 1):
        chunk_paths = paths[i : i + bs]
        rows: list[dict] = []
        for p in chunk_paths:
            rec = json.loads(p.read_text(encoding="utf-8"))
            rows.append(flatten_directory_for_parquet(rec))
        if not rows:
            continue
        table = pa.Table.from_pylist(rows)
        if writer is None:
            writer = pq.ParquetWriter(str(out_path), table.schema, compression="snappy")
        elif table.schema != writer.schema:
            table = table.cast(writer.schema)
        writer.write_table(table)
        total += len(rows)
        print(
            f"[merge] batch {batch_idx}/{num_batches} rows={total} "
            f"({int(time.time() - t0)}s elapsed)"
        )

    if writer is None:
        return {"status": "empty", "run_id": run_id, "rows": 0}

    writer.close()
    data_volume.commit()
    summary = {
        "status": "complete",
        "run_id": run_id,
        "parquet_path": str(out_path),
        "volume_parquet": f"directory_parquet/{name}",
        "rows": total,
        "json_files": len(paths),
    }
    print(json.dumps(summary, indent=2))
    print("[merge] DONE — container exiting")
    return summary


@app.local_entrypoint()
def merge_directory_parquet_entry(
    run_id: str = "full_2026_20260518T053357Z",
    outfile_name: str = "",
    wait: bool = False,
) -> None:
    if wait:
        print(merge_directory_parquet.remote(run_id=run_id, outfile_name=outfile_name or None))
        return
    call = merge_directory_parquet.spawn(run_id=run_id, outfile_name=outfile_name or None)
    print(
        json.dumps(
            {
                "spawned_merge": call.object_id,
                "run_id": run_id,
                "app": "gem_contractor_parquet_ingest_v1",
                "note": "Does not stop gem_contracts_full_analytics_v1 (Azure backup).",
                "next": f"bash scripts/ingest-parquet.sh GEM_CONTRACTOR_DIRECTORY_{run_id}.parquet",
            },
            indent=2,
        )
    )
