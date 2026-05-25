# GeM directory scrape (Modal)

Entrypoint: `modal_app.py`. Run from repo root:

```bash
bash scripts/run-scrape.sh
```

Requires `GEM_COOKIE` on Modal secret `gem-contractor-directory-secrets`.

Outputs on volume `gem_contracts_analytics_v1`:

- `/data/directory_parquet/GEM_CONTRACTOR_DIRECTORY_<RUN_ID>.parquet`
- `/data/directory_records/<RUN_ID>/...`
