# NYC Taxi Lakehouse

TLC monthly Parquet → immutable GCS snapshots → Auto Loader Bronze → validated Delta Silver → snapshot DQ → Gold → atomic native BigQuery publication.

- [Implementation and deployment contract](docs/implementation.md)
- [Operations and test commands](docs/operations.md)
- [P0/P1 validation and remaining acceptance work](docs/p0-p1-validation.md)

```sh
uv sync --locked --group dev
uv run pytest -q
uv run python scripts/validate_bundle.py
```

Java 17 is required for Spark tests. Databricks CLI is required for offline bundle-schema checks. Local tests do not certify cloud IAM, deployed staging or production-scale performance; consult the validation report for evidence.
