from pathlib import Path
from typing import Any

import yaml

CATALOG = "taxi_lakehouse"


def load_config(path: str = "configs/config.yaml") -> dict[str, Any]:
    p = Path(path)
    if not p.is_absolute() and not p.exists():
        _f = globals().get("__file__") or globals().get("filename")
        if _f:
            candidate = Path(_f).resolve().parents[2] / path
            if candidate.exists():
                p = candidate
    with open(p) as f:
        return yaml.safe_load(f)


def catalog_table(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


def _bucket_uri(config: dict[str, Any], *parts: str) -> str:
    bucket = config["gcp"]["bucket"]
    suffix = "/".join(parts)
    return f"gs://{bucket}/{suffix}/"


def raw_yellow_path(config: dict[str, Any]) -> str:
    return _bucket_uri(config, "raw", "yellow")


def raw_ref_path(config: dict[str, Any]) -> str:
    return _bucket_uri(config, "raw", "ref")


def checkpoint_path(name: str, config: dict[str, Any]) -> str:
    return _bucket_uri(config, "_checkpoints", name)


def schema_location_path(name: str, config: dict[str, Any]) -> str:
    return _bucket_uri(config, "_schemas", name)
