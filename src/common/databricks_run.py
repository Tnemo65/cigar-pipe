"""Extract a Databricks job run ID from CLI output."""
from __future__ import annotations

import json
import re
from typing import Any


RUN_URL_PATTERN = re.compile(r"/runs/(\d+)(?:[/?#]|$)")
JOB_URL_PATTERN = re.compile(r"/run/(\d+)(?:[/?#]|$)")


def extract_run_id(output: str | bytes | dict[str, Any]) -> str:
    if isinstance(output, bytes):
        output = output.decode("utf-8")
    if isinstance(output, dict):
        payload = output
        text = json.dumps(output)
    else:
        text = output.strip()
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = None
    if isinstance(payload, dict):
        for key in ("run_id", "job_run_id", "id"):
            if payload.get(key) is not None:
                return str(payload[key])
        for value in payload.values():
            if isinstance(value, str):
                match = RUN_URL_PATTERN.search(value) or JOB_URL_PATTERN.search(value)
                if match:
                    return match.group(1)
    for pattern in (RUN_URL_PATTERN, JOB_URL_PATTERN):
        match = pattern.search(text)
        if match:
            return match.group(1)
    if re.fullmatch(r"\d+", text):
        return text
    raise ValueError("Could not extract a Databricks run ID from bundle output")
