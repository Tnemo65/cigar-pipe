import json
import subprocess
import sys
from pathlib import Path

from src.common.databricks_run import extract_run_id


def test_extract_run_id_works_from_jobs_directory(tmp_path, monkeypatch):
    output = tmp_path / "run.json"
    output.write_text(json.dumps({"run_page_url": "https://workspace/#job/1/run/123"}), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    assert extract_run_id(output.read_text()) == "123"
