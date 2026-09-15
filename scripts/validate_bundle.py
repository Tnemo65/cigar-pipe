"""Validate checked-in bundle schema locally without workspace credentials."""
import json
import subprocess
from pathlib import Path

import jsonschema
import regex
import yaml


def pattern(validator, expression, instance, schema):
    # CLI JSON Schema uses Unicode \p{L}/\p{N}; Python re does not implement it.
    if isinstance(instance, str) and not regex.search(expression, instance):
        yield jsonschema.ValidationError(f"{instance!r} does not match {expression!r}")


def main():
    result = subprocess.run(["databricks", "bundle", "schema"], capture_output=True, text=True, encoding="utf-8", check=True)
    schema = json.loads(result.stdout)
    base = jsonschema.validators.validator_for(schema)
    validator = jsonschema.validators.extend(base, {"pattern":pattern})(schema)
    config = yaml.safe_load((Path(__file__).resolve().parents[1]/"jobs/databricks.yml").read_text())
    validator.validate(config)
    required_runtime_args = {
        "--environment", "--catalog", "--bucket", "--project", "--dataset",
        "--pipeline-run-id", "--start-month", "--end-month", "--processing-date",
        "--allow-unpublished",
    }
    for name in config["targets"]:
        target = config["targets"][name]["variables"]
        for key in ("catalog", "bucket", "dataset"):
            if not target[key].endswith(("-" if key == "bucket" else "_") + name):
                raise ValueError(f"Environment isolation failure: {name}/{key}")
    for job_name, job in config["resources"]["jobs"].items():
        for task in job["tasks"]:
            args = set(task["spark_python_task"]["parameters"])
            missing = required_runtime_args - args
            if missing:
                raise ValueError(f"Missing runtime args for {job_name}/{task['task_key']}: {sorted(missing)}")
            if task["spark_python_task"]["parameters"].count("--pipeline-run-id") != 1:
                raise ValueError(f"pipeline-run-id must be supplied exactly once for {job_name}/{task['task_key']}")
    print("all targets: local bundle schema, isolation, and runtime-argument checks passed")


if __name__ == "__main__":
    main()
