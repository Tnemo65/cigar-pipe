"""Explicit two-phase Unity Catalog bootstrap with a generated GCP credential.

Run credential phase, apply reviewed Terraform with its service-account email,
then run catalog phase. All operations require an explicit CLI profile.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.paths import identifier


def api(profile, method, path, body=None, missing_ok=False):
    command = ["databricks", "api", method, path, "--profile", profile]
    if body is not None:
        command += ["--json", json.dumps(body)]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        if missing_ok and (
            "RESOURCE_DOES_NOT_EXIST" in result.stderr
            or "does not exist" in result.stderr.lower()
            or "not found" in result.stderr.lower()
            or "404" in result.stderr
        ):
            return None
        raise RuntimeError(result.stderr)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def ensure(profile, resource, name, body):
    existing = api(profile, "get", f"/api/2.1/unity-catalog/{resource}/{quote(name)}", missing_ok=True)
    if existing is not None:
        for key in ("url", "credential_name", "storage_root"):
            if key in body and existing.get(key, "").rstrip("/") != body[key].rstrip("/"):
                raise RuntimeError(f"Existing {resource}/{name} has a different {key}")
        return existing
    return api(profile, "post", f"/api/2.1/unity-catalog/{resource}", body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--environment", choices=("dev","staging","prod"), required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--runtime-principal", required=True)
    parser.add_argument("--phase", choices=("credential","catalog"), required=True)
    args = parser.parse_args()
    identifier(args.catalog)
    if not args.catalog.endswith('_'+args.environment) or not args.bucket.endswith('-'+args.environment):
        raise ValueError("Environment suffix mismatch")
    credential = 'taxi_'+args.environment+'_storage'
    result = ensure(args.profile, "storage-credentials", credential,
                    {"name":credential, "databricks_gcp_service_account":{}})
    print(json.dumps({"storage_credential":credential,
                      "uc_storage_service_account":result["databricks_gcp_service_account"]["email"]}))
    if args.phase == "credential":
        return
    for suffix, folder in (("raw","raw"),("managed","managed"),("checkpoints","_checkpoints"),
                            ("schemas","_schemas"),("export","gold_export")):
        name = 'taxi_'+args.environment+'_'+suffix
        ensure(args.profile,"external-locations",name,dict(name=name,
               url=f"gs://{args.bucket}/{args.environment}/{folder}",credential_name=credential,read_only=suffix=="raw"))
        privileges = ["READ_FILES"] if suffix=="raw" else ["READ_FILES","WRITE_FILES"]
        if suffix=="managed":
            privileges = ["CREATE_MANAGED_STORAGE"]
        api(args.profile,"patch",f"/api/2.1/unity-catalog/permissions/external_location/{name}",
            {"changes":[{"principal":args.runtime_principal,"add":privileges}]})
    ensure(args.profile,"catalogs",args.catalog,dict(name=args.catalog,
           storage_root=f"gs://{args.bucket}/{args.environment}/managed"))
    api(args.profile,"patch",f"/api/2.1/unity-catalog/permissions/catalog/{args.catalog}",
        {"changes":[{"principal":args.runtime_principal,"add":["USE_CATALOG"]}]})
    for schema in ("bronze","silver","gold","reference"):
        ensure(args.profile,"schemas",args.catalog+'.'+schema,dict(name=schema,catalog_name=args.catalog))
        api(args.profile,"patch",f"/api/2.1/unity-catalog/permissions/schema/{args.catalog}.{schema}",
            {"changes":[{"principal":args.runtime_principal,"add":["USE_SCHEMA","CREATE_TABLE","SELECT","MODIFY"]}]})


if __name__ == "__main__":
    main()
