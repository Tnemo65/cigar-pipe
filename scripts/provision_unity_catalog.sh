#!/usr/bin/env bash
# Bootstrap Unity Catalog objects: catalog, schemas, and a Storage Credential
# + External Location pointing at the GCS taxi-lake bucket.
# Requires: databricks CLI authenticated (databricks auth login)
# Usage: GCP_PROJECT_ID=my-project DATABRICKS_SA=<sa-email> bash scripts/provision_unity_catalog.sh
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${DATABRICKS_SA:?Set DATABRICKS_SA to the Databricks cluster service account email}"
BUCKET="${GCP_PROJECT_ID}-taxi-lake"
CATALOG="taxi_lakehouse"

echo "==> Creating Unity Catalog catalog: ${CATALOG}"
databricks catalogs create "${CATALOG}" 2>/dev/null || echo "  catalog already exists, skipping"

for schema in bronze silver gold reference; do
    echo "==> Creating schema: ${CATALOG}.${schema}"
    databricks schemas create "${schema}" "${CATALOG}" 2>/dev/null || echo "  schema ${schema} already exists, skipping"
done

echo "==> Creating Storage Credential: taxi-lake-cred"
databricks storage-credentials create \
    --json "$(cat <<EOF
{
  "name": "taxi-lake-cred",
  "gcp_service_account_key": {
    "email": "${DATABRICKS_SA}"
  }
}
EOF
)" 2>/dev/null || echo "  storage credential already exists, skipping"

echo "==> Creating External Location: taxi-lake-location"
databricks external-locations create \
    --json "$(cat <<EOF
{
  "name": "taxi-lake-location",
  "url": "gs://${BUCKET}",
  "credential_name": "taxi-lake-cred"
}
EOF
)" 2>/dev/null || echo "  external location already exists, skipping"

echo "Done. Validate with: databricks external-locations validate --name taxi-lake-location"
