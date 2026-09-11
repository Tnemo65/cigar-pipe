#!/usr/bin/env bash
# Grant IAM roles to the Databricks service account so it can:
#   - Read/write GCS (Storage Object Admin on the taxi-lake bucket)
#   - Write BigQuery (BigQuery Data Editor + Job User on the project)
# Usage: GCP_PROJECT_ID=my-project DATABRICKS_SA=<sa-email> bash scripts/provision_iam.sh
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${DATABRICKS_SA:?Set DATABRICKS_SA to the Databricks cluster service account email}"
BUCKET="${GCP_PROJECT_ID}-taxi-lake"

echo "==> GCS: granting Storage Object Admin on bucket ${BUCKET}"
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --member="serviceAccount:${DATABRICKS_SA}" \
    --role="roles/storage.objectAdmin"

echo "==> BigQuery: granting Data Editor on project"
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${DATABRICKS_SA}" \
    --role="roles/bigquery.dataEditor"

echo "==> BigQuery: granting Job User on project"
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${DATABRICKS_SA}" \
    --role="roles/bigquery.jobUser"

echo "==> Pub/Sub: granting Subscriber on taxi-lake-sub"
gcloud pubsub subscriptions add-iam-policy-binding "taxi-lake-sub" \
    --project="${GCP_PROJECT_ID}" \
    --member="serviceAccount:${DATABRICKS_SA}" \
    --role="roles/pubsub.subscriber"

echo "Done."
