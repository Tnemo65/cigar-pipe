#!/usr/bin/env bash
# Provision GCS bucket with uniform bucket-level access and Pub/Sub
# notification for Auto Loader file-notification mode.
# Usage: GCP_PROJECT_ID=my-project bash scripts/provision_gcp_resources.sh
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="us-central1"
BUCKET="${GCP_PROJECT_ID}-taxi-lake"
TOPIC="taxi-lake-notifications"
SUBSCRIPTION="taxi-lake-sub"

echo "==> Creating GCS bucket: gs://${BUCKET}"
gcloud storage buckets create "gs://${BUCKET}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access \
    --no-public-access-prevention 2>/dev/null || echo "  bucket already exists, skipping"

echo "==> Creating Pub/Sub topic: ${TOPIC}"
gcloud pubsub topics create "${TOPIC}" \
    --project="${GCP_PROJECT_ID}" 2>/dev/null || echo "  topic already exists, skipping"

echo "==> Creating Pub/Sub subscription: ${SUBSCRIPTION}"
gcloud pubsub subscriptions create "${SUBSCRIPTION}" \
    --topic="${TOPIC}" \
    --project="${GCP_PROJECT_ID}" \
    --message-retention-duration=7d \
    --ack-deadline=600 2>/dev/null || echo "  subscription already exists, skipping"

echo "==> Linking GCS bucket notifications -> Pub/Sub topic"
# Allow GCS service account to publish to the topic
GCS_SA="$(gcloud storage service-agent --project="${GCP_PROJECT_ID}")"
gcloud pubsub topics add-iam-policy-binding "${TOPIC}" \
    --project="${GCP_PROJECT_ID}" \
    --member="serviceAccount:${GCS_SA}" \
    --role="roles/pubsub.publisher"

gcloud storage buckets notifications create "gs://${BUCKET}" \
    --topic="${TOPIC}" \
    --project="${GCP_PROJECT_ID}" \
    --event-types=OBJECT_FINALIZE \
    --payload-format=json 2>/dev/null || echo "  notification already exists, skipping"

echo "==> Creating BigQuery dataset: taxi_analytics"
bq mk --dataset \
    --location="${REGION}" \
    --project_id="${GCP_PROJECT_ID}" \
    "${GCP_PROJECT_ID}:taxi_analytics" 2>/dev/null || echo "  dataset already exists, skipping"

echo "Done."
