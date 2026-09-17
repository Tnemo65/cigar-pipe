terraform {
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
}

variable "project_id" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid GCP project ID (6-30 lowercase letters, digits, and hyphens)."
  }
}

variable "uc_storage_service_account" {
  type        = string
  description = "Databricks-generated GCP service account of this environment's UC storage credential"
}
variable "region" {
  type    = string
  default = "us-central1"
}
variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Use dev, staging or prod."
  }
}
variable "billing_account_id" {
  type        = string
  default     = ""
  description = "GCP billing account that owns the project budget (required for prod budget state)."
  validation {
    condition     = var.environment != "prod" || can(regex("^[A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+$", var.billing_account_id))
    error_message = "prod requires a billing account ID in XXXXXX-XXXXXX-XXXXXX format."
  }
}
variable "monthly_budget_usd" {
  type        = number
  description = "Hard monthly budget for this environment in USD."
  validation {
    condition     = var.monthly_budget_usd > 0
    error_message = "Set a positive monthly budget so cost alerts can fire."
  }
}
variable "budget_alert_emails" {
  type        = list(string)
  default     = []
  description = "Operational owners that receive budget threshold alerts."
  validation {
    condition     = var.environment != "prod" || length(var.budget_alert_emails) > 0
    error_message = "prod requires at least one budget alert recipient."
  }
}
variable "budget_notification_channels" {
  type        = list(string)
  description = "Cloud Monitoring notification channel IDs for budget alerts (empty uses default billing recipients)."
  default     = []
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  bucket = "${var.project_id}-taxi-lake-${var.environment}"
  prefix = "projects/_/buckets/${local.bucket}/objects/${var.environment}/"

  # Budget is project-scoped and managed once from the production Terraform state.
  # Dev/staging spend is bounded by the runner cost guards and job limits.
  manage_project_budget = var.environment == "prod"
}

resource "google_monitoring_notification_channel" "budget_email" {
  for_each     = local.manage_project_budget ? toset(var.budget_alert_emails) : toset([])
  display_name = "taxi-lakehouse-${var.environment}-budget-${each.key}"
  type         = "email"
  labels = {
    email_address = each.key
  }
}

resource "google_billing_budget" "environment" {
  count           = local.manage_project_budget ? 1 : 0
  billing_account = var.billing_account_id
  display_name    = "taxi-lakehouse-${var.environment}"

  budget_filter {
    projects               = ["projects/${data.google_project.current.number}"]
    credit_types_treatment = "INCLUDE_ALL_CREDITS"
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = floor(var.monthly_budget_usd)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.8
  }
  threshold_rules {
    threshold_percent = 1.0
  }

  all_updates_rule {
    # Keep budget creation independent from unverified email-channel state.
    # Default billing recipients receive threshold notifications; the channel
    # resource remains managed for later explicit wiring.
    monitoring_notification_channels = var.budget_notification_channels
    disable_default_iam_recipients   = false
  }
}

resource "google_storage_bucket" "lake" {
  name                        = local.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  versioning { enabled = true }
  lifecycle_rule {
    condition {
      age            = var.environment == "prod" ? 14 : 3
      matches_prefix = ["${var.environment}/gold_export/"]
    }
    action { type = "Delete" }
  }
  lifecycle { prevent_destroy = true }
}

resource "google_service_account" "runtime" {
  for_each     = toset(["landing", "processing", "serving"])
  account_id   = "taxi-${each.key}-${var.environment}"
  display_name = "Taxi ${each.key} ${var.environment}"
}

resource "google_storage_bucket_iam_member" "landing_read" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.runtime["landing"].email}"
  condition {
    title      = "raw_read_only"
    expression = "resource.name.startsWith('${local.prefix}raw/')"
  }
}

resource "google_storage_bucket_iam_member" "processing_read" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.runtime["processing"].email}"
  condition {
    title      = "raw_and_managed_read_only"
    expression = "resource.name.startsWith('${local.prefix}raw/') || resource.name.startsWith('${local.prefix}managed/')"
  }
}

resource "google_storage_bucket_iam_member" "serving_read" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.runtime["serving"].email}"
  condition {
    title      = "managed_read_only"
    expression = "resource.name.startsWith('${local.prefix}managed/')"
  }
}

resource "google_storage_bucket_iam_member" "raw_create" {
  # Landing only creates immutable raw objects; it has no read or delete grant.
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.runtime["landing"].email}"
  condition {
    title      = "immutable_raw_only"
    expression = "resource.name.startsWith('${local.prefix}raw/')"
  }
}

resource "google_storage_bucket_iam_member" "processing_write" {
  bucket = google_storage_bucket.lake.name
  # Checkpoints and Auto Loader schema state are updated in place. Keep the
  # broader objectAdmin role restricted to those two non-raw prefixes.
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.runtime["processing"].email}"
  condition {
    title      = "checkpoint_schema_only"
    expression = "resource.name.startsWith('${local.prefix}_checkpoints/') || resource.name.startsWith('${local.prefix}_schemas/')"
  }
}

resource "google_storage_bucket_iam_member" "export_write" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.runtime["serving"].email}"
  condition {
    title      = "staging_export_only"
    expression = "resource.name.startsWith('${local.prefix}gold_export/')"
  }
}

resource "google_storage_bucket_iam_member" "uc_read" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.uc_storage_service_account}"
}

resource "google_storage_bucket_iam_member" "uc_raw_read" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.uc_storage_service_account}"
  condition {
    title      = "uc_raw_read_only"
    expression = "resource.name.startsWith('${local.prefix}raw/')"
  }
}


resource "google_storage_bucket_iam_member" "uc_write" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.uc_storage_service_account}"
  condition {
    title      = "managed_and_operational_paths_only"
    expression = "resource.name.startsWith('${local.prefix}managed/') || resource.name.startsWith('${local.prefix}_checkpoints/') || resource.name.startsWith('${local.prefix}_schemas/') || resource.name.startsWith('${local.prefix}gold_export/')"
  }
}

resource "google_storage_bucket_iam_member" "bigquery_source_reader" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:service-${data.google_project.current.number}@gs-project-accounts.iam.gserviceaccount.com"
  condition {
    title      = "serving_exports_only"
    expression = "resource.name.startsWith('${local.prefix}gold_export/')"
  }
}

resource "google_bigquery_dataset" "serving" {
  dataset_id                 = "taxi_analytics_${var.environment}"
  location                   = var.region
  delete_contents_on_destroy = false
  lifecycle { prevent_destroy = true }
}

resource "google_bigquery_dataset_iam_member" "serving_editor" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runtime["serving"].email}"
}

resource "google_bigquery_dataset_iam_member" "github_publisher_editor" {
  dataset_id = google_bigquery_dataset.serving.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:taxi-github-${var.environment}@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.runtime["serving"].email}"
}

output "runtime_service_accounts" {
  value = { for name, sa in google_service_account.runtime : name => sa.email }
}
