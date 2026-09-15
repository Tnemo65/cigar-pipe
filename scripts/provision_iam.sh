#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' 'Provisioning moved to infra/main.tf. Use a separate Terraform state per environment.' 'Run terraform plan first; inspect legacy project-level IAM bindings before applying.' >&2
exit 1
