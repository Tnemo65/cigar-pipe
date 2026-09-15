"""Read-only reconciliation for a specific completed pipeline run."""
import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from google.cloud import bigquery
from scripts.cloud_integration import cli
from scripts.databricks_sql import execute_statement
from src.common.runtime import runtime_config, month_start
from src.common import paths


def assert_quarantine_rate(raw_count: int, quarantine_count: int, threshold: float) -> float:
    if raw_count <= 0:
        raise ValueError("raw row count must be positive")
    rate = quarantine_count / raw_count
    if rate > threshold:
        raise AssertionError(
            f"quarantine rate {rate:.4f} exceeds threshold {threshold:.4f}"
        )
    return rate


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", default="")
    parser.add_argument("--warehouse-id",required=True)
    args,remaining = parser.parse_known_args()
    cfg = runtime_config(remaining)
    def request(payload):
        profile_args = ["--profile", args.profile] if args.profile else []
        if "statement_id" in payload:
            return cli("api", "get", "/api/2.0/sql/statements/" + payload["statement_id"], *profile_args)
        return cli("api", "post", "/api/2.0/sql/statements", "--json", json.dumps(payload), *profile_args)
    def query(sql):
        response = execute_statement(sql,args.warehouse_id,run_cli=request)
        return response.get("result",{}).get("data_array",[])
    state_table = paths.catalog_table("reference","pipeline_run_state",cfg)
    state = query(
        f"SELECT status,payload FROM {state_table} "
        f"WHERE pipeline_run_id='{cfg['pipeline_run_id']}' AND task_name='monitor'"
    )
    if len(state)!=1 or state[0][0] not in ("SUCCESS","NO_DATA"):
        raise RuntimeError("Run has no successful monitoring state")
    payload=json.loads(state[0][1])
    if state[0][0]=="NO_DATA":
        print(json.dumps({"status":"NO_DATA","pipeline_run_id":cfg['pipeline_run_id']}))
        return
    project,dataset=cfg['gcp']['project_id'],cfg['bigquery']['dataset']
    client=bigquery.Client(project=project)
    receipt=list(client.query(f"SELECT pipeline_run_id FROM `{project}.{dataset}.publication_runs` WHERE pipeline_run_id=@run_id",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('run_id','STRING',cfg['pipeline_run_id'])])).result())
    if len(receipt)!=1:
        raise RuntimeError("Missing/duplicate atomic publication receipt")
    for metric in payload['gold_metrics']:
        month=month_start(metric['month']); mart=paths.identifier(metric['mart'])
        sql=f"SELECT COUNT(*) AS rows, COALESCE(SUM(trip_count),0) AS trips FROM `{project}.{dataset}.{mart}` WHERE pickup_month=DATE '{month}'"
        actual=list(client.query(sql).result())[0]
        if actual.rows!=metric['rows'] or actual.trips!=metric['trips']:
            raise RuntimeError(f"Serving reconciliation failed: {mart}/{month}")
        if mart=='revenue_by_zone_hour':
            source=query(f"SELECT COALESCE(SUM(total_revenue),0) FROM {paths.catalog_table('gold',mart,cfg)} WHERE pickup_month=DATE '{month}'")[0][0]
            actual=list(client.query(f"SELECT COALESCE(SUM(total_revenue),0) AS revenue FROM `{project}.{dataset}.{mart}` WHERE pickup_month=DATE '{month}'").result())[0].revenue
            if Decimal(str(source))!=Decimal(str(actual)):
                raise RuntimeError(f"Revenue mismatch: {month}")
    print(json.dumps({"status":"SUCCESS","pipeline_run_id":cfg['pipeline_run_id'],"months":payload['months']}))


if __name__ == "__main__":
    main()
