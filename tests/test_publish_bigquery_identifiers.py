from scripts.publish_bigquery import stage_table_name


def test_stage_table_name_is_safe_for_bigquery_identifiers():
    assert stage_table_name("revenue_by_zone_hour", "run-abc") == "_stage_revenue_by_zone_hour_run_abc"
