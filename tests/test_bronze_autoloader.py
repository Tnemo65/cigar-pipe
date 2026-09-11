from src.ingestion.bronze_autoloader import add_lineage_columns


def test_add_lineage_columns_adds_ingested_at(spark):
    df = spark.createDataFrame([(1,)], ["col_a"])
    result = add_lineage_columns(df)
    assert "_ingested_at" in result.columns
    assert "_source_file" in result.columns
    row = result.collect()[0]
    assert row._ingested_at is not None
