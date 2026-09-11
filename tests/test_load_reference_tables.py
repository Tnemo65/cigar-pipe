from scripts.load_reference_tables import load_dim_zone


def test_load_dim_zone_renames_and_flags_sentinels(spark, tmp_path):
    csv_path = tmp_path / "taxi_zone_lookup.csv"
    csv_path.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "4,Manhattan,Alphabet City,Yellow Zone\n"
        "264,Unknown,Unknown,N/A\n"
        "265,N/A,Outside of NYC,N/A\n"
    )

    result = load_dim_zone(spark, str(csv_path))
    rows = {r.location_id: r for r in result.collect()}

    assert rows[4].is_sentinel is False
    assert rows[4].borough == "Manhattan"
    assert rows[264].is_sentinel is True
    assert rows[265].is_sentinel is True
    assert set(result.columns) == {"location_id", "borough", "zone", "service_zone", "is_sentinel"}
