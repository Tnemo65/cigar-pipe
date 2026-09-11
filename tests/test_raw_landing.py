import pytest

from src.ingestion.raw_landing import LandingConflict, LandingResult, land_bytes


def test_land_bytes_requires_content():
    with pytest.raises(ValueError, match="must not be empty"):
        land_bytes(b"", "gs://bucket/object", lambda *_: None)


def test_land_bytes_validates_uploader_checksum():
    with pytest.raises(LandingConflict, match="checksum mismatch"):
        land_bytes(
            b"payload",
            "gs://bucket/object",
            lambda *_: LandingResult("gs://bucket/object", "wrong", None, False),
        )


def test_land_bytes_returns_idempotent_result():
    result = land_bytes(
        b"payload",
        "gs://bucket/object",
        lambda content, uri, checksum: LandingResult(uri, checksum, "42", True),
    )

    assert result.already_landed is True
    assert result.generation == "42"
