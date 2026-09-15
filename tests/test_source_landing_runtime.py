from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from src.ingestion.raw_landing import LandingResult
from src.ingestion.source_landing import fetch_snapshot, http_session


def response_session(content, status=200, length=None):
    response = MagicMock(status_code=status)
    response.headers = {"Content-Length": str(length if length is not None else len(content))}
    response.iter_content.return_value = [content[:10], content[10:]]
    response.__enter__.return_value = response
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(str(status))
    session = MagicMock()
    session.get.return_value = response
    return session


def parquet_bytes(tmp_path):
    path = tmp_path/'source.parquet'
    pq.write_table(pa.table({'tpep_pickup_datetime':[datetime(2024,1,1,8)],
                            'tpep_dropoff_datetime':[datetime(2024,1,1,9)],
                            'fare_amount':[12.], 'trip_distance':[2.], 'total_amount':[15.]}), path)
    return path.read_bytes()


def test_source_validates_parquet_and_streams_immutable_snapshot(tmp_path):
    content = parquet_bytes(tmp_path)
    session = response_session(content)
    def upload(filename, uri, checksum):
        assert Path(filename).read_bytes() == content
        assert f'snapshot={checksum}' in uri
        return LandingResult(uri, checksum, '1234', False)
    result = fetch_snapshot('2024-01-01', {'gcp':{'bucket':'lake-staging'}, 'environment':'staging'}, session, upload)
    assert result['rows_received'] == 1
    assert len(result['snapshot_id']) == 64
    assert result['generation'] == '1234'
    assert session.get.call_args.kwargs['stream'] is True


def test_parquet_file_handle_is_closed_before_temporary_directory_cleanup(tmp_path):
    content = parquet_bytes(tmp_path)
    session = response_session(content)
    import hashlib
    uploader = MagicMock(return_value=LandingResult(
        "gs://lake/object", hashlib.sha256(content).hexdigest(), "1", False
    ))
    result = fetch_snapshot(
        "2024-01-01",
        {"gcp": {"bucket": "lake-staging"}, "environment": "staging"},
        session,
        uploader,
    )
    assert result["rows_received"] == 1


@pytest.mark.parametrize('status', [403, 500])
def test_permission_and_server_errors_are_not_no_data(status):
    with pytest.raises(requests.HTTPError):
        fetch_snapshot('2024-01-01', {}, response_session(b'', status))


def test_only_404_is_unpublished():
    assert fetch_snapshot('2024-01-01', {}, response_session(b'',404)) is None


def test_truncated_or_nonparquet_payload_never_lands(tmp_path):
    uploader = MagicMock()
    cfg = {'gcp':{'bucket':'lake-staging'}, 'environment':'staging'}
    with pytest.raises(RuntimeError, match='Incomplete'):
        fetch_snapshot('2024-01-01', cfg, response_session(parquet_bytes(tmp_path),length=1), uploader)
    with pytest.raises(pa.ArrowInvalid):
        fetch_snapshot('2024-01-01', cfg, response_session(b'<html>error</html>'), uploader)
    uploader.assert_not_called()


def test_retry_policy_limits_and_backoff():
    with http_session() as session:
        policy = session.get_adapter('https://example.com').max_retries
        assert policy.total == 4
        assert policy.backoff_factor > 0
        assert 429 in policy.status_forcelist and 503 in policy.status_forcelist
        assert 404 not in policy.status_forcelist
