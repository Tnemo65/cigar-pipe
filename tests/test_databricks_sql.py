import pytest

from scripts.databricks_sql import DatabricksSqlError, execute_statement


def test_execute_statement_polls_until_success():
    responses = iter(
        [
            {"statement_id": "stmt-1", "status": {"state": "PENDING"}},
            {"statement_id": "stmt-1", "status": {"state": "RUNNING"}},
            {
                "statement_id": "stmt-1",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["42"]]},
            },
        ]
    )
    requests = []

    def fake_run(payload):
        requests.append(payload)
        return next(responses)

    result = execute_statement(
        "SELECT 42",
        "warehouse-1",
        poll_seconds=0,
        run_cli=fake_run,
    )

    assert result["result"]["data_array"] == [["42"]]
    assert requests[0]["statement"] == "SELECT 42"
    assert requests[0]["warehouse_id"] == "warehouse-1"
    assert requests[1:] == [{"statement_id": "stmt-1"}] * 2


def test_execute_statement_raises_on_failed_statement():
    responses = iter(
        [
            {"statement_id": "stmt-2", "status": {"state": "PENDING"}},
            {
                "statement_id": "stmt-2",
                "status": {
                    "state": "FAILED",
                    "error": {"message": "invalid SQL"},
                },
            },
        ]
    )

    with pytest.raises(DatabricksSqlError, match="FAILED"):
        execute_statement(
            "SELECT invalid",
            "warehouse-1",
            poll_seconds=0,
            run_cli=lambda _: next(responses),
        )


def test_execute_statement_rejects_nonterminal_response_without_id():
    with pytest.raises(DatabricksSqlError, match="no statement_id"):
        execute_statement(
            "SELECT 1",
            "warehouse-1",
            run_cli=lambda _: {"status": {"state": "PENDING"}},
        )
