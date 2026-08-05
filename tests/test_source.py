from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import httpx
import pytest
from dander.ingestion import (
    RECORD_NOT_FOUND,
    ConnectionStatus,
    ConnectorOperation,
    CountResult,
    EnterpriseSourceError,
    RecordNotFound,
    SourceCapabilities,
)

from dander_connector_salesforce.source import SalesforceBulk2Source
from tests.conftest import CsvResponse, FakeAuth, FakeClient

if TYPE_CHECKING:
    from dander.ingestion import SourceConfig

_HEADER = (
    "Id,Name,Type,Industry,AnnualRevenue,NumberOfEmployees,BillingCity,BillingState,"
    "BillingCountry,CreatedDate,LastModifiedDate,SystemModstamp,IsDeleted"
)
_ROW_1 = (
    "001A,Acme,Customer,Technology,125.50,42,Boston,MA,US,2026-01-01T00:00:00.000Z,"
    "2026-08-01T11:00:00.000Z,2026-08-01T11:00:00.000Z,false"
)
_ROW_2 = (
    "001B,Beta,,,,,,,US,2026-01-02T00:00:00.000Z,2026-08-01T12:00:00.000Z,"
    "2026-08-01T12:00:00.000Z,true"
)


def _rest_account(*, account_id: str = "001000000000001AAA") -> dict[str, object]:
    return {
        "attributes": {"type": "Account", "url": f"/sobjects/Account/{account_id}"},
        "Id": account_id,
        "Name": "Acme",
        "Type": "Customer",
        "Industry": "Technology",
        "AnnualRevenue": 125.5,
        "NumberOfEmployees": 42,
        "BillingCity": "Boston",
        "BillingState": "MA",
        "BillingCountry": "US",
        "CreatedDate": "2026-01-01T00:00:00.000Z",
        "LastModifiedDate": "2026-08-01T11:00:00.000Z",
        "SystemModstamp": "2026-08-01T11:00:00.000Z",
        "IsDeleted": False,
    }


def _request_body(request: httpx.Request) -> dict[str, object]:
    payload: object = json.loads(request.content)
    assert isinstance(payload, dict)
    return cast("dict[str, object]", payload)


def test_streams_pages_uses_locators_normalizes_and_cleans_up(
    config: SourceConfig, auth: FakeAuth
) -> None:
    first = CsvResponse([_HEADER, _ROW_1], locator="next-page", records=1)
    second = CsvResponse([_HEADER, _ROW_2], locator="null", records=1)
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "InProgress"},
            {"state": "JobComplete"},
            first,
            second,
            {},
        ]
    )
    sleeps: list[float] = []
    source = SalesforceBulk2Source(config, auth, client=client, sleeper=sleeps.append)

    rows = list(source.extract("accounts"))

    assert [row["Id"] for row in rows] == ["001A", "001B"]
    assert rows[0]["IsDeleted"] is False
    assert rows[1]["IsDeleted"] is True
    assert rows[1]["AnnualRevenue"] is None
    assert client.requests[3].url.params["maxRecords"] == "2"
    assert "locator" not in client.requests[3].url.params
    assert client.requests[4].url.params["locator"] == "next-page"
    assert client.requests[-1].method == "DELETE"
    assert first.closed and second.closed
    assert sleeps == [1.0]


def test_replay_adds_server_side_watermark_filter(config: SourceConfig, auth: FakeAuth) -> None:
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "JobComplete"},
            CsvResponse([_HEADER], locator="null", records=0),
            {},
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    assert list(source.extract("accounts", since="2026-08-01T12:34:56.789123+00:00")) == []

    body = _request_body(client.requests[0])
    assert str(body["query"]).endswith("WHERE SystemModstamp >= 2026-08-01T12:34:56.789Z")
    assert "ORDER BY" not in str(body["query"])
    assert "LIMIT" not in str(body["query"])


def test_failed_job_is_reported_and_deleted(config: SourceConfig, auth: FakeAuth) -> None:
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "Failed", "errorMessage": "invalid field"},
            {},
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="Failed: invalid field"):
        list(source.extract("accounts"))

    assert client.requests[-1].method == "DELETE"


@pytest.mark.parametrize(
    ("lines", "message"),
    [
        ([_HEADER, _ROW_1 + ",unexpected"], "malformed CSV row"),
        ([_HEADER.replace("Id,", "Unknown,"), _ROW_1], "undeclared Salesforce field"),
        ([_HEADER, _ROW_1.replace("false", "not-bool")], "invalid BOOL"),
    ],
)
def test_rejects_malformed_or_undeclared_csv(
    config: SourceConfig,
    auth: FakeAuth,
    lines: list[str],
    message: str,
) -> None:
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "JobComplete"},
            CsvResponse(lines, locator="null", records=1),
            {},
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match=message):
        list(source.extract("accounts"))


def test_streaming_throttle_retries_once_and_closes_response(
    config: SourceConfig, auth: FakeAuth
) -> None:
    request = httpx.Request("GET", "https://salesforce.example.test/results")
    throttled_response = httpx.Response(429, request=request)
    throttled = CsvResponse([], locator="null", records=0)
    throttled._payload = httpx.HTTPStatusError(
        "throttled", request=request, response=throttled_response
    )
    success = CsvResponse([_HEADER], locator="null", records=0)
    client = FakeClient([throttled, success])
    sleeps: list[float] = []
    source = SalesforceBulk2Source(config, auth, client=client, sleeper=sleeps.append)

    response = source._send_streaming(request, "accounts")

    assert response.headers["Sforce-Locator"] == "null"
    assert throttled.closed
    assert sleeps == [0.2]


@pytest.mark.parametrize(
    ("status", "message"),
    [(401, "authentication failed"), (403, "permission denied")],
)
def test_clear_auth_and_permission_failures(
    config: SourceConfig, auth: FakeAuth, status: int, message: str
) -> None:
    request = httpx.Request("POST", "https://salesforce.example.test/jobs/query")
    response = httpx.Response(status, request=request)
    error = httpx.HTTPStatusError("failure", request=request, response=response)
    source = SalesforceBulk2Source(config, auth, client=FakeClient([error]))

    with pytest.raises(EnterpriseSourceError, match=message):
        list(source.extract("accounts"))


def test_repeated_locator_fails_before_unbounded_loop(config: SourceConfig, auth: FakeAuth) -> None:
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "JobComplete"},
            CsvResponse([_HEADER], locator="repeat", records=0),
            CsvResponse([_HEADER], locator="repeat", records=0),
            {},
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="repeated a Salesforce result locator"):
        list(source.extract("accounts"))


def test_declared_discovery_has_no_network(config: SourceConfig, auth: FakeAuth) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(config, auth, client=client)

    discovered = source.discover()

    assert discovered["accounts"]["incremental_cursor"] == "SystemModstamp"
    assert client.requests == []


def test_read_capabilities_are_structurally_discovered(
    config: SourceConfig, auth: FakeAuth
) -> None:
    source = SalesforceBulk2Source(config, auth, client=FakeClient([]))

    assert SourceCapabilities(source).supported_operations == frozenset(
        {
            ConnectorOperation.COUNT,
            ConnectorOperation.GET_SINGLE_OBJECT,
            ConnectorOperation.TEST_CONNECTION,
        }
    )


def test_connection_uses_limits_resource_without_business_records(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([{"DailyApiRequests": {"Max": 15_000, "Remaining": 14_999}}])
    source = SalesforceBulk2Source(config, auth, client=client)

    assert source.test_connection() == ConnectionStatus(ok=True)
    assert client.requests[0].method == "GET"
    assert client.requests[0].url.path.endswith("/services/data/v67.0/limits")
    assert "q" not in client.requests[0].url.params


@pytest.mark.parametrize(
    ("status", "detail"),
    [(401, "authentication failed"), (403, "permission denied")],
)
def test_connection_returns_expected_auth_refusal(
    config: SourceConfig,
    auth: FakeAuth,
    status: int,
    detail: str,
) -> None:
    request = httpx.Request("GET", "https://salesforce.example.test/services/data/v67.0/limits")
    response = httpx.Response(status, request=request)
    error = httpx.HTTPStatusError("failure", request=request, response=response)
    source = SalesforceBulk2Source(config, auth, client=FakeClient([error]))

    assert source.test_connection() == ConnectionStatus(ok=False, detail=detail)


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"DailyApiRequests": {}}, {"DailyApiRequests": {"Max": True, "Remaining": 1}}],
)
def test_connection_rejects_malformed_limits_response(
    config: SourceConfig,
    auth: FakeAuth,
    payload: object,
) -> None:
    source = SalesforceBulk2Source(config, auth, client=FakeClient([payload]))

    with pytest.raises(EnterpriseSourceError, match="limits response was invalid"):
        source.test_connection()


def test_count_uses_exact_aggregate_query_and_optional_watermark(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient(
        [
            {
                "totalSize": 1,
                "done": True,
                "records": [{"attributes": {"type": "AggregateResult"}, "total": 2}],
            }
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    result = source.count("accounts", since="2026-08-01T12:34:56.789123+00:00")

    assert result == CountResult.exact(2)
    assert client.requests[0].method == "GET"
    assert client.requests[0].url.path.endswith("/queryAll")
    assert client.requests[0].url.params["q"] == (
        "SELECT COUNT() total FROM Account WHERE SystemModstamp >= 2026-08-01T12:34:56.789Z"
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"totalSize": 1, "done": False, "records": [{"total": 2}]},
        {"totalSize": 1, "done": True, "records": [{"total": -1}]},
        {"totalSize": 1, "done": True, "records": [{"total": "2"}]},
    ],
)
def test_count_rejects_malformed_aggregate_result(
    config: SourceConfig,
    auth: FakeAuth,
    payload: object,
) -> None:
    source = SalesforceBulk2Source(config, auth, client=FakeClient([payload]))

    with pytest.raises(
        EnterpriseSourceError,
        match="query response was invalid|count returned an invalid",
    ):
        source.count("accounts")


def test_count_accepts_salesforce_total_size_only_response(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    source = SalesforceBulk2Source(
        config,
        auth,
        client=FakeClient([{"totalSize": 2, "done": True, "records": []}]),
    )

    assert source.count("accounts") == CountResult.exact(2)


def test_get_single_object_queries_one_id_and_matches_extract_shape(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    account_id = "001000000000001AAA"
    client = FakeClient(
        [{"totalSize": 1, "done": True, "records": [_rest_account(account_id=account_id)]}]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    record = source.get_single_object("accounts", {"Id": account_id})

    assert not isinstance(record, RecordNotFound)
    assert record["Id"] == account_id
    assert record["AnnualRevenue"] == "125.5"
    assert record["NumberOfEmployees"] == "42"
    assert record["IsDeleted"] is False
    assert "attributes" not in record
    assert client.requests[0].url.path.endswith("/queryAll")
    assert str(client.requests[0].url.params["q"]).endswith(
        f"FROM Account WHERE Id = '{account_id}' LIMIT 1"
    )


def test_get_single_object_returns_named_not_found_sentinel(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    source = SalesforceBulk2Source(
        config,
        auth,
        client=FakeClient([{"totalSize": 0, "done": True, "records": []}]),
    )

    assert source.get_single_object("accounts", {"Id": "001000000000001AAA"}) is RECORD_NOT_FOUND


@pytest.mark.parametrize(
    "identity",
    [{}, {"Other": "001000000000001AAA"}, {"Id": "unsafe value"}],
)
def test_get_single_object_rejects_invalid_identity_before_network(
    config: SourceConfig,
    auth: FakeAuth,
    identity: dict[str, str],
) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="identity field 'Id'|invalid Id") as error:
        source.get_single_object("accounts", identity)

    assert not any(value in str(error.value) for value in identity.values())
    assert client.requests == []
