from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import httpx
import pytest
from dander.ingestion import EnterpriseSourceError

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
