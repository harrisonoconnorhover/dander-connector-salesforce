from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import unquote

import httpx
import pytest
from dander.ingestion import (
    RECORD_NOT_FOUND,
    ConnectionStatus,
    ConnectorOperation,
    CountResult,
    DeleteOutcome,
    EnterpriseSourceError,
    RawField,
    RecordNotFound,
    SourceCapabilities,
    SourceConfig,
)

from dander_connector_salesforce.source import (
    SalesforceBulk2Source,
    SalesforceExternalIdUpsertSource,
    _query_body,
)
from tests.conftest import CsvResponse, FakeAuth, FakeClient, JsonResponse

if TYPE_CHECKING:
    from collections.abc import Mapping

_FIXTURES = Path(__file__).parent / "fixtures"


def _fixture_lines(endpoint: str) -> list[str]:
    return (_FIXTURES / f"{endpoint}.csv").read_text(encoding="utf-8").splitlines()


_HEADER, _ROW_1, _ROW_2 = _fixture_lines("accounts")


def _rest_account(*, account_id: str = "001000000000001AAA") -> dict[str, object]:
    return {
        "attributes": {"type": "Account", "url": f"/sobjects/Account/{account_id}"},
        "Id": account_id,
        "Name": "Acme",
        "OwnerId": "005000000000001AAA",
        "ParentId": None,
        "Type": "Customer",
        "Industry": "Technology",
        "AnnualRevenue": 125.5,
        "NumberOfEmployees": 42,
        "Website": "https://acme.example",
        "Phone": "+1-617-555-0100",
        "BillingCity": "Boston",
        "BillingState": "MA",
        "BillingPostalCode": "02110",
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


def _upsert_config(config: SourceConfig) -> SourceConfig:
    account = config.endpoints[0]
    request_body = dict(account.request_body)
    request_body["upsert_external_id_field"] = "Dander_External_ID__c"
    query = str(request_body["query"])
    request_body["query"] = query.replace("SELECT Id,", "SELECT Id, Dander_External_ID__c,", 1)
    configured_account = account.model_copy(
        update={
            "request_body": request_body,
            "primary_key": ["Dander_External_ID__c"],
            "raw_schema": [
                *account.raw_schema,
                RawField(
                    name="Dander_External_ID__c",
                    type="STRING",
                    mode="REQUIRED",
                ),
            ],
        }
    )
    return config.model_copy(update={"endpoints": [configured_account, *config.endpoints[1:]]})


def _describe_external_id(*, external: bool = True, unique: bool = True) -> dict[str, object]:
    return {
        "fields": [
            {
                "name": "Dander_External_ID__c",
                "externalId": external,
                "unique": unique,
            }
        ]
    }


class _StatefulUpsertClient:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.records: dict[str, dict[str, object]] = {}
        self.created = 0
        self.updated = 0

    def send(self, request: httpx.Request, *, stream: bool = False) -> JsonResponse:
        assert stream is False
        self.requests.append(request)
        if request.method == "GET" and request.url.path.endswith("/describe"):
            return JsonResponse(_describe_external_id())
        assert request.method == "PATCH"
        raw_value = request.url.raw_path.decode().rsplit("/", 1)[-1]
        external_value = unquote(raw_value)
        body = _request_body(request)
        if external_value in self.records:
            self.updated += 1
            self.records[external_value].update(body)
        else:
            self.created += 1
            self.records[external_value] = dict(body)
        return JsonResponse({})


@pytest.mark.parametrize(
    ("endpoint", "boolean_field", "second_boolean", "operation"),
    [
        ("accounts", "IsDeleted", True, "queryAll"),
        ("contacts", "IsDeleted", True, "queryAll"),
        ("opportunities", "IsDeleted", True, "queryAll"),
        ("users", "IsActive", False, "query"),
    ],
)
def test_streams_multiple_pages_for_every_endpoint_and_cleans_up(
    config: SourceConfig,
    auth: FakeAuth,
    endpoint: str,
    boolean_field: str,
    second_boolean: bool,
    operation: str,
) -> None:
    header, first_row, second_row = _fixture_lines(endpoint)
    first = CsvResponse([header, first_row], locator="next-page", records=1)
    second = CsvResponse([header, second_row], locator="null", records=1)
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

    rows = list(source.extract(endpoint))

    assert len(rows) == 2
    assert rows[0][boolean_field] is not second_boolean
    assert rows[1][boolean_field] is second_boolean
    if endpoint == "accounts":
        assert rows[1]["AnnualRevenue"] is None
    if endpoint == "contacts":
        assert rows[1]["Email"] is None
    assert client.requests[3].url.params["maxRecords"] == "2"
    assert "locator" not in client.requests[3].url.params
    assert client.requests[4].url.params["locator"] == "next-page"
    assert client.requests[-1].method == "DELETE"
    assert _request_body(client.requests[0])["operation"] == operation
    assert first.closed and second.closed
    assert sleeps == [1.0]


@pytest.mark.parametrize("endpoint", ["accounts", "contacts", "opportunities", "users"])
def test_empty_endpoint_response_is_valid(
    config: SourceConfig,
    auth: FakeAuth,
    endpoint: str,
) -> None:
    header = _fixture_lines(endpoint)[0]
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "JobComplete"},
            CsvResponse([header], locator="null", records=0),
            {},
        ]
    )

    assert list(SalesforceBulk2Source(config, auth, client=client).extract(endpoint)) == []


@pytest.mark.parametrize("endpoint", ["accounts", "contacts", "opportunities", "users"])
def test_replay_adds_inclusive_server_side_watermark_filter(
    config: SourceConfig,
    auth: FakeAuth,
    endpoint: str,
) -> None:
    header = _fixture_lines(endpoint)[0]
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "JobComplete"},
            CsvResponse([header], locator="null", records=0),
            {},
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    assert list(source.extract(endpoint, since="2026-08-01T12:34:56.789123+00:00")) == []

    body = _request_body(client.requests[0])
    assert str(body["query"]).endswith("WHERE SystemModstamp >= 2026-08-01T12:34:56.789Z")
    assert "ORDER BY" not in str(body["query"])
    assert "LIMIT" not in str(body["query"])


def test_cleanup_refusal_does_not_hide_successful_rows(
    config: SourceConfig,
    auth: FakeAuth,
    caplog: pytest.LogCaptureFixture,
) -> None:
    request = httpx.Request("DELETE", "https://salesforce.example.test/jobs/query/750-job")
    response = httpx.Response(403, request=request)
    cleanup_error = httpx.HTTPStatusError("permission denied", request=request, response=response)
    client = FakeClient(
        [
            {"id": "750-job"},
            {"state": "JobComplete"},
            CsvResponse([_HEADER, _ROW_1], locator="null", records=1),
            cleanup_error,
        ]
    )

    rows = list(SalesforceBulk2Source(config, auth, client=client).extract("accounts"))

    assert len(rows) == 1
    assert "salesforce_query_job_cleanup_failed" in caplog.text


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
            ConnectorOperation.CREATE,
            ConnectorOperation.DELETE,
            ConnectorOperation.GET_DELETED,
            ConnectorOperation.GET_SINGLE_OBJECT,
            ConnectorOperation.TEST_CONNECTION,
            ConnectorOperation.UPDATE,
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
                "totalSize": 2,
                "done": True,
                "records": [],
            }
        ]
    )
    source = SalesforceBulk2Source(config, auth, client=client)

    result = source.count("accounts", since="2026-08-01T12:34:56.789123+00:00")

    assert result == CountResult.exact(2)
    assert client.requests[0].method == "GET"
    assert client.requests[0].url.path.endswith("/queryAll")
    assert client.requests[0].url.params["q"] == (
        "SELECT COUNT() FROM Account WHERE SystemModstamp >= 2026-08-01T12:34:56.789Z"
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


def test_get_deleted_returns_business_keys_and_forwards_window(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient(
        [
            {
                "earliestDateAvailable": "2026-07-22T12:00:00.000+0000",
                "latestDateCovered": "2026-08-06T12:00:00.000+0000",
                "deletedRecords": [
                    {
                        "id": "001000000000001AAA",
                        "deletedDate": "2026-08-02T10:30:00.000+0000",
                    },
                    {
                        "id": "001000000000002AAA",
                        "deletedDate": "2026-08-03T10:30:00.000+0000",
                    },
                ],
            }
        ]
    )
    source = SalesforceBulk2Source(
        config,
        auth,
        client=client,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=UTC),
    )

    assert list(source.get_deleted("accounts", since="2026-08-01T00:00:00Z")) == [
        {"Id": "001000000000001AAA"},
        {"Id": "001000000000002AAA"},
    ]
    request = client.requests[0]
    assert request.method == "GET"
    assert request.url.path.endswith("/sobjects/Account/deleted")
    assert request.url.params["start"] == "2026-08-01T00:00:00.000Z"
    assert request.url.params["end"] == "2026-08-06T12:00:00.000Z"


def test_get_deleted_defaults_to_salesforce_retention_window(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([{"deletedRecords": []}])
    source = SalesforceBulk2Source(
        config,
        auth,
        client=client,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=UTC),
    )

    assert list(source.get_deleted("contacts")) == []
    assert client.requests[0].url.params["start"] == "2026-07-22T12:00:00.000Z"


@pytest.mark.parametrize(
    ("endpoint", "since", "message"),
    [
        ("users", None, "does not expose a deleted-record feed"),
        ("accounts", "2026-07-21T00:00:00Z", "retained for 15 days"),
        ("accounts", "2026-08-06T12:00:00Z", "must precede the end"),
        ("accounts", "2026-08-07T00:00:00Z", "must precede the end"),
    ],
)
def test_get_deleted_rejects_unsupported_endpoint_or_window_before_network(
    config: SourceConfig,
    auth: FakeAuth,
    endpoint: str,
    since: str | None,
    message: str,
) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(
        config,
        auth,
        client=client,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=UTC),
    )

    with pytest.raises(EnterpriseSourceError, match=message):
        list(source.get_deleted(endpoint, since=since))

    assert client.requests == []


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"deletedRecords": [None]},
        {"deletedRecords": [{"id": "invalid", "deletedDate": "2026-08-02T00:00:00Z"}]},
        {"deletedRecords": [{"id": "001000000000001AAA"}]},
    ],
)
def test_get_deleted_rejects_malformed_response(
    config: SourceConfig,
    auth: FakeAuth,
    payload: object,
) -> None:
    source = SalesforceBulk2Source(
        config,
        auth,
        client=FakeClient([payload]),
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=UTC),
    )

    with pytest.raises(EnterpriseSourceError, match="deleted-record response was invalid"):
        list(source.get_deleted("opportunities", since="2026-08-01T00:00:00Z"))


def test_create_posts_one_declared_record_and_returns_identity(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([{"id": "001000000000003AAA", "success": True, "errors": []}])
    source = SalesforceBulk2Source(config, auth, client=client)

    assert source.create("accounts", {"Name": "Dander Test Account"}) == {
        "Id": "001000000000003AAA"
    }
    request = client.requests[0]
    assert request.method == "POST"
    assert request.url.path.endswith("/sobjects/Account")
    assert _request_body(request) == {"Name": "Dander Test Account"}
    assert auth.requests == 1


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"UnknownField": "value"},
        {"Id": "001000000000003AAA", "Name": "Dander Test Account"},
    ],
)
def test_create_rejects_empty_undeclared_or_read_only_fields_before_network(
    config: SourceConfig,
    auth: FakeAuth,
    record: dict[str, object],
) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="at least one field|undeclared|read-only"):
        source.create("accounts", record)

    assert client.requests == []


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"id": "invalid", "success": True, "errors": []},
        {"id": "001000000000003AAA", "success": False, "errors": []},
        {"id": "001000000000003AAA", "success": True, "errors": ["failure"]},
    ],
)
def test_create_rejects_malformed_response(
    config: SourceConfig,
    auth: FakeAuth,
    payload: object,
) -> None:
    source = SalesforceBulk2Source(config, auth, client=FakeClient([payload]))

    with pytest.raises(EnterpriseSourceError, match="create response was invalid"):
        source.create("accounts", {"Name": "Dander Test Account"})


def test_create_does_not_retry_ambiguous_transport_failure(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    request = httpx.Request("POST", "https://salesforce.example.test/sobjects/Account")
    client = FakeClient([httpx.ReadTimeout("ambiguous", request=request)])
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="ambiguous; write was not retried"):
        source.create("accounts", {"Name": "Dander Test Account"})

    assert len(client.requests) == 1
    assert auth.requests == 1


def test_update_patches_one_record_and_returns_identity(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([{}])
    source = SalesforceBulk2Source(config, auth, client=client)
    identity = {"Id": "001000000000003AAA"}

    assert source.update("accounts", identity, {"Name": "Updated Account"}) == identity
    request = client.requests[0]
    assert request.method == "PATCH"
    assert request.url.path.endswith("/sobjects/Account/001000000000003AAA")
    assert _request_body(request) == {"Name": "Updated Account"}
    assert auth.requests == 1


@pytest.mark.parametrize(
    "identity",
    [{}, {"Other": "001000000000003AAA"}, {"Id": "invalid"}],
)
def test_update_rejects_invalid_identity_before_network(
    config: SourceConfig,
    auth: FakeAuth,
    identity: dict[str, str],
) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="identity field 'Id'|invalid Id"):
        source.update("accounts", identity, {"Name": "Updated Account"})

    assert client.requests == []


def test_update_reuses_declared_field_validation(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="read-only"):
        source.update(
            "accounts",
            {"Id": "001000000000003AAA"},
            {"SystemModstamp": "2026-08-06T12:00:00Z"},
        )

    assert client.requests == []


def test_delete_removes_one_record_and_returns_closed_outcome(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([{}])
    source = SalesforceBulk2Source(config, auth, client=client)

    assert source.delete("accounts", {"Id": "001000000000003AAA"}) is DeleteOutcome.DELETED
    request = client.requests[0]
    assert request.method == "DELETE"
    assert request.url.path.endswith("/sobjects/Account/001000000000003AAA")
    assert auth.requests == 1


def test_delete_returns_not_found_for_repeatable_absent_record(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    request = httpx.Request(
        "DELETE", "https://salesforce.example.test/sobjects/Account/001000000000003AAA"
    )
    response = httpx.Response(404, request=request)
    error = httpx.HTTPStatusError("not found", request=request, response=response)
    client = FakeClient([error])
    source = SalesforceBulk2Source(config, auth, client=client)

    assert source.delete("accounts", {"Id": "001000000000003AAA"}) is DeleteOutcome.NOT_FOUND
    assert len(client.requests) == 1


def test_delete_rejects_invalid_identity_before_network(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    client = FakeClient([])
    source = SalesforceBulk2Source(config, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match="invalid Id"):
        source.delete("accounts", {"Id": "invalid"})

    assert client.requests == []


def test_upsert_creates_then_updates_one_external_identity_without_duplicate(
    config: SourceConfig,
    auth: FakeAuth,
) -> None:
    configured = _upsert_config(config)
    client = _StatefulUpsertClient()
    source = SalesforceExternalIdUpsertSource(configured, auth, client=client)
    external_value = "account/with ? reserved"

    assert source.upsert(
        "accounts",
        {"Dander_External_ID__c": external_value, "Name": "First Name"},
    ) == {"Dander_External_ID__c": external_value}
    assert source.upsert(
        "accounts",
        {"Dander_External_ID__c": external_value, "Name": "Updated Name"},
    ) == {"Dander_External_ID__c": external_value}

    assert client.created == 1
    assert client.updated == 1
    assert client.records == {external_value: {"Name": "Updated Name"}}
    patches = [request for request in client.requests if request.method == "PATCH"]
    assert len(patches) == 2
    assert patches[0].url.raw_path.endswith(b"account%2Fwith%20%3F%20reserved")
    assert _request_body(patches[0]) == {"Name": "First Name"}


def test_upsert_control_is_not_sent_to_bulk_query_jobs(config: SourceConfig) -> None:
    configured = _upsert_config(config)

    body = _query_body(configured.endpoints[0], None)

    assert "upsert_external_id_field" not in body
    assert body["operation"] == "queryAll"


@pytest.mark.parametrize(
    ("configure", "record", "message"),
    [
        (False, {"Name": "Missing control"}, "non-Id upsert_external_id_field"),
        (True, {"Name": "Missing key"}, "requires a non-empty string"),
        (True, {"Dander_External_ID__c": "", "Name": "Empty key"}, "non-empty string"),
        (True, {"Dander_External_ID__c": ["not", "scalar"], "Name": "Bad"}, "non-empty string"),
    ],
)
def test_upsert_rejects_missing_or_non_scalar_external_identity_before_network(
    config: SourceConfig,
    auth: FakeAuth,
    configure: bool,
    record: Mapping[str, object],
    message: str,
) -> None:
    selected = _upsert_config(config) if configure else config
    client = FakeClient([])
    source = SalesforceExternalIdUpsertSource(selected, auth, client=client)

    with pytest.raises(EnterpriseSourceError, match=message):
        source.upsert("accounts", record)

    assert client.requests == []


@pytest.mark.parametrize(
    ("control", "primary_key", "query_field", "schema_field", "message"),
    [
        ("Id", ["Id"], True, True, "non-Id upsert_external_id_field"),
        ("Parent.External__c", ["Parent.External__c"], True, True, "non-Id"),
        ("Dander_External_ID__c", ["Id"], True, True, "sole primary key"),
        ("Dander_External_ID__c", ["Dander_External_ID__c"], False, True, "selected"),
        ("Dander_External_ID__c", ["Dander_External_ID__c"], True, False, "undeclared"),
    ],
)
def test_upsert_requires_an_explicit_declared_external_identity(
    config: SourceConfig,
    auth: FakeAuth,
    control: str,
    primary_key: list[str],
    query_field: bool,
    schema_field: bool,
    message: str,
) -> None:
    account = config.endpoints[0]
    request_body = dict(account.request_body)
    request_body["upsert_external_id_field"] = control
    if query_field:
        request_body["query"] = str(request_body["query"]).replace(
            "SELECT Id,", f"SELECT Id, {control},", 1
        )
    raw_schema = list(account.raw_schema)
    if schema_field:
        raw_schema.append(RawField(name=control.replace(".", "_"), type="STRING"))
    configured = config.model_copy(
        update={
            "endpoints": [
                account.model_copy(
                    update={
                        "request_body": request_body,
                        "primary_key": primary_key,
                        "raw_schema": raw_schema,
                    }
                ),
                *config.endpoints[1:],
            ]
        }
    )
    source = SalesforceExternalIdUpsertSource(configured, auth, client=FakeClient([]))

    with pytest.raises(EnterpriseSourceError, match=message):
        source.upsert("accounts", {control: "external-001", "Name": "Test Account"})


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"fields": []},
        _describe_external_id(external=False),
        _describe_external_id(unique=False),
    ],
)
def test_upsert_requires_verified_unique_external_id_metadata(
    config: SourceConfig,
    auth: FakeAuth,
    payload: object,
) -> None:
    source = SalesforceExternalIdUpsertSource(
        _upsert_config(config),
        auth,
        client=FakeClient([payload]),
    )

    with pytest.raises(
        EnterpriseSourceError,
        match="describe response was invalid|metadata was unavailable|unique External ID",
    ):
        source.upsert(
            "accounts",
            {"Dander_External_ID__c": "account-001", "Name": "Test Account"},
        )


@pytest.mark.parametrize("failure", ["permission", "ambiguous"])
def test_upsert_reports_write_failure_without_retry(
    config: SourceConfig,
    auth: FakeAuth,
    failure: str,
) -> None:
    request = httpx.Request("PATCH", "https://salesforce.example.test/upsert")
    if failure == "permission":
        response = httpx.Response(403, request=request)
        write_error: httpx.HTTPError = httpx.HTTPStatusError(
            "forbidden", request=request, response=response
        )
        message = "permission denied"
    else:
        write_error = httpx.ReadTimeout("ambiguous", request=request)
        message = "ambiguous"
    client = FakeClient([_describe_external_id(), write_error])
    source = SalesforceExternalIdUpsertSource(_upsert_config(config), auth, client=client)

    with pytest.raises(EnterpriseSourceError, match=message):
        source.upsert(
            "accounts",
            {"Dander_External_ID__c": "account-001", "Name": "Test Account"},
        )

    assert len(client.requests) == 2
