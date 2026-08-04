"""Bounded Salesforce Bulk API 2.0 Query adapter."""

from __future__ import annotations

import csv
import logging
import re
from datetime import UTC, datetime
from time import sleep
from typing import TYPE_CHECKING, Any, Protocol, cast

import httpx
from dander.ingestion import (
    Endpoint,
    EnterpriseSource,
    EnterpriseSourceError,
    HeaderCursorPagination,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from dander.ingestion import EnterpriseHttpClient, SourceConfig
    from dander.security import AuthStrategy

_LOGGER = logging.getLogger(__name__)
_ACTIVE_STATES = frozenset({"UploadComplete", "InProgress"})
_FAILED_STATES = frozenset({"Aborted", "Failed"})
_MAX_POLLS = 240
_SOQL_SCALE_BREAKERS = re.compile(r"\b(?:GROUP\s+BY|LIMIT|OFFSET|ORDER\s+BY|TYPEOF|WHERE)\b", re.I)


class _StreamingResponse(Protocol):
    headers: Mapping[str, str]

    def raise_for_status(self) -> object: ...

    def iter_lines(self) -> Iterator[str]: ...

    def close(self) -> None: ...


class _StreamingHttpClient(Protocol):
    def send(self, request: httpx.Request, *, stream: bool) -> _StreamingResponse: ...


class SalesforceBulk2Source(EnterpriseSource):
    """Run bounded, server-filtered Salesforce Bulk API 2.0 query jobs."""

    def __init__(
        self,
        config: SourceConfig,
        auth: AuthStrategy,
        *,
        client: EnterpriseHttpClient | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        super().__init__(config, auth, client=client, sleeper=sleeper)

    def discover(self) -> Mapping[str, Any]:
        """Return declared query schemas without contacting Salesforce."""
        return {
            endpoint.name: {
                "path": endpoint.path,
                "primary_key": list(endpoint.primary_key),
                "incremental_cursor": endpoint.incremental_cursor,
                "raw_schema": [field.model_dump(by_alias=True) for field in endpoint.raw_schema],
            }
            for endpoint in self.config.endpoints
        }

    def extract(self, endpoint: str, *, since: str | None = None) -> Iterator[Mapping[str, Any]]:
        """Create one query job, stream every result page, and delete the completed job."""
        declaration = self._endpoint(endpoint)
        pagination = _validate_endpoint(declaration)
        body = _query_body(declaration, since)
        job_url = f"{self.config.base_url.rstrip('/')}/{declaration.path.lstrip('/')}"
        response = self._send(
            httpx.Request(
                "POST",
                job_url,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json=body,
            ),
            endpoint,
        )
        job_id = _job_id(response.json(), endpoint)
        try:
            self._await_job(job_url, job_id, endpoint)
            yield from self._results(job_url, job_id, endpoint, declaration, pagination)
        finally:
            self._delete_job(job_url, job_id, endpoint)

    def _await_job(self, job_url: str, job_id: str, endpoint: str) -> None:
        poll_url = f"{job_url}/{job_id}"
        for attempt in range(_MAX_POLLS):
            response = self._send(
                httpx.Request("GET", poll_url, headers={"Accept": "application/json"}),
                endpoint,
            )
            payload = _job_payload(response.json(), endpoint)
            state = payload.get("state")
            if state == "JobComplete":
                return
            if state in _FAILED_STATES:
                detail = payload.get("errorMessage")
                suffix = f": {detail}" if isinstance(detail, str) and detail else ""
                raise EnterpriseSourceError(
                    f"Salesforce Bulk API job for endpoint {endpoint!r} ended in {state}{suffix}"
                )
            if state not in _ACTIVE_STATES:
                raise EnterpriseSourceError(
                    f"Salesforce Bulk API job for endpoint {endpoint!r} returned "
                    f"unknown state {state!r}"
                )
            if attempt + 1 < _MAX_POLLS:
                self._sleep(self._poll_delay())
        raise EnterpriseSourceError(
            f"Salesforce Bulk API job for endpoint {endpoint!r} did not finish within "
            f"{_MAX_POLLS} polls"
        )

    def _results(
        self,
        job_url: str,
        job_id: str,
        endpoint: str,
        declaration: Endpoint,
        pagination: HeaderCursorPagination,
    ) -> Iterator[Mapping[str, Any]]:
        results_url = f"{job_url}/{job_id}/results"
        locator: str | None = None
        seen_locators: set[str] = set()
        while True:
            params: dict[str, str | int] = {pagination.size_param: pagination.page_size}
            if locator is not None:
                params[pagination.cursor_param] = locator
            response = self._send_streaming(
                httpx.Request(
                    "GET",
                    results_url,
                    params=params,
                    headers={"Accept": "text/csv", "Accept-Encoding": "gzip"},
                ),
                endpoint,
            )
            try:
                rows = csv.DictReader(response.iter_lines())
                fieldnames = list(rows.fieldnames or ())
                if fieldnames:
                    fieldnames[0] = fieldnames[0].removeprefix("\ufeff")
                    rows.fieldnames = fieldnames
                _validate_csv_fields(fieldnames, declaration)
                page_rows = 0
                for index, row in enumerate(rows):
                    if None in row or any(value is None for value in row.values()):
                        raise EnterpriseSourceError(
                            f"Endpoint {endpoint!r} returned malformed CSV row {index}"
                        )
                    page_rows += 1
                    yield _normalize_csv_row(row, declaration, index=index)
                _validate_page_count(response.headers, page_rows, endpoint)
                next_locator = response.headers.get(pagination.next_cursor_header)
            finally:
                response.close()

            if next_locator is None:
                raise EnterpriseSourceError(
                    f"Endpoint {endpoint!r} response omitted {pagination.next_cursor_header!r}"
                )
            if next_locator == pagination.terminal_value:
                return
            if next_locator in seen_locators:
                raise EnterpriseSourceError(
                    f"Endpoint {endpoint!r} repeated a Salesforce result locator"
                )
            seen_locators.add(next_locator)
            locator = next_locator

    def _send_streaming(self, request: httpx.Request, endpoint: str) -> _StreamingResponse:
        policy = self.config.rate_limit
        max_retries = policy.max_retries if policy is not None else 0
        client = cast("_StreamingHttpClient", self._client)
        for attempt in range(max_retries + 1):
            response: _StreamingResponse | None = None
            try:
                response = client.send(self._auth.apply(request), stream=True)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                if response is not None:
                    response.close()
                status = error.response.status_code
                if 400 <= status < 500 and status != 429:
                    reason = {401: "authentication failed", 403: "permission denied"}.get(
                        status, "request was rejected"
                    )
                    raise EnterpriseSourceError(
                        f"Endpoint {endpoint!r} {reason} (HTTP {status})"
                    ) from error
                retry_error: httpx.HTTPError = error
            except httpx.HTTPError as error:
                if response is not None:
                    response.close()
                retry_error = error
            if attempt == max_retries:
                raise EnterpriseSourceError(
                    f"Endpoint {endpoint!r} request failed after bounded retries"
                ) from retry_error
            assert policy is not None
            multiplier = 2**attempt if policy.backoff.value == "exponential" else 1
            self._sleep(multiplier / policy.requests_per_second)
        raise AssertionError("bounded streaming retry loop did not return or raise")

    def _delete_job(self, job_url: str, job_id: str, endpoint: str) -> None:
        try:
            self._send(httpx.Request("DELETE", f"{job_url}/{job_id}"), endpoint)
        except EnterpriseSourceError:
            _LOGGER.warning(
                "salesforce_query_job_cleanup_failed",
                extra={"dander_event": "salesforce_query_job_cleanup_failed", "endpoint": endpoint},
            )

    def _poll_delay(self) -> float:
        if self.config.rate_limit is None:
            return 1.0
        return max(1.0, 1 / self.config.rate_limit.requests_per_second)


def _validate_endpoint(endpoint: Endpoint) -> HeaderCursorPagination:
    pagination = endpoint.pagination
    if not isinstance(pagination, HeaderCursorPagination):
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint.name!r} requires header_cursor pagination"
        )
    if endpoint.path.rstrip("/") != "/jobs/query":
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint.name!r} must target /jobs/query"
        )
    if not endpoint.raw_schema:
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint.name!r} requires a declared raw schema"
        )
    query = endpoint.request_body.get("query")
    operation = endpoint.request_body.get("operation")
    if not isinstance(query, str) or not query.strip():
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint.name!r} requires request_body.query"
        )
    if operation not in {"query", "queryAll"}:
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint.name!r} requires query or queryAll operation"
        )
    if ";" in query or _SOQL_SCALE_BREAKERS.search(query):
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint.name!r} query must be one unfiltered, "
            "unordered SELECT; Dander adds the watermark filter and preserves PK chunking"
        )
    return pagination


def _query_body(endpoint: Endpoint, since: str | None) -> dict[str, object]:
    body: dict[str, object] = dict(endpoint.request_body)
    query = cast("str", body["query"]).strip()
    if since is not None:
        if endpoint.incremental_cursor is None:
            raise EnterpriseSourceError(
                f"Endpoint {endpoint.name!r} received a cursor without incremental_cursor"
            )
        literal = _datetime_literal(since, endpoint.name)
        query = f"{query} WHERE {endpoint.incremental_cursor} >= {literal}"
    body["query"] = query
    body.setdefault("contentType", "CSV")
    body.setdefault("columnDelimiter", "COMMA")
    body.setdefault("lineEnding", "LF")
    return body


def _datetime_literal(value: str, endpoint: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise EnterpriseSourceError(
            f"Endpoint {endpoint!r} received an invalid Salesforce timestamp cursor"
        ) from error
    if parsed.tzinfo is None:
        raise EnterpriseSourceError(
            f"Endpoint {endpoint!r} received a timestamp cursor without a timezone"
        )
    utc = parsed.astimezone(UTC)
    milliseconds = utc.microsecond // 1000
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{milliseconds:03d}Z"


def _job_payload(payload: object, endpoint: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint!r} returned invalid job metadata"
        )
    return payload


def _job_id(payload: object, endpoint: str) -> str:
    job_id = _job_payload(payload, endpoint).get("id")
    if not isinstance(job_id, str) or not job_id:
        raise EnterpriseSourceError(
            f"Salesforce Bulk API endpoint {endpoint!r} omitted its query job id"
        )
    return job_id


def _validate_csv_fields(fieldnames: list[str], endpoint: Endpoint) -> None:
    if (
        not fieldnames
        or any(not name for name in fieldnames)
        or len(fieldnames) != len(set(fieldnames))
    ):
        raise EnterpriseSourceError(
            f"Endpoint {endpoint.name!r} returned invalid Salesforce CSV headers"
        )
    declared = {field.name for field in endpoint.raw_schema}
    if unknown := sorted(set(fieldnames) - declared):
        raise EnterpriseSourceError(
            f"Endpoint {endpoint.name!r} returned undeclared Salesforce field {unknown[0]!r}"
        )
    required = {field.name for field in endpoint.raw_schema if field.mode == "REQUIRED"}
    if missing := sorted(required - set(fieldnames)):
        raise EnterpriseSourceError(
            f"Endpoint {endpoint.name!r} omitted required Salesforce field {missing[0]!r}"
        )


def _normalize_csv_row(
    row: Mapping[str | None, str | None], endpoint: Endpoint, *, index: int
) -> dict[str, object | None]:
    fields = {field.name: field for field in endpoint.raw_schema}
    normalized: dict[str, object | None] = {}
    for raw_name, raw_value in row.items():
        assert raw_name is not None and raw_value is not None
        if raw_value == "":
            normalized[raw_name] = None
            continue
        field = fields[raw_name]
        if field.data_type == "BOOL":
            boolean = raw_value.strip().lower()
            if boolean not in {"true", "false"}:
                raise EnterpriseSourceError(
                    f"Endpoint {endpoint.name!r} returned invalid BOOL CSV field "
                    f"{raw_name!r} at row {index}"
                )
            normalized[raw_name] = boolean == "true"
        else:
            normalized[raw_name] = raw_value
    return normalized


def _validate_page_count(headers: Mapping[str, str], actual: int, endpoint: str) -> None:
    raw_count = headers.get("Sforce-NumberOfRecords")
    try:
        expected = int(raw_count) if raw_count is not None else None
    except ValueError as error:
        raise EnterpriseSourceError(
            f"Endpoint {endpoint!r} returned an invalid Salesforce page count"
        ) from error
    if expected is None or expected != actual:
        raise EnterpriseSourceError(
            f"Endpoint {endpoint!r} Salesforce page count did not match its CSV rows"
        )
