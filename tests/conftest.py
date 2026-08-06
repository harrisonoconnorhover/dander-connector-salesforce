from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest
from dander.ingestion import load_source_config
from dander.security import AuthStrategy

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dander.ingestion import SourceConfig


class FakeAuth(AuthStrategy):
    def __init__(self) -> None:
        self.requests = 0

    def apply(self, request: httpx.Request) -> httpx.Request:
        self.requests += 1
        request.headers["Authorization"] = "Bearer synthetic"
        return request


@pytest.fixture
def auth() -> FakeAuth:
    return FakeAuth()


@pytest.fixture
def config() -> SourceConfig:
    path = (
        Path(__file__).parents[1]
        / "src"
        / "dander_connector_salesforce"
        / "templates"
        / "salesforce_jwt.example.yaml"
    )
    result = load_source_config(path)
    result.base_url = "https://salesforce.example.test/services/data/v67.0"
    for endpoint in result.endpoints:
        endpoint.pagination = endpoint.pagination.model_copy(update={"page_size": 2})
    return result


class JsonResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        if isinstance(self._payload, httpx.HTTPError):
            raise self._payload

    def json(self) -> object:
        return self._payload


class CsvResponse(JsonResponse):
    def __init__(self, lines: list[str], *, locator: str, records: int) -> None:
        super().__init__(None)
        self._lines = lines
        self.headers = httpx.Headers(
            {"Sforce-Locator": locator, "Sforce-NumberOfRecords": str(records)}
        )
        self.closed = False

    def iter_lines(self) -> Iterator[str]:
        yield from self._lines

    def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self, payloads: list[object]) -> None:
        self._payloads = iter(payloads)
        self.requests: list[httpx.Request] = []
        self.streams: list[bool] = []

    def send(self, request: httpx.Request, *, stream: bool = False) -> JsonResponse:
        self.requests.append(request)
        self.streams.append(stream)
        payload = next(self._payloads)
        return payload if isinstance(payload, JsonResponse) else JsonResponse(payload)
