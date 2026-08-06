"""Installed-plugin integration with Dander's capability CLI."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dander.cli.main import app
from dander.ingestion import ConnectionStatus
from typer.testing import CliRunner

from dander_connector_salesforce.source import SalesforceBulk2Source

if TYPE_CHECKING:
    import pytest

_TEMPLATE = (
    Path(__file__).parents[1]
    / "src"
    / "dander_connector_salesforce"
    / "templates"
    / "salesforce_jwt.example.yaml"
)


def _project(tmp_path: Path) -> tuple[Path, Path]:
    connectors = tmp_path / "connectors"
    connectors.mkdir()
    (connectors / "salesforce.yaml").write_text(
        _TEMPLATE.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    manifest = tmp_path / "dander.yaml"
    manifest.write_text(
        """
version: 1
plugins:
  salesforce:
    distribution: dander-connector-salesforce
    version: 0.3.0rc1
pipelines:
  salesforce_accounts:
    source: salesforce
    models: []
    build_models: false
""".strip(),
        encoding="utf-8",
    )
    return manifest, connectors


def test_inspect_discovers_capabilities_from_installed_plugin(tmp_path: Path) -> None:
    manifest, connectors = _project(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "connector",
            "inspect",
            "salesforce_accounts",
            "--config",
            str(manifest),
            "--connectors-dir",
            str(connectors),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "salesforce" in result.output
    assert "salesforce_bulk2" in result.output
    assert result.output.count("yes") == 6


def test_check_invokes_plugin_connection_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, connectors = _project(tmp_path)
    probes = 0

    def test_connection(_source: SalesforceBulk2Source) -> ConnectionStatus:
        nonlocal probes
        probes += 1
        return ConnectionStatus(ok=True)

    monkeypatch.setattr(SalesforceBulk2Source, "test_connection", test_connection)

    result = CliRunner().invoke(
        app,
        [
            "connector",
            "check",
            "salesforce_accounts",
            "--config",
            str(manifest),
            "--connectors-dir",
            str(connectors),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "connection check passed" in result.output
    assert probes == 1
