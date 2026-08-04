from __future__ import annotations

import importlib.metadata
from typing import TYPE_CHECKING

from dander.plugins import PLUGIN_API_VERSION

from dander_connector_salesforce import SalesforceBulk2Source, create_plugin

if TYPE_CHECKING:
    from dander.ingestion import SourceConfig


def test_plugin_contract_and_descriptor_are_api_v1_compatible() -> None:
    plugin = create_plugin()

    assert plugin.plugin_id == "salesforce"
    assert plugin.api_version == PLUGIN_API_VERSION
    assert plugin.engine == "salesforce_bulk2"
    assert plugin.connectors[0].connector_id == "salesforce"
    assert plugin.connectors[0].endpoints[0].endpoint_id == "accounts"
    assert {field.name for field in plugin.connectors[0].endpoints[0].fields} >= {
        "Id",
        "SystemModstamp",
        "IsDeleted",
    }


def test_distribution_registers_exact_dander_entry_point() -> None:
    distribution = importlib.metadata.distribution("dander-connector-salesforce")
    entry_points = [
        point
        for point in distribution.entry_points
        if point.group == "dander.connectors" and point.name == "salesforce"
    ]

    assert len(entry_points) == 1
    assert entry_points[0].load() is create_plugin


def test_factory_returns_plugin_owned_source(config: SourceConfig, auth: object) -> None:
    plugin = create_plugin()
    source = plugin.source_factory(config, auth)  # type: ignore[arg-type]

    assert isinstance(source, SalesforceBulk2Source)
    assert source.__class__.__module__ == "dander_connector_salesforce.source"
