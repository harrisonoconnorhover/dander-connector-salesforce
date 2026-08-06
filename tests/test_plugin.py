from __future__ import annotations

import importlib.metadata
from typing import TYPE_CHECKING

from dander.ingestion import ConnectorOperation, SourceCapabilities
from dander.plugins import PLUGIN_API_VERSION

from dander_connector_salesforce import SalesforceBulk2Source, create_plugin
from dander_connector_salesforce.source import SalesforceExternalIdUpsertSource

if TYPE_CHECKING:
    from dander.ingestion import SourceConfig


def test_plugin_contract_and_descriptor_are_api_v1_compatible() -> None:
    plugin = create_plugin()

    assert plugin.plugin_id == "salesforce"
    assert plugin.api_version == PLUGIN_API_VERSION
    assert plugin.engine == "salesforce_bulk2"
    assert plugin.connectors[0].connector_id == "salesforce"
    endpoints = {endpoint.endpoint_id: endpoint for endpoint in plugin.connectors[0].endpoints}
    assert set(endpoints) == {"accounts", "contacts", "opportunities", "users"}
    assert {field.name for field in endpoints["accounts"].fields} >= {
        "Id",
        "OwnerId",
        "SystemModstamp",
        "IsDeleted",
    }
    assert {field.name for field in endpoints["contacts"].fields} >= {"Email", "Phone"}
    assert {field.name for field in endpoints["opportunities"].fields} >= {
        "Amount",
        "StageName",
        "IsWon",
    }
    assert {field.name for field in endpoints["users"].fields} >= {"ProfileId", "IsActive"}


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
    assert not isinstance(source, SalesforceExternalIdUpsertSource)
    assert not SourceCapabilities(source).supports(ConnectorOperation.UPSERT)
    assert source.__class__.__module__ == "dander_connector_salesforce.source"


def test_factory_advertises_upsert_only_for_explicit_endpoint_configuration(
    config: SourceConfig,
    auth: object,
) -> None:
    account = config.endpoints[0]
    request_body = dict(account.request_body)
    request_body["upsert_external_id_field"] = "Dander_External_ID__c"
    configured = config.model_copy(
        update={
            "endpoints": [
                account.model_copy(update={"request_body": request_body}),
                *config.endpoints[1:],
            ]
        }
    )

    source = create_plugin().source_factory(configured, auth)  # type: ignore[arg-type]

    assert isinstance(source, SalesforceExternalIdUpsertSource)
    assert SourceCapabilities(source).supports(ConnectorOperation.UPSERT)
