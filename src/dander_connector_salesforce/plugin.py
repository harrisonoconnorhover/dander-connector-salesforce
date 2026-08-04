"""Dander plugin entry point and presentation-safe Salesforce descriptor."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dander.plugins import (
    PLUGIN_API_VERSION,
    ConnectorDescriptor,
    ConnectorEndpointDescriptor,
    ConnectorFieldDescriptor,
    ConnectorPlugin,
)

from dander_connector_salesforce.source import SalesforceBulk2Source

if TYPE_CHECKING:
    from dander.ingestion import Source, SourceConfig
    from dander.security import AuthStrategy

_ACCOUNT_FIELDS = (
    ("Id", "ID", "STRING", True),
    ("Name", "Name", "STRING", True),
    ("Type", "Type", "STRING", False),
    ("Industry", "Industry", "STRING", False),
    ("AnnualRevenue", "Annual revenue", "NUMERIC", False),
    ("NumberOfEmployees", "Number of employees", "INT64", False),
    ("BillingCity", "Billing city", "STRING", False),
    ("BillingState", "Billing state", "STRING", False),
    ("BillingCountry", "Billing country", "STRING", False),
    ("CreatedDate", "Created date", "TIMESTAMP", True),
    ("LastModifiedDate", "Last modified date", "TIMESTAMP", True),
    ("SystemModstamp", "System modification stamp", "TIMESTAMP", True),
    ("IsDeleted", "Deleted", "BOOL", True),
)


def _source_factory(config: SourceConfig, auth: AuthStrategy) -> Source:
    return SalesforceBulk2Source(config, auth)


def create_plugin() -> ConnectorPlugin:
    """Return the API-v1 plugin declaration consumed by Dander."""
    endpoint = ConnectorEndpointDescriptor(
        endpoint_id="accounts",
        display_name="Accounts",
        fields=tuple(
            ConnectorFieldDescriptor(
                name=name,
                display_name=display_name,
                data_type=data_type,
                required=required,
            )
            for name, display_name, data_type, required in _ACCOUNT_FIELDS
        ),
    )
    connector = ConnectorDescriptor(
        connector_id="salesforce",
        display_name="Salesforce",
        engine="salesforce_bulk2",
        description="Read Salesforce Accounts through bounded Bulk API 2.0 query jobs.",
        endpoints=(endpoint,),
    )
    return ConnectorPlugin(
        plugin_id="salesforce",
        api_version=PLUGIN_API_VERSION,
        engine="salesforce_bulk2",
        display_name="Salesforce",
        description="First-party Salesforce Bulk API 2.0 connector.",
        source_factory=_source_factory,
        connectors=(connector,),
    )
