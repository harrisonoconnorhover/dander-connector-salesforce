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

_ENDPOINT_FIELDS = {
    "accounts": (
        ("Id", "ID", "STRING", True),
        ("Name", "Name", "STRING", True),
        ("OwnerId", "Owner ID", "STRING", False),
        ("ParentId", "Parent account ID", "STRING", False),
        ("Type", "Type", "STRING", False),
        ("Industry", "Industry", "STRING", False),
        ("AnnualRevenue", "Annual revenue", "NUMERIC", False),
        ("NumberOfEmployees", "Number of employees", "INT64", False),
        ("Website", "Website", "STRING", False),
        ("Phone", "Phone", "STRING", False),
        ("BillingCity", "Billing city", "STRING", False),
        ("BillingState", "Billing state", "STRING", False),
        ("BillingPostalCode", "Billing postal code", "STRING", False),
        ("BillingCountry", "Billing country", "STRING", False),
        ("CreatedDate", "Created date", "TIMESTAMP", True),
        ("LastModifiedDate", "Last modified date", "TIMESTAMP", True),
        ("SystemModstamp", "System modification stamp", "TIMESTAMP", True),
        ("IsDeleted", "Deleted", "BOOL", True),
    ),
    "contacts": (
        ("Id", "ID", "STRING", True),
        ("AccountId", "Account ID", "STRING", False),
        ("OwnerId", "Owner ID", "STRING", False),
        ("FirstName", "First name", "STRING", False),
        ("LastName", "Last name", "STRING", True),
        ("Name", "Full name", "STRING", True),
        ("Email", "Email", "STRING", False),
        ("Phone", "Phone", "STRING", False),
        ("Title", "Title", "STRING", False),
        ("Department", "Department", "STRING", False),
        ("MailingCity", "Mailing city", "STRING", False),
        ("MailingState", "Mailing state", "STRING", False),
        ("MailingPostalCode", "Mailing postal code", "STRING", False),
        ("MailingCountry", "Mailing country", "STRING", False),
        ("CreatedDate", "Created date", "TIMESTAMP", True),
        ("LastModifiedDate", "Last modified date", "TIMESTAMP", True),
        ("SystemModstamp", "System modification stamp", "TIMESTAMP", True),
        ("IsDeleted", "Deleted", "BOOL", True),
    ),
    "opportunities": (
        ("Id", "ID", "STRING", True),
        ("AccountId", "Account ID", "STRING", False),
        ("OwnerId", "Owner ID", "STRING", False),
        ("Name", "Name", "STRING", True),
        ("StageName", "Stage", "STRING", True),
        ("Amount", "Amount", "NUMERIC", False),
        ("Probability", "Probability", "NUMERIC", False),
        ("CloseDate", "Close date", "DATE", True),
        ("Type", "Type", "STRING", False),
        ("LeadSource", "Lead source", "STRING", False),
        ("ForecastCategoryName", "Forecast category", "STRING", False),
        ("IsClosed", "Closed", "BOOL", True),
        ("IsWon", "Won", "BOOL", True),
        ("CreatedDate", "Created date", "TIMESTAMP", True),
        ("LastModifiedDate", "Last modified date", "TIMESTAMP", True),
        ("SystemModstamp", "System modification stamp", "TIMESTAMP", True),
        ("IsDeleted", "Deleted", "BOOL", True),
    ),
    "users": (
        ("Id", "ID", "STRING", True),
        ("Name", "Name", "STRING", True),
        ("Alias", "Alias", "STRING", True),
        ("UserType", "User type", "STRING", True),
        ("ProfileId", "Profile ID", "STRING", True),
        ("IsActive", "Active", "BOOL", True),
        ("CreatedDate", "Created date", "TIMESTAMP", True),
        ("LastModifiedDate", "Last modified date", "TIMESTAMP", True),
        ("SystemModstamp", "System modification stamp", "TIMESTAMP", True),
    ),
}

_ENDPOINT_NAMES = {
    "accounts": "Accounts",
    "contacts": "Contacts",
    "opportunities": "Opportunities",
    "users": "Users",
}


def _source_factory(config: SourceConfig, auth: AuthStrategy) -> Source:
    return SalesforceBulk2Source(config, auth)


def create_plugin() -> ConnectorPlugin:
    """Return the API-v1 plugin declaration consumed by Dander."""
    endpoints = tuple(
        ConnectorEndpointDescriptor(
            endpoint_id=endpoint_id,
            display_name=_ENDPOINT_NAMES[endpoint_id],
            fields=tuple(
                ConnectorFieldDescriptor(
                    name=name,
                    display_name=display_name,
                    data_type=data_type,
                    required=required,
                )
                for name, display_name, data_type, required in fields
            ),
        )
        for endpoint_id, fields in _ENDPOINT_FIELDS.items()
    )
    connector = ConnectorDescriptor(
        connector_id="salesforce",
        display_name="Salesforce",
        engine="salesforce_bulk2",
        description=(
            "Read Salesforce Accounts, Contacts, Opportunities, and Users through bounded "
            "Bulk API 2.0 query jobs."
        ),
        endpoints=endpoints,
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
