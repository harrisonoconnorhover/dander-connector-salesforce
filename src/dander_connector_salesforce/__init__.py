"""Salesforce Bulk API 2.0 connector plugin for Dander."""

from dander_connector_salesforce.plugin import create_plugin
from dander_connector_salesforce.source import SalesforceBulk2Source

__all__ = ["SalesforceBulk2Source", "create_plugin"]
