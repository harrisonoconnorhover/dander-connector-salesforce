# Dander Salesforce Connector

First-party Salesforce Bulk API 2.0 connector plugin for
[Dander](https://github.com/harrisonoconnorhover/dander).

> **Alpha:** pin both Dander and this plugin exactly. The plugin is read-only and currently
> supports one bounded, server-filtered Accounts `queryAll` pipeline.

## Install

Declare the exact plugin version in `dander.yaml`:

```yaml
plugins:
  salesforce:
    distribution: dander-connector-salesforce
    version: 0.1.1
```

Then install exactly what the manifest declares:

```console
dander plugins install
```

Copy [`salesforce_jwt.example.yaml`](src/dander_connector_salesforce/templates/salesforce_jwt.example.yaml)
into the project's `connectors/` directory, replace the public org settings, and keep the
External Client App ID and RSA private key in Dander's configured secret store.

## Runtime contract

- Engine: `salesforce_bulk2`
- Authentication: Dander core's `oauth2_jwt` strategy
- API: Salesforce Bulk API 2.0 Query
- Publication: Dander's existing SCD1 writer
- Cursor: `SystemModstamp`, applied as a server-side SOQL filter on replay
- Memory: CSV result pages are streamed and bounded by `maxRecords`
- Optional reads: exact aggregate count and one Account lookup by validated Salesforce `Id`
- Connection check: authenticated REST `/limits` probe that returns no business records

With a capability-enabled Dander build, inspect and check the installed connector without running
ingestion:

```console
dander connector inspect salesforce
dander connector check salesforce
```

The built-in Salesforce adapter in Dander 0.4 remains a deprecated fallback. When this plugin is
explicitly pinned, the plugin implementation takes precedence.

## Development

```console
uv sync --extra dev
uv run ruff check .
uv run mypy src tests
uv run pytest
```

Apache-2.0 licensed.
