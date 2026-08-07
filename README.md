# Dander Salesforce Connector

First-party Salesforce Bulk API 2.0 connector plugin for
[Dander](https://github.com/harrisonoconnorhover/dander).

> **Alpha:** pin both Dander and this plugin exactly. Scheduled ingestion is read-only. Explicit
> connector write commands are experimental and mutate Salesforce. Contact Email and Phone are
> enabled by default and are personal data; deploy them only into an approved project.

## What it reads

The four endpoints stream bounded CSV result pages and commit separate `SystemModstamp` watermarks:

| Endpoint | Raw relation | Salesforce operation | Deletion state |
| --- | --- | --- | --- |
| `accounts` | `raw.salesforce_accounts` | `queryAll` | `IsDeleted` |
| `contacts` | `raw.salesforce_contacts` | `queryAll` | `IsDeleted` |
| `opportunities` | `raw.salesforce_opportunities` | `queryAll` | `IsDeleted` |
| `users` | `raw.salesforce_users` | `query` | `IsActive` |

Inclusive `SystemModstamp >= cursor` filters deliberately reread the cursor boundary. Dander's
SCD1 writer makes that replay duplicate-free. Soft-deleted CRM records remain as tombstones rather
than being physically deleted from BigQuery, and inactive owners remain available for historical
joins. Salesforce records that have already been hard-deleted or purged cannot be recovered by
this connector.

Salesforce's deleted-record feed retains at most 15 days. Dander rejects a wider or non-forward
window instead of silently presenting it as complete.

## Install and configure

Declare the exact candidate version in `dander.yaml`:

```yaml
version: 1
plugins:
  salesforce:
    distribution: dander-connector-salesforce
    version: 0.3.0rc1
pipelines:
  salesforce_crm:
    source: salesforce
    models: []
    build_models: false
    schedule: "0 7 * * *"
    time_zone: America/New_York
    paused: true
    secrets:
      SALESFORCE_EXTERNAL_CLIENT_APP_ID: salesforce-client-id
      SALESFORCE_EXTERNAL_CLIENT_APP_PRIVATE_KEY: salesforce-private-key
```

Existing installations may keep their current pipeline ID to avoid replacing Cloud Run and
Scheduler resources. Install the manifest's exact package pin:

```console
dander plugins install
```

Copy [`salesforce_jwt.example.yaml`](src/dander_connector_salesforce/templates/salesforce_jwt.example.yaml)
into `connectors/salesforce.yaml`. Replace only the public org URL, OAuth subject, and login-domain
settings. Store the External Client App ID and RSA private key in Dander's secret store; never put
credential values in YAML.

The Salesforce integration user needs API access plus read access to each selected object and
field. Custom fields are opt-in: add each field to both the endpoint's SOQL `SELECT` and its
`raw_schema`. This explicit declaration prevents undeclared source drift from entering BigQuery.

## Runtime contract

- Engine: `salesforce_bulk2`
- Authentication: Dander core's cloud-neutral `oauth2_jwt` strategy
- API: Salesforce Bulk API 2.0 Query
- Publication: Dander's idempotent SCD1 writer
- Memory: CSV pages are streamed and bounded by Salesforce `maxRecords`
- Replay: inclusive server-side `SystemModstamp` filters
- Optional reads: exact aggregate count and one-record lookup by validated Salesforce `Id`
- Optional deleted feed: Accounts, Contacts, and Opportunities within Salesforce's 15-day window
- Explicit writes: create, update, and delete by validated Salesforce `Id`
- Connection check: authenticated REST `/limits` probe that returns no business records

With Dander `0.5.0` or newer, inspect and check an installed pipeline without ingestion:

```console
dander connector inspect salesforce_crm
dander connector check salesforce_crm
```

Dander retains its deprecated built-in Salesforce fallback for compatibility. An explicitly pinned
plugin takes precedence.

## Experimental write-back

Write-back is never invoked by `dander run`. It requires the separate connector command, local JSON
input files, and an explicit `--confirm-write` acknowledgement:

```console
dander connector write salesforce_crm accounts create --record new-account.json --confirm-write
dander connector write salesforce_crm accounts update --identity account-id.json \
  --changes account-changes.json --confirm-write
dander connector write salesforce_crm accounts delete --identity account-id.json --confirm-write
```

Upsert is disabled by default. To enable it for a custom endpoint, declare one Salesforce External
ID field as the endpoint's sole primary key, include it in both the SOQL selection and `raw_schema`,
and name it in `request_body`:

```yaml
primary_key: [Dander_External_ID__c]
request_body:
  operation: queryAll
  query: SELECT Dander_External_ID__c, Name FROM Account
  upsert_external_id_field: Dander_External_ID__c
raw_schema:
  - {name: Dander_External_ID__c, type: STRING, mode: REQUIRED}
  - {name: Name, type: STRING, mode: REQUIRED}
```

Before each upsert, the connector checks Salesforce metadata and refuses fields that are not marked
both External ID and Unique. External-ID values are encoded as one URL path segment. The local
control field is not sent in Bulk query job bodies.

These write capabilities require public `dander-platform>=0.6.0rc1,<0.7`. Provider writes remain
explicit CLI operations and are not invoked by normal scheduled ingestion.

## Development

```console
uv sync --extra dev
uv run ruff check .
uv run mypy src tests
uv run pytest
```

Apache-2.0 licensed.
