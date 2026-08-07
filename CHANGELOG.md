# Changelog

## Unreleased

## 0.3.1rc1 — 2026-08-07 (alpha)

- Extend package compatibility through Dander `0.7.x` without changing connector runtime
  behavior or plugin API v1.

## 0.3.0 — 2026-08-07 (alpha)

- Promote the accepted four-endpoint Salesforce candidate without changing connector runtime
  behavior.
- Record successful source-free hosted ingestion, governed transforms/tests, inclusive replay,
  soft-deletion tombstones, cleanup, and ServiceNow compatibility on Dander `0.6.0rc2`.

## 0.3.0rc1 — 2026-08-06 (alpha)

- Expand the bounded Bulk API 2.0 connector to independently watermarked Accounts, Contacts,
  Opportunities, and Users endpoints with declared schemas.
- Preserve soft-deleted Account, Contact, and Opportunity tombstones through `queryAll`; represent
  User deactivation with `IsActive`.
- Add realistic multi-page fixtures and coverage for inclusive replay, sparse and empty responses,
  typed booleans, failure handling, and query-job cleanup.
- Add explicit deleted-record lookup plus single-attempt create, update, delete, and metadata-verified
  External ID upsert capabilities for operator-invoked write-back.
- Allow Dander `0.6.x` while retaining plugin API v1 and the `salesforce_bulk2` engine.

## 0.2.0 — 2026-08-05 (alpha)

- Promote the accepted read-capability candidate without changing connector runtime behavior.
- Provide connection testing, exact counts, and validated single-record lookup alongside bounded
  Bulk API 2.0 extraction.

## 0.2.0rc2 — 2026-08-05 (alpha)

- Generate Salesforce's valid unaliased `COUNT()` SOQL form after isolated live acceptance
  exposed a rejected aggregate query in `0.2.0rc1`.

## 0.2.0rc1 — 2026-08-05 (alpha)

- Add structural targeted-lookup, exact-count, and record-free connection-test capabilities.
- Reuse Salesforce REST query/queryAll and limits resources while retaining Bulk API 2.0 for
  bounded extraction.
- Require public Dander `0.5.0rc1` or newer for its capability contract and CLI.

## 0.1.1

- Declare compatibility with Dander `0.5.x`; connector runtime behavior is unchanged from
  `0.1.0`.

## 0.1.0

- Promote the accepted Salesforce connector candidate without changing packaged runtime behavior.
- Require stable Dander `0.4.x` while preserving the plugin API-v1 and `salesforce_bulk2` contract.

## 0.1.0rc1

- Add the first-party `salesforce_bulk2` Dander connector plugin.
- Stream bounded Bulk API 2.0 CSV pages with opaque locator pagination.
- Apply `SystemModstamp` replay cursors as server-side SOQL filters.
- Publish a non-secret connector descriptor for Dander and Druff discovery.
