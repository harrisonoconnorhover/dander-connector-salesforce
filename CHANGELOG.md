# Changelog

## Unreleased

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
