# Morning Handoff

## Finished

- Created the public first-party Salesforce connector repository.
- Added the `salesforce_bulk2` Dander plugin entry point and non-secret descriptor.
- Moved the bounded Bulk API 2.0 implementation into the plugin package.
- Added the connector template, package metadata, documentation, and focused tests.
- Added Linux CI, dependency/secret scans, outside-checkout packaging, and protected publication workflow.

## Try It

Declare `dander-connector-salesforce==0.1.0rc1` under `plugins.salesforce` in `dander.yaml`, then run `dander plugins install`.

## Checks

- Ruff lint and format passed; strict mypy passed.
- All 14 focused tests passed.
- Dependency audit reported no known vulnerabilities.
- Wheel and sdist built; both contain the adapter and connector template.
- Outside-checkout wheel installation and entry-point discovery passed.
- Protected PR checks passed for Python, distribution, and secret validation.

## Decisions

- Dander core retains its deprecated built-in Salesforce fallback through 0.4.
- The plugin uses Dander's existing OAuth2 JWT strategy and writer runtime.
- The package candidate is prepared but not published without a separate approval.

## Remaining

- Configure the new package's PyPI trusted publisher only after publication approval.
- Publish candidates only after explicit approval.
- Run the isolated live proof only against published candidates.

## Review First

- `src/dander_connector_salesforce/source.py`
- `src/dander_connector_salesforce/plugin.py`
- `tests/test_source.py`
