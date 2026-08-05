# Morning Handoff

## Finished

- Prepared Salesforce plugin `0.1.0` from the accepted `0.1.0rc1` candidate.
- Raised the Dander dependency floor from the release candidate to stable `0.4.0`.
- Kept all packaged runtime source and connector behavior unchanged.
- Retained the exact plugin ID, engine, entry point, template, and API-v1 descriptor.

## Try It

Declare `dander-connector-salesforce==0.1.0` under `plugins.salesforce` in `dander.yaml`, then run `dander plugins install`.

## Checks

- Ruff lint and format passed; strict mypy passed.
- All 14 focused tests passed; dependency audit found no known vulnerabilities.
- Wheel and sdist built successfully with no runtime-source changes from the candidate.
- Outside-checkout installation discovered the plugin and loaded its packaged template.
- Stable Dander `0.4.0` and plugin `0.1.0` resolved together successfully.

## Decisions

- Dander core retains its deprecated built-in Salesforce fallback through 0.4.
- The plugin uses Dander's existing OAuth2 JWT strategy and writer runtime.
- Final publication remains a separate explicit approval gate.

## Remaining

- Merge the version-only PR through protected `main`.
- Obtain explicit approval before tagging and publishing `0.1.0`.
- Verify the public package after protected publication.

## Review First

- `src/dander_connector_salesforce/source.py`
- `src/dander_connector_salesforce/plugin.py`
- `tests/test_source.py`
