# Morning Handoff

## Finished

- Added record-free Salesforce connection testing through the REST limits resource.
- Added exact Account counts through an aggregate SOQL query.
- Added targeted Account lookup by a validated Salesforce `Id` without a Bulk job.
- Proved Dander's installed-plugin inspect/check path discovers and invokes the implementation.
- Prepared the merged capability implementation as `0.2.0rc1` for isolated acceptance.

## Try It

```bash
uv run --with-editable /Users/harrison/Documents/dander pytest
```

## Checks

- Ruff, formatting, strict mypy, dependency audit, and `git diff --check` passed.
- All 34 plugin, source, and cross-repository CLI tests passed.
- The wheel and source distribution built successfully; the wheel installed outside both source
  checkouts with public Dander `0.5.0rc1` and exposed all three read capabilities.

## Decisions

- Bulk API 2.0 remains the only extraction path; small optional reads use Salesforce REST.
- Targeted lookup accepts only the endpoint's declared `Id` and validates its 15/18-character form.
- Provider mutations, deleted-record consumption, and automatic preflight remain out of scope.

## Remaining

- Merge and publish `0.2.0rc1` through protected CI and trusted publishing.
- Run the source-free disposable-org capability and graph-operation acceptance.
- Promote `0.2.0` only if the complete candidate acceptance passes.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `tests/test_dander_cli.py`
