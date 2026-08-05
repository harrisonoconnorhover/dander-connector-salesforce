# Morning Handoff

## Finished

- Added record-free Salesforce connection testing through the REST limits resource.
- Added exact Account counts through an aggregate SOQL query.
- Added targeted Account lookup by a validated Salesforce `Id` without a Bulk job.
- Proved Dander's installed-plugin inspect/check path discovers and invokes the implementation.
- Corrected the count query after `0.2.0rc1` live acceptance exposed Salesforce's rejection of an
  alias on `COUNT()`; prepared replacement candidate `0.2.0rc2`.

## Try It

```bash
uv run --with-editable /Users/harrison/Documents/dander pytest
```

## Checks

- Ruff, formatting, strict mypy, dependency audit, and `git diff --check` passed.
- All 34 plugin, source, and cross-repository CLI tests passed.
- The `0.2.0rc2` wheel and source distribution built; a source-free install with public Dander
  `0.5.0rc2` discovered the API-v1 Salesforce plugin.

## Decisions

- Bulk API 2.0 remains the only extraction path; small optional reads use Salesforce REST.
- Targeted lookup accepts only the endpoint's declared `Id` and validates its 15/18-character form.
- Provider mutations, deleted-record consumption, and automatic preflight remain out of scope.

## Remaining

- Merge and publish `0.2.0rc2` through protected CI and trusted publishing.
- Run the source-free disposable-org capability and graph-operation acceptance.
- Promote `0.2.0` only if the complete candidate acceptance passes.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `tests/test_dander_cli.py`
