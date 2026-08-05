# Morning Handoff

## Finished

- Added record-free Salesforce connection testing through the REST limits resource.
- Added exact Account counts through an aggregate SOQL query.
- Added targeted Account lookup by a validated Salesforce `Id` without a Bulk job.
- Proved Dander's installed-plugin inspect/check path discovers and invokes the implementation.

## Try It

```bash
uv run --with-editable /Users/harrison/Documents/dander pytest
```

## Checks

- Ruff, format, and strict mypy passed against public Dander `0.5.0rc1`.
- All 34 plugin, source, and cross-repository CLI tests passed.
- Built artifacts installed with public Dander outside both source checkouts; capability inspection
  reported targeted lookup, exact count, and connection checking.

## Decisions

- Bulk API 2.0 remains the only extraction path; small optional reads use Salesforce REST.
- Targeted lookup accepts only the endpoint's declared `Id` and validates its 15/18-character form.
- Provider mutations, deleted-record consumption, and automatic preflight remain out of scope.

## Remaining

- Open and merge the focused Salesforce capability PR through protected CI.
- Perform one narrow disposable-org capability check before final promotion.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `tests/test_dander_cli.py`
