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

- Ruff and strict mypy passed against local merged Dander `main`.
- All 34 plugin, source, and cross-repository CLI tests passed against local Dander `main`.
- Standard tests intentionally fail at import because public Dander `0.4.0` lacks the new
  capability types; this confirms the candidate-publication gate.

## Decisions

- Bulk API 2.0 remains the only extraction path; small optional reads use Salesforce REST.
- Targeted lookup accepts only the endpoint's declared `Id` and validates its 15/18-character form.
- Provider mutations, deleted-record consumption, and automatic preflight remain out of scope.

## Remaining

- Publish a Dander candidate containing protected PRs #71 and #72 after explicit approval.
- Raise the plugin dependency floor and lockfile to that candidate.
- Rerun ordinary CI, build/install the plugin candidate, and open its focused PR.
- Perform one narrow disposable-org capability check before final promotion.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `tests/test_dander_cli.py`
