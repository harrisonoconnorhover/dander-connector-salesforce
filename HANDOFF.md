# Morning Handoff

## Finished

- Added record-free Salesforce connection testing through the REST limits resource.
- Added exact Account counts through an aggregate SOQL query.
- Added targeted Account lookup by a validated Salesforce `Id` without a Bulk job.
- Proved the accepted candidate source-free against a disposable Salesforce org and GCP project.
- Prepared stable `0.2.0` from the accepted `0.2.0rc2` runtime.

## Try It

```bash
uv run --with-editable /Users/harrison/Documents/dander pytest
```

## Checks

- Ruff, formatting, strict mypy, dependency audit, and `git diff --check` passed.
- All 34 plugin, source, and cross-repository CLI tests passed.
- The wheel and source distribution built; a source-free install discovered the API-v1 plugin.
- Live acceptance passed connection check, exact count, single-record lookup, ingestion, replay,
  cursor and lease checks, staging cleanup, scheduler restoration, and Terraform no-drift.

## Decisions

- Bulk API 2.0 remains the only extraction path; small optional reads use Salesforce REST.
- Targeted lookup accepts only the endpoint's declared `Id` and validates its 15/18-character form.
- Provider mutations, deleted-record consumption, and automatic preflight remain out of scope.

## Remaining

- Merge the stable release PR through protected CI.
- Tag and publish `0.2.0` through trusted publishing.
- Verify an exact public source-free installation.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `tests/test_dander_cli.py`
