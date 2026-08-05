# Morning Handoff

## Finished

- Published stable Salesforce connector `0.2.0` from its accepted live-tested candidate.
- Kept bounded Bulk API 2.0 extraction and added record-free connection testing, exact Account
  counts, and validated targeted Account lookup.
- Installed the public plugin source-free in the retained Dander project with an exact manifest
  pin and Dander `0.5.x` compatibility.

## Try It

```bash
uv sync --extra dev
uv run pytest
```

## Checks

- Ruff, formatting, strict mypy, and all 34 tests passed.
- Live acceptance covered connection, count, targeted lookup, ingestion, replay, cursor/lease
  safety, staging cleanup, scheduler restoration, and Terraform no-drift.
- Local Markdown links and `git diff --check` passed.

## Decisions

- Bulk API 2.0 remains the extraction path; small optional reads use Salesforce REST.
- Provider mutations, deleted-record consumption, and automatic preflight remain out of scope.

## Remaining

- Continue retained-project soak observation on the public `0.2.0` plugin.

## Review First

- `README.md`
- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
