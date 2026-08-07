# Morning Handoff

## Finished

- Prepared stable `dander-connector-salesforce==0.3.1` from the accepted candidate.
- Kept all four endpoints, plugin API v1, and connector runtime behavior unchanged.
- Updated only release metadata, the stable example pin, changelog, lockfile, and handoff.

## Try It

Install the built package outside the checkout and pin `0.3.1` in `dander.yaml`.

## Checks

- Isolated Dander `0.7.0rc2` live acceptance passed across all four Salesforce endpoints.
- All 93 tests, Ruff lint/format, strict mypy, and package build passed locally.

## Decisions

- Promote the accepted candidate without a functional connector change.
- Preserve plugin API v1 and the Dander `>=0.4,<0.8` compatibility range.

## Remaining

- Merge through protected main, tag `v0.3.1`, and publish through the protected environment.
- Verify the public package resolves with stable Dander `0.7.0` after its publication.

## Review First

- `CHANGELOG.md`
- `pyproject.toml`
- `README.md`
