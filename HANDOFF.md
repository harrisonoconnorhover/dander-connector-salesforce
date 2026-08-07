# Morning Handoff

## Finished

- Merged Salesforce `get_deleted` for Accounts, Contacts, and Opportunities through PR `#10`.
- Merged single-attempt Salesforce create through PR `#15`.
- Merged one-record update through PR `#16`.
- Added repeat-safe delete outcomes as the next focused provider slice.
- Use public `dander-platform>=0.6.0rc1,<0.7` throughout CI and package metadata.

## Try It

Run `uv sync --extra dev && uv run pytest`. Provider mutations remain explicit CLI operations;
no live Salesforce tenant is needed for the simulator-backed suite.

## Checks

- Published-contract plugin suite passed: `74 passed`.
- Ruff lint/format and strict mypy passed.
- PRs `#10`, `#15`, and `#16` passed all protected CI checks.
- Empty, malformed, permission-shaped, identity, and ambiguous-write paths are covered.
- No live Salesforce request or mutation occurred.

## Decisions

- Never retry an ambiguous create; mutations currently use one authenticated attempt.
- Return only business identities or the closed delete outcome, not provider record bodies.
- Do not claim upsert until a Salesforce external-ID field is explicitly declared.

## Remaining

- Merge the delete slice through protected checks.
- Merge the separately reviewed explicit External-ID upsert slice.
- Publish the approved connector candidate after the complete stack is green.
- Run the approved disposable-org acceptance proof before any retained-project change.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `.github/workflows/ci.yml`
