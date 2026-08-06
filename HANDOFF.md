# Morning Handoff

## Finished

- Added Salesforce `get_deleted` for Accounts, Contacts, and Opportunities in draft PR `#10`.
- Added single-attempt Salesforce create in stacked draft PR `#11`.
- Added one-record update in stacked draft PR `#12`.
- Added repeat-safe delete outcomes in stacked draft PR `#13`.
- Pinned capability CI to the exact unreleased Dander contract commits without changing package metadata.

## Try It

Review the stack in order: `#10`, `#11`, `#12`, then `#13`. Tests run with the exact Dander
contract installed by CI; no live Salesforce tenant is needed.

## Checks

- Exact-contract plugin suite passed: `73 passed`.
- Ruff lint/format and strict mypy passed.
- PRs `#10` through `#12` passed all protected CI checks; `#13` CI was started.
- Empty, malformed, permission-shaped, identity, and ambiguous-write paths are covered.
- No live Salesforce request or mutation occurred.

## Decisions

- Never retry an ambiguous create; mutations currently use one authenticated attempt.
- Return only business identities or the closed delete outcome, not provider record bodies.
- Do not claim upsert until a Salesforce external-ID field is explicitly declared.

## Remaining

- Keep all provider PRs draft until Dander's capability contract is merged and published.
- Decide how an endpoint declares its Salesforce external-ID field before implementing upsert.
- Run one narrowly approved disposable-org mutation proof after review.
- Do not publish, deploy, or alter the retained project from this stack.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `.github/workflows/ci.yml`
