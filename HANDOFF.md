# Morning Handoff

## Finished

- Added Salesforce `get_deleted` for Accounts, Contacts, and Opportunities in draft PR `#10`.
- Added single-attempt Salesforce create in stacked draft PR `#11`.
- Added one-record update in stacked draft PR `#12`.
- Added repeat-safe delete outcomes in stacked draft PR `#13`.
- Added opt-in, metadata-verified External ID upsert and pinned CI to the exact unreleased Dander
  contract without changing package metadata.

## Try It

Review the stack in order: `#10`, `#11`, `#12`, `#13`, then the upsert PR. Tests run with the exact
Dander contract installed by CI; no live Salesforce tenant is needed.

## Checks

- Exact Dander write-back contract plugin suite passed: `93 passed`.
- Ruff lint/format and strict mypy passed.
- Dander and plugin wheels built and installed together in a clean environment outside both repos.
- Stateful tests cover create-versus-update upsert behavior and reserved-character path encoding.
- Empty, malformed, permission-shaped, identity, metadata, and ambiguous-write paths are covered.
- No live Salesforce request or mutation occurred.

## Decisions

- Never retry an ambiguous create; mutations currently use one authenticated attempt.
- Return only business identities or the closed delete outcome, not provider record bodies.
- Enable upsert only for an explicitly declared, provider-verified unique External ID field.

## Remaining

- Keep all provider PRs draft until Dander's capability contract is merged and published.
- Run one narrowly approved disposable-org mutation proof after review.
- Do not publish, deploy, or alter the retained project from this stack.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `.github/workflows/ci.yml`
