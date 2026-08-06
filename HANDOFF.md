# Morning Handoff

## Finished

- Merged Salesforce `get_deleted` through PR `#10` and create through PR `#15`.
- Merged one-record update through PR `#16` and repeat-safe delete through PR `#17`.
- Added opt-in, metadata-verified External ID upsert as the final focused provider slice.
- Use public `dander-platform>=0.6.0rc1,<0.7` throughout CI and package metadata.
- Kept all provider writes explicit and single-attempt.

## Try It

Run `uv sync --extra dev && uv run pytest`. Upsert requires an endpoint-declared field that
Salesforce metadata marks as both External ID and Unique.

## Checks

- Published-contract plugin suite passed: `93 passed`.
- Ruff lint/format and strict mypy passed.
- The plugin wheel installed with public Dander in a clean environment outside both repositories.
- Stateful tests cover create-versus-update upsert behavior and reserved-character path encoding.
- Empty, malformed, permission-shaped, identity, metadata, and ambiguous-write paths are covered.
- No live Salesforce request or mutation occurred.

## Decisions

- Never retry an ambiguous create; mutations currently use one authenticated attempt.
- Return only business identities or the closed delete outcome, not provider record bodies.
- Enable upsert only for an explicitly declared, provider-verified unique External ID field.

## Remaining

- Merge the upsert slice through protected checks.
- Publish the approved connector candidate after the complete stack is green.
- Run the approved disposable-org mutation proof before any retained-project change.

## Review First

- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
- `src/dander_connector_salesforce/plugin.py`
