# Morning Handoff

## Finished

- Prepared `0.3.0rc1` with independently watermarked Accounts, Contacts, Opportunities, and Users.
- Added declared CRM schemas, `queryAll` tombstones, User active state, and personal-data guidance.
- Added realistic fixtures and forced two-page streaming coverage for all four endpoints.
- Preserved plugin API v1, `salesforce_bulk2`, OAuth2 JWT, and the built-in Dander fallback.
- Replaced the temporary Git commit dependency with public `dander-platform>=0.6.0rc1,<0.7`.

## Try It

```bash
uv sync --extra dev
uv run pytest
```

Copy `src/dander_connector_salesforce/templates/salesforce_jwt.example.yaml` into a Dander project's
`connectors/salesforce.yaml`; keep the new `salesforce_crm` pipeline paused for initial proof.

## Checks

- All connector tests passed: `56 passed`.
- Ruff, formatting, and strict mypy passed.
- The same tests passed against the published Dander candidate dependency.
- Wheel/sdist build and external wheel installation/discovery passed.
- `git diff --check` passed.

## Decisions

- Use `queryAll` only for deletable CRM objects; use `query` plus `IsActive` for Users.
- Keep inclusive cursors and let Dander SCD1 make boundary replay idempotent.
- Keep Contact Email/Phone enabled but explicitly identify them as personal data.

## Remaining

- Protected CI and review must pass before merge.
- Merge the provider capability PRs before publishing the approved candidate.
- Dander's governed Salesforce models and project wiring are the next sequential PR.
- Live multi-object acceptance remains pending.

## Review First

- `src/dander_connector_salesforce/templates/salesforce_jwt.example.yaml`
- `src/dander_connector_salesforce/plugin.py`
- `tests/test_source.py`
