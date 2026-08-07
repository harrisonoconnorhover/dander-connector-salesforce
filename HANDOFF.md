# Morning Handoff

## Finished

- Prepared stable `dander-connector-salesforce==0.3.0` from the accepted `0.3.0rc1` runtime.
- Kept the four Salesforce endpoints, plugin API v1, and `salesforce_bulk2` behavior unchanged.
- Recorded the isolated hosted proof against public `dander-platform==0.6.0rc2`.
- Left the retained project and Salesforce source code unchanged.

## Try It

Install the built package outside the checkout, pin `0.3.0` in `dander.yaml`, and run
`dander connector check salesforce` from a generated project with secret references configured.

## Checks

- Live Accounts, Contacts, Opportunities, and Users ingestion succeeded with forced pagination.
- Inclusive replay remained duplicate-free and cursors did not regress.
- Create/update ingestion and soft-delete tombstones were verified through governed models/tests.
- ServiceNow passed on the same source-free Dander image; schedules remained paused.
- Ruff, formatting, strict typing, all 93 tests, package build, and external wheel discovery passed.

## Decisions

- Stable `0.3.0` promotes the accepted candidate without a connector runtime change.
- Write-back remains opt-in, explicitly confirmed, and experimental.
- Salesforce hard-delete recovery remains a documented limitation.

## Remaining

- Run the full protected checks, merge, tag, and publish `0.3.0`.
- Pin the stable connector in Dander `0.6.0`.
- Upgrade the retained project only through its separately reviewed plan.

## Review First

- `CHANGELOG.md`
- `pyproject.toml`
- `README.md`
