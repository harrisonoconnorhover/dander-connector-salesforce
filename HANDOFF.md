# Morning Handoff

## Finished

- Published `dander-connector-salesforce==0.3.0rc1` against public `dander-platform==0.6.0rc1`.
- Deployed their source-free shared image to the isolated proof project with both schedules paused.
- Proved live create, update/read-back, unique External ID upsert, delete, repeat-delete, and `get_deleted` against the disposable Salesforce org.
- Completed a hosted `queryAll` graph run that retained both deleted proof Accounts as tombstones.
- Left the retained project unchanged.

## Try It

Install the exact candidates outside both repositories, then run `dander connector check salesforce`
from a generated project with Salesforce secret references configured.

## Checks

- Live connector check passed; create/update read-back and two-call upsert produced the expected state.
- Both deletes returned `deleted`; the repeat returned `not_found`; the delayed deleted feed returned both IDs.
- Hosted Salesforce run `651d73a3bc93496499ff6fbf8b1f58c2` succeeded with 3 extracted and 3 affected rows.
- Both tombstones reached `raw.salesforce_accounts`; the graph target contained 16 rows.
- Leases released, no staging tables remained, both schedules stayed paused, and Terraform reported `No changes.`

## Decisions

- Keep `Dander_External_ID__c` in the disposable org for later acceptance; it is unique and External ID metadata-verified.
- Keep write-back opt-in, explicitly confirmed, and single-attempt.
- Treat the proof as isolated acceptance only; no retained-project migration occurred.

## Remaining

- Decide whether to promote the accepted Dander and Salesforce candidates to stable releases.
- Migrate the retained project only through its separately reviewed plan and smoke sequence.

## Review First

- `docs/live-writeback-acceptance.md`
- `src/dander_connector_salesforce/source.py`
- `tests/test_source.py`
