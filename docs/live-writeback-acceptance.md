# Salesforce Write-Back Acceptance

This is a prepared checklist, not authorization to run it. Use only a disposable Salesforce org
after the connector and Dander capability contracts are reviewed and published.

## Setup

- Keep every hosted schedule paused.
- Create a text field named `Dander_External_ID__c` on Account and mark it External ID and Unique.
- Grant the disposable integration user only the object and field access needed by the test.
- Install exact published Dander and connector versions outside both source checkouts.
- Configure a dedicated Account endpoint with `Dander_External_ID__c` as its sole primary key.

## Proof

1. Run `connector check` and inspect capabilities.
2. Create one Account with a recognizable test prefix and record the returned Salesforce `Id`.
3. Update that Account by `Id`, then read it back.
4. Upsert a second Account by `Dander_External_ID__c`; repeat with changed data and verify Salesforce
   still contains one record for that external identity.
5. Delete both test Accounts by `Id`; repeat one delete and verify the not-found outcome is clear.
6. Query the deleted-record feed inside the 15-day window and verify the deleted identities appear.
7. Confirm no scheduled ingestion ran and no unrelated Salesforce record changed.

## Cleanup

- Remove any surviving prefixed test records.
- Remove the disposable custom field if the org is not being retained for later acceptance.
- Record exact package versions, commands, returned identities, and sanitized outcomes.
