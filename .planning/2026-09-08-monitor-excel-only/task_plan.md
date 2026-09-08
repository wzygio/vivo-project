# Task Plan: Excel-only monitor and rolling snapshots

## Goal
Separate the Excel warning read model from raw computation, reorganize snapshots, restore rolling increments.

## Current Phase
Task implementation and production maintenance verified; merge/full-repository gate separate.

## Next Step
Production maintenance complete. User can resume queries. Do not merge without approval; unrelated full-repository test failures remain separately tracked.

## Phases

### Phase 1: Requirements for snapshot frontier
- **Status:** complete
- User approved directory → increment → delete/rebuild order, direct deletion without backup.
- Spec: .scratch/monitor-excel-only/PRD.md

### Phase 2: Snapshot plan
- **Status:** complete
- 01 path migration; 02 incremental repositories; verify then production rebuild.
- Preserve unrelated equipment edits and mutable Excel workbooks.

### Phase 3: Snapshot implementation and verification
- **Status:** complete
- Directory and rolling incremental code/tests complete, including cross-process transaction locks.
- After user confirmed stopped writers:75files migrated;58oldraw/metadata files deleted;14/14refreshes succeeded;21snapshots coverage/date/hash checks passed.

### Phase 4: Excel-only dashboard and validation
- **Status:** complete
- Excel-only source, weekly replacement, parent delta, missing-baseline protection, CPK workbook-only.
- Browser E2E and actual-page AppTest passed; 455 related unit tests passed.

### Phase 5: Delivery and project record
- **Status:** in_progress
- User generation document and architecture updated; no merge without user authorization.
- Full-repository regression failures outside task remain reported separately; ADR phase gate not passed.

## Errors Encountered
Fixed legacy test fixture schema/coverage contracts, crossyear query truncation, raw publication process locking and CLI sanitization.
Corporate Excel COM emitted 0x80010108 diagnostics during pytest teardown; task suite still exited 0 with455passing tests.
