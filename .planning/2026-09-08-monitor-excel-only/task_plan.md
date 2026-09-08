# Task Plan: Excel-only monitor and rolling snapshots

## Goal
Separate the Excel warning read model from raw computation, reorganize snapshots, restore rolling increments.

## Current Phase
Development/testing: approved snapshot frontier.

## Next Step
Implement path migration and incremental repositories independently, validate before modifying runtime data.

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
- **Status:** in_progress
- No production file deletion until migration manifest, code tests, and writer safety checks pass.

### Phase 4: Excel-only dashboard and validation
- **Status:** pending
- Resolve current CPK year/quarter source; do not substitute summed/averaged weekly CPK.

### Phase 5: Delivery and project record
- **Status:** pending
- Update user docs and architecture; no merge without user authorization.

## Errors Encountered
None in implementation yet. Prior file inspections sometimes truncated; reread selected instruction tail before acting.
