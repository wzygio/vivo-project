# API2.2 optional queries and response fields

Mode: development-flow complex
Base: be925c9eea4f7f230ab7c151f9e918a1ed835f41
Branch/worktree: feat/anomaly-api-query; output/tmp/anomaly-api-query-20261102
Spec: .scratch/anomaly-api-query/PRD.md

## Phases

### Phase 1: Requirements
**Status:** complete
Approval2026-11-02: inclusive dates, explicit partial results,01 ->02 ->03 and existing HTTP/file seams confirmed by user.
User explicitly requested feat-API2.2: all filters optional, default authorized all-products/all-sources; OOS default, inclusive Shanghai calendar dates, unlimited row count by default, remove redundant starting station, explain cursor and propose future fields without adding them. All material questions answered; partial available results explicitly marked.

### Phase 2: Planning
**Status:** complete
Execution view approved with requirements. Tickets01/02 complete; frontier03. Failure contract uses meta.complete/failed_sources, all-failed503 with safe error.failed_sources.
01 optional authorized scope/unlimited results and removal →02 local published date window/filter end-to-end →03 global cursor binding/security/integration/docs. Reuse HTTP/file seams and existing source application readers; extend Inline date-selection contract while preserving page defaults.

### Phase 3: Development and testing
**Status:** in_progress
Test-first at existing HTTP and temporary-file source seams; static checks, focused coverage, guarded regression and independent Standards/Spec review. No production DB or workbook writes. Keep all original user changes. Prior session's merge/deployment authorization applies only to the existing8890API; never restart8503or unrelated services.

### Phase 4: Project record
**Status:** pending
Update owning API guide/architecture/ADR and record verification. Explain cursor simply; list possible extra return fields with their actual source/prerequisites, implement none beyond explicitly required contract.

## Decisions and limits
All means enabled ∩ authorized, never expanded access. Existing API key, byte/cache/thread/time limits remain; optional limit controls row pagination only. Unknown/excess query fields remain forbidden. Only published local data is read. Missing coverage cannot trigger DB or calculation fallback. No new credentials or business-data regeneration.
