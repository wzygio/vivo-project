# 01: Optional authorized queries and complete results

Status: complete
Blocked by: None
Spec: ../PRD.md

Allow authenticated callers to omit every query parameter and retrieve all released OOS records across enabled authorized products and categories. Remove the duplicate starting station. No implicit100-row truncation; retain resource caps and explicit1..500 pagination.

- [x] No parameters selects only authorized and enabled scopes.
- [x] Explicit unauthorized filters fail before reading.
- [x] More than100 records return without limit; explicit pagination still works.
- [x] No starting-station response field.

## Comments
2026-11-02: User approved sequence and HTTP/file test seams.
RED: zero-parameter HTTP returned422; GREEN:120 rows across four authorized product/category combinations. Existing API regression102 passed after date/partial additions. Focused guarded suite245 passed before final byte-budget refinement.
