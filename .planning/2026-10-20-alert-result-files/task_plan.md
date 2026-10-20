# P0 alert result publication and file consumption

## Objective and authorization
Implement the three P0 items accepted in the revised performance report: publish Yield Lot/trend result workbooks from the existing report workflow; consume them for matrix/warmup without analysis fallback; reuse workbook-level parsing for Inline OOS. Preserve L1 updates, existing rules and lazy detail charts. Do not query production data or overwrite maintained assets during tests.

## Phases
1. [complete] Trace producers, result/read ports, resource configuration, existing tests; specify minimal contracts.
2. [complete] Add failing tests for atomic per-product publication, independent failure, empty/missing/stale semantics, file-only matrix and Inline workbook reuse.
3. [complete] Implement Yield publication/reader, composition/resources and report integration; wire matrix defaults/signatures and Inline readers.
4. [complete] Run focused unit/integration/AppTest and architecture checks, isolated cross-process/performance probes; fix findings.
5. [complete] Update owning documentation, verify diffs/assets and record delivery evidence.

## Decisions
- Two configured XLSX result files, product sheets plus metadata. Existing file/Excel helpers are preferred.
- Application owns result contract and orchestration; infrastructure owns Excel I/O; composition assembles defaults.
- Missing, stale, failed or uncovered records do not trigger matrix-side calculation or SQL.
- Publication happens outside expensive calculation caches, skips unchanged content, preserves other products and previous success on failure.
- Tests use injected paths and memory sources. No temporary servers are planned; AppTest can exercise UI without listeners.

## Errors
- Initial RED runs failed collection because result/publication modules did not exist, as expected. The first 11 contract and file-only tests now pass.
