# Inline Domain Monitor/SPC Naming Refactor

## Goal

Complete the user's domain split and rename without losing the existing worktree migration:

- legacy alert-monitoring `spc` names become `monitor`;
- legacy capability-report `cpm` names become `spc`;
- related modules live in `monitor` / `spc` subpackages at each applicable layer;
- the broad `spc_domain` package becomes `inline_domain`;
- `spc_repository.py` remains named for the physical SPC data source.

## Current Phase

Complete.

## Phases

1. [x] Inventory existing user rename work, identify all source/test/page imports, and record the final name map.
2. [x] Write or rename focused tests to the new public contracts, then establish a failing baseline.
3. [x] Move packages/files to the `inline_domain`, `monitor`, and `spc` hierarchy; update all imports and public class/function names.
4. [x] Update pages, sections, tests, static links, and documentation references; preserve the physical `spc_repository.py` name.
5. [x] Run focused and domain-level verification, compile checks, and diff checks; document unrelated/pre-existing failures.

## Guardrails

- Preserve the user's in-progress rename changes; do not reset or restore deleted files.
- Do not rename `spc_repository.py`.
- Treat historical documents and source-data naming separately from executable import paths.
- The prior UNI point-line request is superseded by this naming-refactor request; retain only existing worktree changes needed by the final renamed code.
