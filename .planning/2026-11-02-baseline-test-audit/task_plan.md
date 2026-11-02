# Audit and resolve the24 tracked regression failures

Base:9c61618257835d05770d8bdd2d65b1a377456252
Branch:fix/baseline-regressions-20261102
Worktree:output/tmp/baseline-test-audit-20261102
Authority:user explicitly requests deletion of obsolete-business tests and repair of valuable ones. pytest-test-curator + ECC Python; current supported contracts decide, never merely current output.

### Phase1: Reproduce and map
**Status:** complete
All24 reproduced on pinned9c616182 with guards,68 maintained files unchanged. Mapped14 issue groups. Actual supported contracts identified; no new requirements needed.
Collect exact24 nodeids, reproduce under DB/Office/maintained-write guard, map14 issue groups to current knowledge/callers. Preserve current formal services.

### Phase2: Decide and repair
**Status:** complete
22 Items retained/repaired;2 obsolete Items retired with replacements. One actual production defect fixed: factory-wide parts reader rejected empty product cache revision. Other repairs isolate current policies/configuration/ports and correct outdated assertions.
Record item-level active/replaced/retired decisions with evidence. Delete only confirmed obsolete Items, retain supported unique boundaries and fix wrong implementation/assertions/fixtures. No broad moves or weakening for green.

### Phase3: Validate and review
**Status:** in_progress
Retained22 plus initial replacement route28 pass; all owned file groups pass40/90/64. Frozen-base collection2845 ->2849, exactly2 retired and6 new Items. Full guarded run in progress; independent review next.
Exact nodes ->changed files ->affected scope ->complete unit/architecture/integration as appropriate. Independent code-review once changes are concrete. No production DB or business workbook writes. Temporary services unnecessary for AppTest/unit tests.

### Phase4: Deliver
**Status:** pending
Update existing14 issues and dated cross-domain audit with every decision, before/after counts, commands and limits. Commit only owned changes, integrate safely under existing task authorization; do not restart formal services for test-only changes.
