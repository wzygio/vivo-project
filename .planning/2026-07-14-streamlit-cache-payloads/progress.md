# Progress

## 2026-07-14

- Started the CPK alert request under the user-mandated `create-local-markdown-issue` → `triage` → `tdd` workflow; confirmed the repo uses root `.scratch/` Local Markdown cards and canonical Matt triage states.
- Read the issue-card template, Agent Brief contract, TDD behavior-testing guidance, domain context, architecture, and ADR routing. No `.codegraph/` index exists, so targeted `rg` exploration will be used.
- Confirmed the CPM service already preloads and caches raw points, Sheet features, and period capability before the query-gated chart section. The proposed alert should consume that same product-level ViewModel above the filters.
- Audited the capability and repository schemas. Only `param_name` exists as the parameter field, and the report mixes month/week/day rows; these two output-contract ambiguities will be made explicit in the Issue and triage decision.
- `create-local-markdown-issue` created `.scratch/cpm-cpk-alert/issues/01-cpk-alert-summary.md` as an `enhancement` in `needs-triage`, with no production-code side effects.
- A broad repository status search timed out once; the retry was scoped to configured tracker directories and found historical cards under `docs/.scratch/`. The new card follows the current Harness contract at root `.scratch/`.
- `triage` found no ADR/domain/out-of-scope conflict and confirmed the existing cached payload is the correct reuse boundary. The card moved to `needs-info` with AI-disclaimed triage notes; TDD is paused until the three output-contract questions are answered.
- Reporter resolved all triage questions: daily rows only, retain every valid CPK below `1.33`, and expose exactly five columns with one `param_name`-backed parameter-name field. Triage updated the card and Agent Brief to `ready-for-agent`; TDD phase started under explicit development authorization.
- One combined Issue/plan patch failed atomically because the stored plan contained a real Unicode RED→GREEN arrow while prior terminal output showed mojibake; reread the UTF-8 line and applied the Issue and plan updates separately.
- TDD slice 1 RED/GREEN complete: the public daily-alert builder now returns every daily CPK below `1.33` in the exact five-column display contract, excludes monthly/boundary rows, and sorts newest dates first.
- TDD slice 2 RED/GREEN complete: alert-bearing products render an expanded CPK alert center with an error summary and five-column detail table.
- TDD slice 3 RED/GREEN complete: products without capability data receive an explicit no-data status instead of a misleading all-clear message.
- TDD slice 4 RED/GREEN complete: executing the real CPM page now proves one service load followed by `alerts → filters → charts`; the alert is available before query-gated rendering and reuses the same ViewModel.
- Tightened the established contracts with non-numeric CPK and all-clear assertions before focused regression verification.
- CPM page and dashboard focused suite passed (`23 passed`).
- A filename discovery `rg` pattern returned no matches because of Windows path/output matching; used a scoped PowerShell filename filter instead and identified the eight relevant CPM/SPC test modules for expanded verification.
- Expanded CPM/SPC regression passed (`52 passed`); touched production/test modules compiled and `git diff --check` passed.
- Updated the ready-for-agent Issue with checked acceptance criteria and an AI-disclaimed TDD delivery note. No architecture/reference update was required because data ownership and the existing cached payload boundary remain unchanged.
- Final page-header cache discovery plus real page execution contract passed (`2 passed`), confirming the existing refresh entry still discovers the CPM payload cache.

- Confirmed the requested UI is implemented in the shared SPC section rather than the page composition file; recorded the current table orientation and three-chart layout before starting the RED tests.
- Capability-table RED/GREEN completed: period labels are rows and CPM/CPK are columns, while the existing 2M + 3W + 7D ordering, value formatting, and missing-value marker remain unchanged.
- Layout RED/GREEN completed with a Streamlit boundary test: the first row is two columns, and chamber/time Sheet-distribution figures render in direct full-width rows below it.
- Full SPC CPM dashboard unit module passed (`18 passed`); the focused dashboard, CPM service, decoration, and SPC config regression set passed (`28 passed`). `compileall` and `git diff --check` also passed.
- Reviewed the worktree after verification. Numerous unrelated pre-existing changes remain untouched; this request changed only the SPC dashboard section, its tests, and the active planning record.

- Started the cross-page Streamlit cache payload audit under explicit `$tdd` authorization.
- Restored the previous completed planning context and activated this new plan.
- Recorded the deterministic class-identity pickle reproduction and the existing native-dict precedent.
- Completed the first AST cache-boundary inventory; identified CPM and critical-parts report ViewModels as the two direct custom-class cache returns.
- Reviewed Yield nested dictionary returns and confirmed they contain DataFrames/native containers rather than project-defined runtime objects.
- Confirmed `docs/ADR/` exists and is currently empty; ADR creation remains gated on successful implementation and verification.
- CPM tracer-bullet RED reproduced the exact user-facing `UnserializableReturnValueError` through the real `st.cache_data` wrapper while the service module was reimported during a cache miss.
- CPM GREEN: split the cached native payload from the uncached ViewModel facade; exact regression test passes and `test_spc_cpm_service.py` is `4 passed`.
- Critical-parts RED reproduced the same `UnserializableReturnValueError` through its real cache boundary during module reimport.
- Critical-parts GREEN: cached native payload plus uncached ViewModel facade; exact regression passes and combined critical-parts tests are `13 passed`.
- Planning update error: one multi-file patch referenced a heading in the wrong file and was rejected atomically; reread the plan files and applied a corrected patch.
- Added and passed the page-header cache discovery contract test (`1 passed`).
- Post-change cache-return audit found no remaining custom ViewModel return under `st.cache_data`; compile check passed.
- Focused regression suite passed (`60 passed`).
- Broad unit regression completed with `124 passed, 2 failed`; recorded the two unchanged Shadow EMA failures and did not modify that unrelated module.
- Broad tests rewrote the tracked Z571 OOS Excel artifacts as a side effect. Confirmed they were clean at task start and restored only those two binary files from `HEAD`; no user-authored changes were affected.
- Added ADR-0001 and linked the reload-stable cache boundary from `ARCHITECTURE.md`.
- Final safety regression passed (`6 passed`); compile and diff checks passed; all plan phases complete.
- New request: optimize SPC boxplot Expander layout. Added phases 6–7 to the active plan; implementation will preserve report data semantics and use TDD.
