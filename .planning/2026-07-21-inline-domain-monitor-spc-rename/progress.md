# Progress Log

## 2026-07-21 — started

- Replaced the interrupted UNI point-line implementation with the user's system-wide rename request.
- Read the planning-with-files and TDD workflow instructions.
- Confirmed CodeGraph is unavailable in this repository.
- Recorded the existing partial user migration and its deleted/added files.
- No production file has been changed by this refactor yet.
- Read the prior active plan before switching the active plan pointer; it was complete and unrelated.
- Confirmed the existing migration has swapped file contents but not their public class/function/import names.
- Baseline focused test run: collection fails in two legacy tests because `src.spc_domain.application.cpm_service` has already been deleted. This is the expected rename gap to close, not a behavior failure.
- Directory-level source-package migration was blocked by an access-denied error. Switched to individual source-file moves so the change remains safe and reversible in the dirty worktree.
- Moved executable modules and unit tests to the inline-domain monitor/SPC subpackages; updated imports, public service/view-model names, page/section APIs, config accessors, active architecture references, and smoke targets.
- Focused inline smoke is green (77 tests); smoke-runner unit tests are green (8 tests). Full-unit verification remains blocked by an unrelated pre-existing Yield export mismatch.
