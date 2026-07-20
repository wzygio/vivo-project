# Findings & Decisions

## Requirements
- Input: one development requirement.
- Four modules: requirement formulation, planning, development/testing, project sedimentation.
- Outputs: `.scratch/` issue, `.planning/` plan and checklist, code plus test evidence, `docs/ADR/` decision record.
- Invoke the installed dependency skills; do not duplicate their internal workflows.
- Each module must be independently editable and optionally executable by a sub-agent; default execution stays in the main agent.

## Research Findings
- `create-local-markdown-issue` creates a `needs-triage` card and explicitly does not implement.
- `triage` can stop at maintainer recommendation and only `ready-for-agent` is safe for implementation.
- `planning-with-files` owns `.planning/<plan-id>/task_plan.md`, `findings.md`, and `progress.md`; the requested checklist should live in the plan.
- `tdd` requires vertical red-green cycles and expects interface/behavior approval before coding.
- `playwright-interactive` requires a QA inventory and separate functional, visual, viewport-fit, and exploratory browser checks.
- Existing user skills favor a thin orchestration entry point and delegate specialized behavior to named skills; runtime modules own their own execution.
- The current repo uses `.scratch/`, `.planning/`, and `docs/ADR/` (uppercase ADR).

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Keep orchestration declarative in Markdown rather than add scripts | The work is agent judgment and cross-skill dispatch, not deterministic data transformation. |
| Put module contracts one reference level below `SKILL.md` | Matches progressive disclosure and permits isolated edits. |
| Pass handoff artifacts by path, not by copying prior conversation | Supports sub-agent context isolation and traceability. |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| Dependency skills contain deliberate human approval gates | Model them as stop/resume gates; do not bypass or duplicate them. |
| ADR output path casing varies across repositories | Prefer repository convention when present; for this requested flow default to `docs/ADR/`. |

## Resources
- `C:\Users\V0141351\.codex\skills\.system\skill-creator\SKILL.md`
- Named dependency skills under `C:\Users\V0141351\.agents\skills\`
- `python-project-initializer` and `daily-report-generator` as thin-orchestrator examples
