# Harness Garbage Collection

This file tracks stale Harness content and cleanup candidates. It is Agent-maintained and rebuildable from repository scans plus human review.

## Current Status

- No stale Harness entrypoints are known after the 2026-06-12 repair.
- `docs/plans/plan-harness_refactor.md` and `docs/plans/plan-indicator_improvement.md` are retained as long-lived plan history and are indexed from `docs/plans/index.md`.
- Runtime logs under `logs/`, task outputs under `output/`, caches, and `__pycache__/` folders are not Harness content; clean them only under a task-specific cleanup request.

## Cleanup Rules

- Before deleting a Harness file, confirm it is not linked from `AGENTS.md`, `.roorules`, `CONTEXT.md`, `docs/design/index.md`, `docs/plans/index.md`, or `specs/README.md`.
- Prefer replacing obsolete content with a redirect note when another Agent may still follow an old path.
- Generated Harness files should be reproducible; if not, move stable knowledge into `docs/design/`, `docs/plans/`, `docs/references/`, or `specs/`.

## Periodic Review Checklist

- Run the Harness audit:

```powershell
$env:PYTHONUTF8='1'; python C:\Users\V0141351\.codex\skills\harness-builder\scripts\build_harness.py D:\wzy\Python\vivo-project
```

- Check local Markdown links in Harness files.
- Review `docs/exec-plans/active/` and move completed plans to `docs/exec-plans/completed/`.
- Keep this file updated with any intentional stale path, replacement path, and deletion decision.
