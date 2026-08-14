# AGENTS.md

## Project Overview

This project is `vivo-project`. 天柱专项报表系统 - 基于标准 src 布局

Prefer explicit, testable, traceable workflows over broad automatic inference.

## Code Intelligence Policy

- Keep `ARCHITECTURE.md` shallow and use code intelligence for deep lookup.

## Context Router

- For project shape or runtime flow, read `ARCHITECTURE.md`.
- For Harness creation, audit, or repair, use `$manage-harness`.
- For shared engineering standards, use `$ecc-production-rules`.
- For project-owned knowledge routing, start at `references/index.md`.
- For project-owned domain knowledge, read `references/domain/`.
- For specs, runtime traces, or templates, use `specs/` when present.

## Iteration Router

- Update `CONTEXT.md` when the project purpose or stable operating model changes.
- Update `ARCHITECTURE.md` when ownership or runtime flow changes.
- Update `references/domain/` when stable terminology, invariants, mappings, or
  project-specific designs change.
- Update `references/retrospective.md` when the Harness itself evolves.
- Keep Harness `index.md` files folder-only.

## Safety Boundary

- Do not print, copy, commit, or persist secrets.
- Do not delete user data unless the user explicitly asks.
- Preserve unrelated user changes.
- E2E/browser automation artifacts (playwright-cli page snapshots, DOM dumps,
  console logs, screenshots) must be written to `output/test-results/` or
  `output/tmp/`, never to the repository root or `src/`.

## Agent skills

### Harness lifecycle

Use `$manage-harness` to create, audit, or repair the repository Harness.

- Keep only minimal project routing; do not copy shared Harness templates into
  this repository.
- Use `$ecc-production-rules` rather than Harness reference copies for
  engineering standards.
- `references/domain/` is project-owned and outside Harness management.

### ECC production rules

Use `$ecc-production-rules` for implementation, refactoring, debugging,
testing, security review, code review, performance work, and development
workflow decisions.

- This repository's default ECC rule sets are `common + python`.
- Load only task-relevant files from those two rule sets; Python rules override
  common rules when they conflict.
- Load rules directly from the installed Skill; do not create project-local
  rule copies.
- Load the FastAPI-specific rule only after confirming that the affected code
  belongs to a FastAPI application.
- Project instructions, ADRs, and repository tooling override conflicting ECC
  defaults.
- Do not activate other ECC language or framework rule sets unless the task
  explicitly requires them.

### Issue tracker

Issues and PRDs are tracked as local Markdown under `.scratch/`.
See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the canonical Matt Skills role names.
See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repository. Read root `CONTEXT.md`, then the
manufacturing glossary at `references/domain/GLOSSARY.md`
when relevant; consult `docs/ADR/` for applicable architectural decisions.
See `docs/agents/domain.md`.
