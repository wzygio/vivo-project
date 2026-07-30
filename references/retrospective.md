# Docs Artifact Router

## Purpose

Navigate durable project artifacts under `docs/`. Project-owned domain
knowledge belongs in `references/domain/`; shared engineering guidance is
loaded from installed Skills; temporary task state belongs in `.scratch/` and
`.planning/`.

## Artifact Routes

- `docs/PRD/` — approved requirement and product-specification documents.
- `docs/ADR/` — durable architectural decisions and their consequences.
- `docs/dev_docs/` — development prompts, tutorials, and generated engineering guidance.
- `docs/agents/` — repository-specific Agent/Harness operating configuration.
- `docs/others/` — retained project documents that do not belong to the above artifact classes.

## Update Rule

When a durable artifact class is added under `docs/`, add its folder route here. Keep this router folder-level; do not list individual documents.

## Harness Evolution

- 2026-07-29: Consolidated `$harness-creator` and `$harness-refactor` into
  `$manage-harness`.
- Harness creation and repair now keep a minimal project router and do not copy
  shared Harness templates into repositories.
- Removed local design, development, test, and summary rule copies; shared
  engineering standards now come from `$ecc-production-rules`.
- `references/domain/` is project-owned and must never be seeded from the
  Harness or ECC rule libraries.
