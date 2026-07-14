# Docs Artifact Router

## Purpose

Navigate durable project artifacts under `docs/`. Reusable guidance belongs in `references/`; temporary task state belongs in `.scratch/` and `.planning/`.

## Artifact Routes

- `docs/PRD/` — approved requirement and product-specification documents.
- `docs/ADR/` — durable architectural decisions and their consequences.
- `docs/dev_docs/` — development prompts, tutorials, and generated engineering guidance.
- `docs/agents/` — repository-specific Agent/Harness operating configuration.
- `docs/others/` — retained project documents that do not belong to the above artifact classes.

## Update Rule

When a durable artifact class is added under `docs/`, add its folder route here. Keep this router folder-level; do not list individual documents.
