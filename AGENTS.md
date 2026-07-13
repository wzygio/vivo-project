# AGENTS.md

## Project Overview

This project is `vivo-project`. 天柱专项报表系统 - 基于标准 src 布局

Prefer explicit, testable, traceable workflows over broad automatic inference.

## Code Intelligence Policy

- Keep `ARCHITECTURE.md` shallow and use code intelligence for deep lookup.

## Context Router

- For project shape or runtime flow, read `ARCHITECTURE.md`.
- For Harness routing, start at `references/index.md`.
- For design, development, and test knowledge, use `references/`.
- For specs, runtime traces, or templates, use `specs/` when present.

## Iteration Router

- Update `ARCHITECTURE.md` and design references when ownership or runtime flow changes.
- Update development references when coding rules or restrictions change.
- Update test references when validation, smoke, or observability changes.
- Update `references/retrospective.md` when the Harness itself evolves.
- Keep Harness `index.md` files folder-only.

## Safety Boundary

- Do not print, copy, commit, or persist secrets.
- Do not delete user data unless the user explicitly asks.
- Preserve unrelated user changes.
