# AGENTS.md

## Project Overview

`vivo-project` is the Tianzhu manufacturing quality reporting system, built with Streamlit and a standard `src` layout with DDD layers. Prefer explicit, testable, traceable workflows.

## Context Router

Use this file as the repository entry point. Load additional context by task; do not read every linked document automatically.

| Task | Read next |
|---|---|
| First substantive task in this repository; project purpose or business constraints | [CONTEXT.md](CONTEXT.md) |
| Locate code, understand runtime flow, or change ownership/dependencies | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Change UI, messages, downloads, or exports | [Non-administrator presentation boundary](CONTEXT.md#non-administrator-presentation-boundary) before editing |
| Find business rules or project-specific designs | [Knowledge router](references/index.md); select only relevant entries |
| Interpret manufacturing terminology | [Manufacturing glossary](references/domain/shared_kernel/shared/GLOSSARY.md) |
| Change an established architectural boundary | Relevant decisions under [docs/ADR/](docs/ADR/) |
| Change agent instructions, document ownership, or knowledge routing | [Document ownership](CONTEXT.md#maintenance-and-document-ownership) and this file's Iteration Router |

If `.codegraph/` exists, use CodeGraph before text search or source reads for code discovery. Otherwise, skip indexing and follow the scoped `rg` workflow in `ARCHITECTURE.md`. Verify actual paths rather than inventing missing files.

## Safety Boundary

- Do not print, copy, commit, or persist secrets.
- Do not delete user data unless explicitly requested. Preserve unrelated user changes.
- Write browser snapshots, DOM dumps, console logs, and screenshots only to `output/test-results/` or `output/tmp/`, never to the repository root or `src/`. See [output classification](output/README.md).
- Follow the project constraints in `CONTEXT.md`; a linked constraint is not optional when its task trigger applies.

## Development Conventions

Use `$ecc-production-rules` for implementation, refactoring, debugging, testing, security/code review, performance work, and development workflow decisions.

- Default rule sets: `common + python`. Load only task-relevant rules directly from the installed skill; do not copy rule libraries into this repository.
- Python rules override common rules where they conflict. Project instructions, applicable ADRs, and repository tooling override conflicting ECC defaults, subject to higher-priority instructions and the current user request.
- Load FastAPI-specific rules only for confirmed FastAPI code. Do not activate unrelated language/framework rules.
- Issues and PRDs use local Markdown: [issue tracker](docs/agents/issue-tracker.md). For triage, read [role labels](docs/agents/triage-labels.md); for domain documentation, read [domain conventions](docs/agents/domain.md).
- Choose verification that matches the change; report commands, results, and material omissions. Documentation-only edits need route/content checks, not a business-suite run by default.

## Iteration Router

Update the owner of the changed fact:

- Project feature behavior or domain business rules: update the matching `references/{domain,design}/<domain>/<submodule>/` document; update `references/index.md` when its route or scope changes.
- Project architecture, domain/submodule ownership, directory organization or runtime dependencies: update `ARCHITECTURE.md`, especially the canonical Domain Submodule Architecture and its path grammar. Update affected consumers/configuration when paths change.
- Purpose, operating assumptions, project-wide constraints, resource configuration or artifact lifecycle: update `CONTEXT.md` and the owning configuration.
- Agent entry instructions or task triggers: update this file.
- Knowledge filenames or task-to-document routes: update `references/index.md`; retain one authoritative owner per fact.

Keep folder indexes folder-only, except `references/index.md`, which owns file-level knowledge routes and naming rules. Use the canonical domain/submodule map to find scoped files rather than expanding root documents into file catalogs.
