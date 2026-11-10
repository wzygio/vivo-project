# DDD Python Project Instructions

These are project-type instructions shared by the enrolled DDD Python projects.
User-wide operating and safety rules belong to `C:/Users/V0141351/.codex/AGENTS.md`.
Each project's identity, configuration, constraints and specific task routes belong
to its own `CONTEXT.md`. Read the global instructions if they are not already loaded.

## Context Router

Before the first substantive task in a project, read its `CONTEXT.md`. For work
across projects, read each affected project's context. Follow only relevant task
routes; do not load every linked document or another project's business rules.

| Task | Read next |
|---|---|
| Purpose, terminology, project-specific rules, inputs or artifact lifecycle | The affected project's `CONTEXT.md` |
| Locate code, trace ownership, change naming or technical dependencies | Its `ARCHITECTURE.md` when present, then the relevant implementation |
| Find domain rules, specifications, designs or verification guidance | Its knowledge router, normally `references/index.md`, using the routes in CONTEXT |
| Issue, triage or domain-documentation work | The project procedure selected by CONTEXT |
| Change agent instructions or document ownership | The owning instruction tier and CONTEXT's maintenance rules; use `$manage-harness` for Harness work |

If a routed document is missing, inspect the actual project rather than inventing
its contents or copying another project's facts. Preserve established source names,
artifact lifecycles and project-specific exceptions recorded in CONTEXT.

## DDD and Engineering Conventions

- Prefer explicit, testable, traceable use cases and workflows. Follow the project's
  established DDD boundaries: domain/core behavior remains independent of external
  adapters, application code orchestrates use cases, and infrastructure integrates
  external systems. Keep presentation concerns outside domain behavior.
- Keep ARCHITECTURE focused on verified ownership, entry points, naming and technical
  boundaries; inspect implementation for detailed behavior. A domain inventory must
  be explicitly maintained before it drives directory organization. Do not reorganize
  existing packages or business assets just to match a template.
- For source discovery without CodeGraph, choose the scope and filename patterns from
  ARCHITECTURE before content search. The global CodeGraph rule applies when indexed.
- Use `$ecc-production-rules` for implementation, refactoring, debugging, testing,
  review and performance work. Load relevant `common` rules and Python extensions;
  activate framework rules only for a confirmed framework. Keep shared rule libraries
  in their installed location and use the project's configured tools and commands.
- Use `$manage-harness` for Harness creation, audit, migration or repair. Respect the
  project's adopted contract and explicit user overrides; do not copy shared Harness
  templates or historical rule libraries into a repository.
- Keep business rules and durable domain knowledge in project-owned documents. Route
  to the project's issue, triage, glossary and domain procedures through CONTEXT.
  Respect each project's index policy and retained knowledge; do not flatten it into
  one global file catalog or impose another project's paths.

## Update Routes

Update the authoritative owner and its affected consumers together:

- Rules applicable across project types: `C:/Users/V0141351/.codex/AGENTS.md`.
- Rules shared by this DDD Python project type: this hard-linked `AGENTS.md`.
- Project identity, specific instructions/task triggers, configuration constraints,
  important routes, artifact lifecycle and document ownership: its `CONTEXT.md`.
- Technical boundaries, source/test naming and maintained module ownership: its
  `ARCHITECTURE.md` and affected implementation/configuration.
- Business facts and decisions: the owning project document; update its knowledge
  router when the route, scope or naming changes.

The current NTFS link group's maintenance entry is `D:/wzy/Python/AGENTS.md`, with
enrolled paths in `D:/wzy/Python/AGENTS.links.json`. Follow the global hard-link
maintenance rules when editing any enrolled entry. Keep project-specific rules
directly in CONTEXT.
