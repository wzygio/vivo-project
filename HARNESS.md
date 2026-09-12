# Harness Architecture

## Purpose and Scope

This repository Harness is the set of instructions, knowledge routes, work records, and verification entry points that helps coding agents perform traceable work. The four root documents are its documentation interface, not a replacement for tools, tests, or execution permissions.

Read this file when changing the Harness itself. Ordinary feature work starts from `AGENTS.md` and follows only the routes needed for the task.

## Document Ownership

| Document | Owns | Does not own |
|---|---|---|
| [AGENTS.md](AGENTS.md) | Entry instructions, essential safety rules, task triggers, and update routes | The full project architecture or every engineering rule |
| [CONTEXT.md](CONTEXT.md) | Project purpose, operating assumptions, important locations, and business constraints | A second source-tree map or Harness artifact catalog |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Domain/layer/submodule map, dependency rules, runtime ownership, and code lookup | Per-file tutorials, product-specific algorithms, or task progress |
| [HARNESS.md](HARNESS.md) | Disclosure policy, document ownership, artifact lifecycle, and Harness evolution | Business rules, implementation details, or duplicated shared skill rules |
| [references/index.md](references/index.md) | Task-to-knowledge routes and knowledge-file naming rules | Automatically loaded contents of every referenced file |

These responsibilities are a project-owned convention. `CONTEXT.md`, `ARCHITECTURE.md`, and `HARNESS.md` are loaded through explicit routes; do not assume an agent tool automatically loads them because of their names. Do not replace this layout with an external template unless the user requests such a migration.

## Progressive Disclosure

1. Start with the applicable agent instructions and the current task. Read `CONTEXT.md` for the first substantive repository task; revisit the relevant constraints when scope changes.
2. For code work, use `ARCHITECTURE.md` to select a domain, layer, and submodule. Read only relevant source, interfaces, consumers, and tests.
3. For business rules or established design choices, select a document through `references/index.md` or an applicable ADR. Follow deeper links only when they resolve a task-specific question.
4. Read this file for Harness maintenance, not as a prerequisite for every business change.
5. If a route is missing or stale, inspect actual paths and scoped evidence. Correct a verified stale link; do not infer that a missing document means a missing feature.

A route must state both its trigger and destination. Keep critical safety instructions at the entry point; keep lengthy specialized rules behind explicit task triggers. Root-document prose is English; preserve actual code identifiers and filenames. This convention does not require translating all project-owned business documents or renaming resources.

## Filename-Guided Discovery

Filenames are a lightweight discovery interface: use the naming convention of the target area to narrow candidates before reading their contents. This supports progressive disclosure and remains useful when an index is incomplete or stale.

1. **Scope the search.** For code, use [ARCHITECTURE.md](ARCHITECTURE.md#task-directed-code-lookup) to select the domain, layer, and submodule. For knowledge, follow a matching route in [references/index.md](references/index.md) first; use filename discovery when no reliable route answers the question.
2. **Map the question to naming terms.** Identify the topic and the required responsibility or document type. For example, month/week/day trends map to `mwd-trend` in knowledge filenames and `mwd_trend` in code paths. Use the target area's actual naming convention rather than assuming identical separators or suffixes everywhere.
3. **List and filter names.** Use `rg --files` within the selected directory, filtering by topic and, where applicable, type prefixes or role suffixes. Avoid reading every file merely to discover which one is relevant.
4. **Verify candidates.** Inspect document headings and scope, or code definitions, interfaces, and consumers. A matching filename identifies a candidate; it does not prove current behavior or ownership.
5. **Expand only when needed.** If names do not identify a suitable candidate, search headings, symbols, or keywords within the same scope before broadening to neighboring modules. Verify stale routes against actual files and repair confirmed mismatches.

For code discovery, the CodeGraph-first instruction in `AGENTS.md` still applies when an index exists; filename filtering complements that workflow rather than replacing it.

Naming responsibilities remain separate:

- This file owns the discovery principle and search sequence.
- `references/index.md` owns the exact knowledge-file naming grammar, type prefixes, scope, and exceptions. Do not duplicate its complete naming table here.
- `ARCHITECTURE.md` owns code-path selection and scoped lookup examples. Source files follow their language and module conventions, not the knowledge-document naming grammar.

When creating a file, choose a stable, descriptive topic and responsibility that fit the existing naming rules. Preserve established entry-point names and actual identifiers; this principle does not authorize bulk renaming. When a file is intentionally renamed, update its routes and references so both explicit navigation and filename discovery remain usable.

## Work and Knowledge Artifacts

| Path | Role and lifecycle |
|---|---|
| `docs/ADR/` | Durable architectural decisions, rationale, and consequences; follow existing status and numbering conventions |
| `docs/agents/` | Project-specific issue, triage, and domain-documentation procedures |
| `docs/dev_docs/` | Development guidance and supporting engineering documents |
| `docs/dev_docs/generated/` | Generated assessments and delivery evidence; record scope/date and distinguish proposals from accepted decisions |
| `references/domain/` | Protected project-owned terminology, rules, mappings, and domain knowledge |
| `references/design/` | Project-owned designs organized by feature (`feat_design/`), module (`module_design/`), and system (`system_design/`) scope |
| `.scratch/` | Local PRDs, issues, and task discussions under the [issue-tracker convention](docs/agents/issue-tracker.md); not an automatically disposable cache |
| `.planning/` | Optional execution plans, findings, and progress for complex or long-running work; complements the PRD/issues rather than replacing them |
| `output/test-results/`, `output/tmp/` | Runtime verification and temporary artifacts; subject to the repository safety and output rules |

Use one task identifier across PRD, plan, and evidence when those artifacts are needed. Link to requirements instead of copying them into a second plan. Small tasks do not require both task directories. Promote verified, durable knowledge to its owning document; do not silently turn generated recommendations or task notes into policy.

`references/index.md` is the explicit exception to folder-only Harness indexes: it may route to individual knowledge files and owns their naming rules. Other Harness indexes stay folder-only. `references/domain/` and `references/design/` are project-owned content, not shared engineering-rule libraries.

## Maintenance and Validation

- Keep one authoritative owner for each rule. A summary may link to that owner; it must not introduce a competing variant. Follow platform instruction precedence and explicit current user requests when resolving conflicts.
- Use the update routes in `AGENTS.md`. A change to code layout updates `ARCHITECTURE.md`; a new file inside an existing module normally does not.
- Preserve unrelated changes and business assets. Do not restore deleted documents, move reference trees, or copy global skill libraries merely to satisfy an external template.
- After document changes, check local links/anchors, actual directory names, disclosure triggers, duplicate or conflicting rules, and the English-language requirement for the four root documents.
- After executable changes, run checks appropriate to the affected behavior. Locate tests through `ARCHITECTURE.md`; `tools/smoke.py all` runs unit tests, not architecture, integration, or browser suites.
- Report changed files, evidence, unresolved conflicts, and checks not performed. A documentation check is not evidence that runtime behavior passes.
- Add automation, nested instructions, or multi-agent coordination only when repeated failures or task scale justify them. This document does not authorize installing hooks, spawning agents, or expanding execution permissions.

## Harness Evolution


