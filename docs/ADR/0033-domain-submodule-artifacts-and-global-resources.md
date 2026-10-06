# ADR-0033: Domain Submodule Artifacts and Global Resource Locations

- Status: Accepted
- Date: 2026-10-06
- Supersedes: ADR-0027 resource-location configuration ownership only

## Context

Capability names in `ARCHITECTURE.md` provide a small, stable lookup map, while
development documents, knowledge and resources previously used inconsistent
categories and domain spellings. Resource locations were spread across domain
and product YAML, and some consumers reconstructed fixed filenames from a
configured parent. Relocation must preserve maintained workbooks and source data.

## Decision

- Use the Domain Submodule Architecture as the canonical capability inventory.
  Development specifications/evidence, references and resources follow
  `<domain>/<submodule>/`. Domain-wide artifacts use `shared`; project-wide
  artifacts use `shared_kernel/shared`. Equipment parts assets use `parts`,
  without creating a hypothetical source package. Keep existing data lifecycle
  directories and source package ownership.
- `config/global.yaml` owns `resources.<domain>.files` and configured collection
  `directories`; each file entry is a complete repository-relative or absolute
  path. Domain/product configuration retains business policies and sheet names.
  `ConfigLoader` resolves these paths and hydrates Yield runtime descriptors.
  Explicitly injected paths and ports remain caller-owned.
- Readers, writers, upload/download actions and cache signatures preserve the
  configured filename and independent directory. An invalid global config or
  missing entry in an active registry fails rather than selecting another file.
  Valid older isolated configs without a resource section retain compatibility.
- Preserve existing filenames, resource bytes, backups and signature companions.
  Keep historical extraction copies inside their owning artifact module; new
  rebuildable extraction output belongs under `output/`. Keep missing optional
  inputs absent until their owning use case supplies them.
- `CONTEXT.md` owns the unified route/lifecycle table, project constraints and
  document ownership. Retain it because its business constraints remain unique.
  Transfer the useful HARNESS rules there and retire the duplicate root document.
  `AGENTS.md` routes feature changes to scoped references and architectural
  changes to `ARCHITECTURE.md`. Keep reference filename rules unchanged.

## Consequences

Changing a maintained resource location requires one global configuration edit;
changing its business policy or sheet selection remains a domain/product edit.
Deployments must move resource files and configuration together. The migration
manifest records original/target paths and hashes; verification separately checks
all resources, unchanged data, live links and affected runtime consumers.

ADR-0027's OOS/OOC history, decision, workbook-content and presentation contracts
remain applicable. Its domain-YAML resource-path ownership is replaced by this
decision. No source snapshot refresh or manufacturing calculation change is
introduced by this artifact migration.
