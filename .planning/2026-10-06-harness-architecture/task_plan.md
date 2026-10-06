# Harness architecture migration

Source: `docs/dev_docs/dev_spec/shared_kernel/shared/refactor-harness_arch.md` (follow its moved path in the migration manifest).

## Phases

1. Inventory and classify documents/resources; trace resource consumers. Status: complete.
2. Add globally configurable resource registry and regression coverage; preserve injectable paths. Status: complete.
3. Move files using a collision-checked manifest, verify resource hashes, rewrite live references. Status: complete.
4. Consolidate context/routes/iteration rules; retire HARNESS only after transferring unique rules. Status: complete.
5. Run resource, architecture, consumer and documentation checks; review and record delivery. Status: complete.

## Constraints and decisions

- Preserve pre-existing edits, deletions, maintained workbooks, backup files and sidecars.
- Do not read or copy secrets; do not query production databases or refresh source snapshots.
- Use domain/submodule ownership across the six requested trees; retain shared/common capabilities without inventing source packages.
- Existing data directories are checked, not migrated.
- Global configuration owns maintained resource locations; product configuration retains sheet/business policy only.
- Source files remain in their existing packages; this is artifact/resource organization and lookup-rule maintenance.

## Errors

- Review caught fail-open global configuration and basename-dependent SPC Delete persistence. Both fixed test-first and independently re-reviewed.
- Equipment fallback tests assumed afternoon source records remain visible despite the established noon cutoff. Isolated coverage policy in those tests; production cutoff unchanged.
- IQC cutoff test still patched removed DEMO_DIR; migrated its fixture to configured-path injection.
- The first full combined run exposed missing temporary global YAML in six Inline fixture owners. Added a real temporary resource registry; 70 scoped tests and the final 470-test combined suite pass.
