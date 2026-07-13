# Generated Facts

Rebuildable generated summaries, scans, and audit outputs live here.

## Current Files

- `harness-audit.md`: output from `harness-builder` audit for the current repository.
- `harness-garbage-collection.md`: cleanup loop for stale Harness content.

## Rules

- Files in this directory should be reproducible from source files, scripts, or explicit commands.
- Do not hand-maintain business rules here; put stable rules in `specs/`, `config/`, or a design doc.
- Do not store generated reports or task outputs here; use `output/` for those artifacts.
