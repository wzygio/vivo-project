# Specs

This directory is for user-maintainable rules, contracts, and task specifications that should not be hidden inside prompt text or implementation code.

## What Belongs Here

- Stable report contracts and expected output schemas.
- Rule tables or documented matching semantics that users may review.
- Task templates that can be executed by deterministic CLI or service entrypoints.
- Acceptance criteria for repeatable workflows.

## What Does Not Belong Here

- Generated reports, screenshots, logs, caches, or decrypted temporary files.
- One-off analysis outputs; put those under `output/`.
- Private credentials or environment-specific secrets.

## Current Sources Of Truth

- Project architecture: `../ARCHITECTURE.md`
- Design routing: `../docs/design/index.md`
- Plan routing: `../docs/plans/index.md`
- Prompt/task drafts that may later become specs: `../docs/prompt/`
