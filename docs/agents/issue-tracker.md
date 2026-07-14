# Issue tracker: Local Markdown

Issues and PRDs for this repo live as Markdown files in `.scratch/`.

## Conventions

- One feature per directory: `.scratch/<feature-slug>/`
- The PRD is `.scratch/<feature-slug>/PRD.md`
- Implementation issues are `.scratch/<feature-slug>/issues/<NN>-<slug>.md`
- Triage state is recorded in a `Status:` line near the top of each issue.
- Comments and conversation history append under `## Comments`.

## When a skill says “publish to the issue tracker”

Create a new file under `.scratch/<feature-slug>/`, creating directories as needed.

## When a skill says “fetch the relevant ticket”

Read the issue file path supplied by the user.
