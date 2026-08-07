# Streamlit Cache Payload Stabilization

Goal: audit every page-reachable `st.cache_data` boundary, replace project-defined return objects that can break across module reloads with reload-stable payloads, preserve public page behavior, and record the successful design in `docs/ADR/`.

## Phases

| Phase | Status | Notes |
|---|---|---|
| 1. Inventory page-reachable cache functions and classify return types | complete | CPM and critical-parts are the only direct custom-dataclass cache returns; other page paths return native payloads. |
| 2. Add one reload-boundary regression test and fix CPM via RED→GREEN | complete | Real Streamlit cache/thread/reimport test now passes; nested decoration result is reconstructed outside cache. |
| 3. Repeat RED→GREEN for every other affected page boundary | complete | Critical-parts real cache/thread/reimport regression now passes. |
| 4. Run focused and broad regressions; review cache-clear behavior | complete | Focused `60 passed`; broad unit `124 passed, 2` pre-existing Shadow EMA failures; compile and audit pass. |
| 5. Write ADR after successful verification | complete | ADR-0001 records the native-payload boundary, alternatives, consequences, and regression contract. |
| 6. Analyze SPC Expander layout and write the first layout regression test | complete | RED then GREEN tests now cover the transposed capability table and three-row rendering sequence. |
| 7. Implement the three-row layout, verify CPM UI helpers, and update relevant references | complete | Implementation and focused SPC regressions passed; no architecture or workflow reference changes were needed because data ownership and runtime flow are unchanged. |
| 8. Analyze the CPK alert request and create a Local Markdown Issue | complete | Created the enhancement card under the current root `.scratch/` tracker with evidence, acceptance criteria, cache constraints, and three explicit output-contract questions. |
| 9. Triage the issue through the local state machine | complete | Classified as enhancement and moved to `needs-info`; cache/design feasibility is confirmed, but three user-visible data-contract choices remain unresolved. |
| 10. TDD the preloaded cached report payload and CPK alert behavior | complete | Four behavior slices cover daily alert extraction, alert display, no-data state, and one-load page ordering while preserving ADR-0001. |
| 11. Integrate the alert Expander above filters and verify regressions | complete | One cached ViewModel feeds alerts and query-gated charts; focused `23 passed`, expanded CPM/SPC `52 passed`, compile and diff checks passed, and the Issue delivery record is complete. |
| 12. Document the boundary between PRDs, Issues, clarification skills, and execution planning | complete | Published a reusable guide under `docs/dev_docs/generated/`, grounded in the named Skill contracts and this repository's Local Markdown tracker. |
| 13. Verify the generated guide and record the delivery | complete | Confirmed required sections and tracker terminology with `rg`; `git diff --check` passed without whitespace errors. |
| 14. Audit the repository Harness and both Harness Skills | complete | Workflow model is coherent; found stale project routers plus duplicated hard-coded old topology across both Skills. Project Harness remains unchanged in this task. |
| 15. Extract a replaceable Harness architecture profile | complete | One shared profile now owns workflow topology, routes, index layout, validation anchors, audits, and migration policy. |
| 16. Refactor `harness-creator` against the profile | complete | Creator, checker template, SKILL.md, and UI metadata now consume/describe the shared workflow profile; repository dry-run succeeds. |
| 17. Refactor `harness-refactor` against the profile | complete | Refactor loads Creator/profile, archives collision-safely, preserves existing files by default, supports explicit overwrite, and reports dry-run actions. |
| 18. Validate both Skills and assess project-side changes | complete | Isolated create/refactor/overwrite/check scenarios and both Skill validators pass; current project Harness was not modified. |
| 19. Add summary-reference routing to the project Harness | complete | Initialized the summary index, repurposed retrospective as a docs artifact router, and corrected the root references index. |
| 20. Synchronize the shared Harness profile | complete | Summary references now belong to the summary stage, root index topology, validation anchors, and a Profile-owned docs artifact router. |
| 21. Validate navigation and profile-driven dry-run | complete | Current routes exist; Creator/Refactor discover summary references; isolated create/additive-refactor/overwrite/check scenarios preserve the docs router and log separately. |

## Guardrails

- Preserve `st.cache_data` for page-facing data flows; do not replace data caches with `st.cache_resource`.
- Keep cached payloads limited to reload-stable native containers, DataFrames, scalar values, strings, and paths serialized as strings.
- Reconstruct project-defined dataclasses/Pydantic models outside cache boundaries.
- Preserve unrelated dirty worktree changes.
- Follow one-test-at-a-time RED→GREEN; refactor only while green.
- Do not write the ADR until implementation and regression verification succeed.
- Treat the user's design → plan → dev/test → summary workflow as the desired Harness profile, not a universal hard-coded truth.
- Keep architecture policy independently replaceable from creator/refactor orchestration code.
- Do not modify the current project Harness merely to make the refactored Skills pass.
