# Progress: AOI_TT Particle Size

## Session: 2026-09-03

### Requirements and triage

- **Status:** complete
- Created `.scratch/aoi-tt-report/issues/02-distinguish-particle-size.md`.
- Classified as `enhancement + ready-for-agent` with complete Agent Brief and acceptance criteria.
- Confirmed the reference SQL count inflation is caused by joining defect facts to a non-unique SPC Sheet mapping.

### Planning

- **Status:** complete
- Created isolated plan, findings and progress artifacts.
- Mapped all ten acceptance criteria to unit, regression, static and browser verification.
- Recorded user authorization from “分析并完成” and the spec's “直接执行到底” as plan approval.

### Development and testing

- **Status:** complete
- Development branch: `feat/aoi-tt-particle-size` from local `master`.
- Repository root: `D:\wzy\Python\vivo-project`; no separate worktree.
- Pre-existing unrelated changes remain in place and are excluded from this task's ownership.
- RED 1: `uv run pytest -q tests/unit/inline_domain/infrastructure/aoi_tt/test_particle_size_loader.py` failed during collection because the Particle Size loader does not exist yet (expected tracer-bullet failure).
- GREEN 1: loader integration test passes with duplicate SPC rows while O/L counts remain 2/1.
- RED/GREEN 2: Particle detail composer was missing, then passed with Total preservation, O/L count merge, unmatched defect exclusion and zero-fill.
- RED/GREEN 3: aggregate test first combined sizes (11/2); after adding Particle Size to the domain key, core suite passed `12 passed`.
- RED/GREEN 4: application service initially returned Total-only; after orchestration change, service suite passed `10 passed`, with Particle fallback preserving Total.
- RED/GREEN 5: repository initially rejected the Particle loader dependency; after port/delegation wiring, infrastructure slice passed `3 passed`.
- RED/GREEN 6: UI initially returned four filter values and combined duplicate Lot IDs; after Particle selector and per-size rendering, dashboard suite passed `13 passed`.
- RED/GREEN 7: page initially failed to unpack the Particle filter; page now filters details by selected Particle Size and focused test passes.
- Refactored the per-size three-chart renderer into one helper; the station/parameter Expander remains the owner of all selected sizes.
- Full Inline + AOI_TT UI/page regression initially exposed a DDD boundary violation: SQL was located under `infrastructure/aoi_tt`.
- Moved raw ARRAY defect querying into shared infrastructure and kept the AOI_TT loader as an adapter/orchestrator.
- Boundary + focused regression: `65 passed`.
- Full adjacent regression: `298 passed, 14 warnings`; existing Excel COM fallback tests emit Windows fatal diagnostic text but the test process completes successfully.
- Python compile check passed. `git diff --check` only reports pre-existing trailing whitespace in the user-edited feature specification.
- Targeted changed-module coverage: 85% (`44 passed`).
- Playwright E2E passed on M678 / ARRAY / 11620 / TDSUM: default Total/O/L rendered 9 charts in one Expander; Total-only rendered 3 charts.
- Cold-cache browser verification exposed and resolved the stale `aoi_tt_report_v1` payload contract by bumping the page cache signature.
- Desktop visual inspection at 1920×1080 found no filter, Expander or three-column chart overflow.
- Domain lineage, data-source spec, glossary, architecture and ADR-0024 updated.
- Issue acceptance checklist closed; implementation is complete on `feat/aoi-tt-particle-size`.

## Test Results

| Test | Expected | Actual | Status |
|---|---|---|---|
| Particle loader RED | Missing loader must fail | `ModuleNotFoundError` | ✓ RED |
| Particle loader GREEN | Unique mapping prevents count multiplication | 1 passed | ✓ |
| Particle core GREEN | Total/O/L compose and aggregate independently | 12 passed | ✓ |
| Particle service GREEN | Decorated Total expands to O/L | 10 passed | ✓ |
| Particle UI GREEN | Default selector and same-Expander rendering | 13 passed | ✓ |
| Particle page GREEN | Selected size reaches report renderer | 1 passed | ✓ |
| DDD boundary regression | AOI_TT adapter owns no SQL | 65 passed | ✓ |
| Inline adjacent regression | No feature regression | 298 passed | ✓ |
| Targeted coverage | Changed modules ≥80% | 85% | ✓ |
| Playwright E2E | Total/O/L same Expander; Total-only isolation | 9 charts → 3 charts | ✓ |
| Ruff correctness | No undefined/invalid Python constructs | E/F pass | ✓ |

## Error Log

| Error | Attempt | Resolution |
|---|---:|---|
| `rtk rg` PowerShell pattern split during prior analysis | 1 | Use `Select-String` or safer quoting for subsequent targeted searches |
| Particle loader test imports missing module | 1 | Expected RED; implement minimal public loader next |
| SQLite rejected pandas `Timestamp` bind | 1 | Bind native Python `datetime`, compatible with PostgreSQL and SQLite contract tests |
| Page test touched enterprise workbook and emitted COM fatal trace | 1 | Isolated the page test by replacing decision-signature and alert-workbook readers |
| Full regression rejected SQL under AOI_TT adapter | 1 | Moved the query to shared ARRAY defect data access; adapter now only applies AOI_TT policy |
| E2E initially rendered Total-only | 1 | Bumped AOI_TT page cache signature so v1 payload cannot mask the new dimension |
| Plotly count observed 8 while mounting | 1 | Wait for all 9 plot nodes before asserting |
| Particle tag click intercepted by sticky toolbar | 1 | Use the multiselect's documented Backspace removal behavior |

## 5-Question Reboot Check

| Question | Answer |
|---|---|
| Where am I? | Complete |
| Where am I going? | User handoff |
| What's the goal? | Add Total/O/L Particle Size reporting without breaking Total |
| What have I learned? | See `findings.md` |
| What have I done? | Implemented, documented and verified the full vertical slice |
