# Project Context

## Purpose

天柱专项报表系统是面向 OLED/Array 半导体显示制造的良率、SPC、关键备件和自动预警报表系统。

## Operating Model

- Presentation lives under `app/` as Streamlit pages, sections, components, and chart adapters.
- Domain logic lives under `src/` and follows the Application -> Core -> Infrastructure layering described in `ARCHITECTURE.md`.
- Stable project knowledge should be routed through `AGENTS.md`, `docs/design/index.md`, `docs/plans/index.md`, `docs/references/`, `docs/generated/`, and `specs/`.
- Rebuildable outputs belong under `output/`; logs belong under `logs/`; source business fixtures belong under `resources/` or `docs/project_files/` when intentionally preserved.

## Hard Boundaries

- Do not refactor verified Yield concentration and Mapping algorithms without a specific task and regression proof.
- Do not change the `DatabaseManager` singleton/retry behavior casually.
- Do not remove `@st.cache_data` from page-facing flows.
- Do not simplify the Parquet snapshot refresh and degradation strategy without an approved plan.

## Fast Routing

- Architecture and module boundaries: `ARCHITECTURE.md`
- Design documents: `docs/design/index.md`
- Work plans: `docs/plans/index.md`
- Rebuildable scans and audits: `docs/generated/`
- External/vendor references: `docs/references/`
- User-maintainable contracts and task specs: `specs/`
