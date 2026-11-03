# Findings

- CodeGraph absent; follow scoped source searches. Installed Streamlit has bundled docs.
- Report first five entries are P0-A shared input identity, P0-B coverage/ledger reads, P0-C month keys, P1-A lazy details/macro-first, P1-B lightweight metadata/chart indexes.
- Previous alert changes are uncommitted but authorized and must remain: weekly-only trends, previous complete ISO-week Lot reads/publication selection.
- Domain YAML currently has only dashboard threshold and annual-period policy; introduce one explicit opt-in Array-time flag default disabled, retaining query and sort implementation.
- Current service functions already cache read_only and full config/date/resource keys; preserve this surface and add per-request memoization to avoid repeated pickle loads inside nested calls.
- Header already has per-indicator product revisions and separate cache/data refresh. Include a dedicated report-source revision in this page's refresh scope; use it for shared calculations and manual resource context. Keep trend/Lot/Sheet display revisions for publication/chart identities.
- Streamlit 1.60 supports native expander state tracking; no browser custom component or server required for AppTest interaction verification.
