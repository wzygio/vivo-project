# Findings

- No .codegraph. Existing shared app consumer and workbook caches depend on Streamlit; shared selection and injected read adapters now retain page compatibility.
- QTime already selects FlagFalse but dropped it at publication. Decision file identity is in publication policy. Keep one ledger, explicit evidence without public Flag fields.
- Yield Lot has no Flag/station/factory; cannot derive occurrence from10000 array-input join. Maintain attribution and description; only Yield station nullable, factory/layer required.
- Existing dictionary loader is unbounded and not persisted. New offline sync uses station allowlist, view→table fallback, parameterized query, row cap and timeout. API never calls it.
- Existing resource/config dirty state belongs to user. Never migrate or stage assets; append only api_alert_metadata registry key in global config and stage only that hunk.
- Four Inline OOC readers exist. Fixed previous ISO week; layer before first underscore; QTime f_step.
- HTTP timeout does not cancel Office calls. BoundedReads retains slots until underlying work finishes; no unbounded queue. Protected-file runtime still needs service-account/Office verification; synthetic tests forbid real COM.
- New-process signing key cannot distinguish a previous-process cursor from a tampered one: both422 and410 require first-page retry; documentation must say so.
