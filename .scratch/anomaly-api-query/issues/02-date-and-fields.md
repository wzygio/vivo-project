# 02: Date window and business-field filters

Status: complete
Blocked by: 01
Spec: ../PRD.md

Allow optional start/end Shanghai dates inclusive, each defaulting to the prior ISO week's corresponding boundary; filter station, layer and parameter by exact match. Forward dates through existing source selectors; preserve default report behavior and publication validity checks.

- [x] All source families honor selected dates, including events outside the default week.
- [x] Invalid dates/ranges fail422; single-boundary defaults are documented.
- [x] New filters combine with existing scope/entity filters without losing leading zeros.
- [x] Yield location remains null; Q-Time station/layer use origin.

## Comments
RED: all four Inline custom-window HTTP tests422; GREEN: all four pass after forwarding dates. Real Q-Time/Yield outside-week, coverage failure and filter tests pass. Aware-time boundary initially503, then passes after Shanghai normalization. Guarded focused245 pass;0 maintained-file hash changes.
