# 03: Partial results, global pagination and delivery

Status: in-progress
Blocked by: 02
Spec: ../PRD.md

Return successful sources with explicit safe failure markers, globally order/page their public records and invalidate cursors when effective scope, date, release result or failed sources change. Document usage, cursor and possible future fields without adding business fields.

- [ ] Partial success is marked complete=false with product/category/event failures.
- [ ] All-failed returns safe503 and authorized failed-source identities.
- [ ] Empty but available sources are distinguished from failure.
- [ ] Cursor binds effective scopes and failure state, never internal signatures.
- [ ] Guarded regression, type checks and Standards/Spec review provide evidence.
- [ ] Preserve unrelated edits; deliver only to authorized branch/service.
