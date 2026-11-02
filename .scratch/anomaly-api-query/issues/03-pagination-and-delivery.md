# 03: Partial results, global pagination and delivery

Status: in-progress
Blocked by: 02
Spec: ../PRD.md

Return successful sources with explicit safe failure markers, globally order/page their public records and invalidate cursors when effective scope, date, release result or failed sources change. Document usage, cursor and possible future fields without adding business fields.

- [x] Partial success is marked complete=false with product/category/event failures.
- [x] All-failed returns safe503 and authorized failed-source identities.
- [x] Empty but available sources are distinguished from failure.
- [x] Cursor binds effective scopes and failure state, never internal signatures.
- [x] Guarded regression, type checks and Standards/Spec review provide evidence.
- [ ] Preserve unrelated edits; deliver only to authorized branch/service.

## Comments
079b8822 closes Standards1/Spec2 findings (one overlapping timezone issue). Broader focused355 pass/1 AppTest timeout, selected13-file coverage93.564%; final full2808 pass/25 fail, tracked24 nodes unchanged plus1 transient Windows tmp-file sharing failure. Both transient cases' full modules rechecked serially24 pass. Original JUnit and differences preserved. Extended Pyright0/0. Merge/deployment remains before completion.
