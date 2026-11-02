# Sheet/Lot API development

Mode: complex
Spec: .scratch/sheet-anomaly-api/PRD.md
Approval: 用户2026-11-02确认任务图/边界/顺序；OOS+OOC、必需字段完整、QTime起点；后修订Yield厂别必需站点可NULL。

## Phases
- [x] Requirements: complete; approved spec and four ready-for-agent tickets.
- [x] Planning: complete; approved order 01→02→03→04; HTTP/file seams; isolated fixtures, dependency guard, regression, review.
- [ ] Development/testing: in_progress; tickets01-03 implemented with targeted green verification; frontier04 final guarded regression, review closure, scoped commit and merge gate. Same worktree, dedicated feat/sheet-anomaly-api branch; preserve prior dirty assets/config.
- [ ] Project record: pending; ADR draft then workflow approval/merge gate.

## Evidence and recovery
Read PRD, ticket statuses, findings.md, progress.md and Git before resuming. Prechange baseline52 passed; not proof of newAPI. No productionDB/workbook migration/service changes. Merge requires separate confirmation after concrete implementation/review.

Review base b494db8e23ef4e9df1dfb0aefeaf3e24b9fed0c5. Both Standards/Spec P2(empty malformed source accepted as zero) closed. Four fixture/compatibility failures fixed,126 relatedchecks passed. Final focused267 passed,92.37%coverage, Pyright0errors; full2732 cases=2708passed/24failed. Exact24failednodes reproduced with baseline source under sameconfig/guards, no new failingnodes. User must decide baseline-quality exception andmerge/ADR; do notclaimglobalgreen or fourphasecomplete.
