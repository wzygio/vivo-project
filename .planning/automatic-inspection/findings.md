# Verified findings

- Existing dashboard13 rows include4 Inline OOS,2 capability,2 Yield, Qtime,
  IJP AA/hole, parts and IQC. AOI trends independent published resources (produce too).
- Existing actual Yield/IJP task invokes tools publisher every30min; resources valid
  skipped; full production readback runs when expired. Source application shared.
- SPC and Qtime have independent tools but no matching scheduled production tasks.
- Inline readers can label stale-file empty selection ready; unified production
  requires completion evidence, not their display state alone.
- Existing notification has real SMTP/HTTP E2E and fixed test-recipient override.
- Existing worktree contains unrelated script/test edits and maintained workbook
  changes; preserve them. No blanket Git reset, cleanup or broad resource replacement.

- Review found3issues: Yield cached source mistaken currentrun, missing limits
  becoming empty-success, and SETdefaultreadonly not currenttxn. All fixed with
  real forcedrefresh/source-timestamp guards, nonemptylimitgate, DBAPI set_session.
  Actual SHOWreadonly=on; four Yield safety fixture tests passed.
- Current full live diagnostic run imported pre-fix Inline code; 7products Inline
  finished with only M626/M673 SPC failures. Other5SPC andall21CTQ/TT/RS succeeded.
  Yield production lazily imported fixed force-refresh module; first4Yield succeeded.
- Additional SPC diagnostic launcher37260 (output/.../live-spc-process.json),
  started11:33 withcurrentadapter/boundedreadback, onlyM626/M673SPC; no current
  unified Inline writes (main currentlyYield). Must inspect this handle before rerun.
- Broad git diff --check has unrelated existing tools/call_anomaly_api.py trailing
  whitespace; preserve user edit, scope final diff check to task-owned changes.
- Live SPC failures were isolated to physical protected-file size signature drift
  during Office activation and empty capability-sheet headers. The adapter retains
  logical rule/source invariants, rebinds the same fresh member population only when
  those invariants hold, and repairs headers only on empty sheets. Real Z517 SPC
  rerun succeeded; transient protected reads have one bounded retry.
- Parts native SELECT success must be distinguished from unrelated recovered
  metadata errors. The loader issues a fresh completion proof only after successful
  query and processing; the inspection checks its invocation time and current output.
  Actual production plus readback passed108.72s. No database writes were executed.
