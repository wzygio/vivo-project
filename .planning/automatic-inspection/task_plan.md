# Automatic inspection implementation plan

Mode: complex. Source: ../../.scratch/automatic-inspection/PRD.md.
Authorization: user requested full implementation through E2E and exact scheduler updates.

- Requirements/design: complete; current producers and dashboard inventory verified.
- Plan: complete; implement independent adapters in parallel, central runner and migration,
  fixture regressions, real production verification, scheduling readback, docs/review.
- Development/testing: complete; real40/40fullproduction and gatedHTTP/SMTP passed.
- Durable record/delivery: complete; guide, routes andADR0044 match verified behavior.

Expected modules: spc,ctq,aoi_tt,aoi_rs,yield,qtime,ijp_aa,ijp_hole,parts,iqc_evaporation.
No narrowing to API sources. All expected outputs and targets must be audited at end.

Potential hazards: stale empty results, source fallback, protected Excel COM, user flags,
7:15→7:20 race, task overlap, credentials in logs, heavy full-product cold production.

Validation complete:496broadregressions plus25additionaltests passed, inspection/
scheduledentrycoverage82%. Source freshness/readonly/SPCproof and partsSQL fixes
have review and genuine production evidence. Fourth full run succeeded40/40 in
774.97s; gatedmail waited, then APIcomplete2records andSMTPaccepted1message to
the authorized test recipient. Exact daily/weekly task definitions read back;
all12fixturelisteners released andalltrackedproduction/mailprocesschecks terminated.
Finalevidence: output/test-results/automatic-inspection/final-full-run.json,
positive-scheduled-mail.json,final-cleanup-verification.json; guide/ADR0044 updated.
Delivery stays local on feat/alert-notification; Git publication/merge not requested.
