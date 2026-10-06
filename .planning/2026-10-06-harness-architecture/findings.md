# Findings

- Named capabilities: Inline aoi_rs/aoi_tt/ctq/spc/monitor/shared; Indicator qtime/ijp/ijp_hole; IQC eva_materials/lifetime; Yield mapping/mwd_trend/sheet_lot. Equipment has an integrated parts capability without named source subpackages.
- Resource paths currently live in domain YAML; Yield additionally constructs paths from product FileResource names and product directories. IQC example resources use a hardcoded directory.
- Existing user changes include root architecture/Harness/router documents, the task spec, chamber presentation/tests, Inline configuration and resource workbooks. Preserve them throughout migration.
- HARNESS contains unique disclosure/ownership/lifecycle/validation rules; its repeated route table can be absorbed into CONTEXT. Decide retirement after transferring these rules.

- Manifest inventories 195 assets and 170 moves. All 116 resources and 99 unchanged data files match original SHA256, including six pre-existing modified SPC files and ignored historical resources.
- Global registry includes 119 file entries and two collection directory entries. Three optional inputs remain absent: QTime manual decisions and two IQC demo JSON files.
- Equipment assets use parts; code stays ungrouped. Existing data lifecycle groups were reviewed and retained, rather than forcing physical source/data moves.
- CONTEXT retains unique business constraints and therefore remains necessary. HARNESS's effective rules are now in CONTEXT, and the duplicate file has been retired. ADR-0033 replaces only ADR-0027 resource configuration ownership.
- Live links in changed roots, moved documents, knowledge and ADRs resolve. Five missing long_text attachments remain in the untouched historical docs/project_files source document; they are outside this migration and were not fabricated.
- Reference filename grammar is unchanged. Old directory spelling/case is corrected; broken old source routes were resolved against actual files, and obsolete nonexistent trend_regulator claims were removed from the Yield overview.
