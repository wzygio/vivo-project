# Findings

- Existing authoritative OOC rule is Sheet Mean outside UCL/LCL with priority OOS > SOOS > OOC.
- Task1-1 OOS facts use extreme values for SPC/CTQ; Task1-1-1 therefore uses OOS > OOC and omits SOOS facts.
- SPC/CTQ prepared Sheet features already carry USL/LSL/UCL/LCL; no new query is needed.
- AOI_TT repository already projects real USL/UCL; no new query is needed.
- AOI_RS authoritative `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx` contract has only single-sided `spec`;
  it cannot produce real OOC without a future control-line source.
- Existing Excel persistence and Parquet overlap-replacement machinery is key-column/contract driven and
  can be reused for OOC with independent filenames and history directory.
- Production resource paths are partly hardcoded as `resource_dir / filename`; migration needs a shared
  scope locator while tests retain explicit directory overrides.
- Pre-existing dirty SPC/config/resource changes implement a separate CPK/CPM exemption rule and must
  be preserved and excluded from this task's commit unless overlap requires a compatible additive edit.
