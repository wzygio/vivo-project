# Findings

- No `.codegraph/`; scoped source search applies.
- SPC alerts currently consume in-memory decoration, matrix state ExcelAlarmReader, matrix detail another workbook reader. Only SPC display passes PPA exclusions; matrix signature omits policy.
- Inline generated workbooks have product detail, __flags and __refresh_meta__; persistence may skip within TTL while returning freshly merged memory data. Excel must be the authoritative consumer input.
- ExcelAlarmReader projects away source keys; a shared public normalized detail read is needed for common display/selection.
- Existing files have user changes in matrix and published snapshot logic, including initialized-empty no_data green behavior. Preserve them.
- IJP/parts/IQC use matrix-owned saved snapshot reader; reports should share this reader via a neutral app assembly module. Q-Time requires a saved-result producer/consumer addition.
- Shared process cache means same computation cache and file parse, not same mutable DataFrame identity; scheduled separate processes share files only.

- Completed shared display for all13 matrix rows and the AOI report-only density warnings. Default matrix context only injects shared read_alert_result; legacy fake context ports remain for isolated testing.
- Q-Time/Yield report selection windows are explicit kwargs, preserving previous rolling/30-day scope; matrix defaults remain prior ISO week. Sharing display does not imply replacing producer or chart pipelines.
- Enterprise-protected OOS workbooks without new detail_signature migrate on the next normal write, preserving prior TTL and bytes; newly signed outputs detect real content changes within TTL.
