# 02: Inline四类OOS/OOC与Sheet/Lot
Status: implemented-verified
Blocked by: 01

- [x] CTQ/TT/RS与SPC共同服务，OOS/OOC明确区分。
- [x] RS保持sheet/lot身份，TT分类、日期/参数政策保持。
- [x] True/False/Delete/非法、缺失/损坏/空表有隔离测试。
- [x] 不以明细条数冒充异常片数，保留前导零及周边界。

Evidence: API file/HTTP integration, domain release/metadata tests and targeted126 checks passed; finalproject regression and merge tracked by04.
