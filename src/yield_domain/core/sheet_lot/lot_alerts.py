"""Shared Lot OOS selection for report publication and presentation."""
import pandas as pd


def compute_lot_oos_records(
    lot_data: dict | None, warning_lines: dict, time_period: int | None = 30,
) -> tuple[list[dict], int]:
    """扫描 Lot 超规记录。纯计算，无渲染。"""
    total_recent_lots = 0
    oos_records = []

    if lot_data and "code_level_details" in lot_data:
        all_dfs = []
        for df in lot_data["code_level_details"].values():
            if not df.empty and "lot_id" in df.columns and "warehousing_time" in df.columns:
                all_dfs.append(df)

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df["warehousing_time"] = pd.to_datetime(
                combined_df["warehousing_time"], format="%Y%m%d", errors="coerce"
            )
            max_date = combined_df["warehousing_time"].max()

            if pd.notna(max_date):
                threshold_date = (max_date - pd.Timedelta(days=time_period)
                                  if time_period is not None else combined_df["warehousing_time"].min())
                recent_df = combined_df[combined_df["warehousing_time"] >= threshold_date].copy()
                total_recent_lots = recent_df["lot_id"].nunique()

                for _, row in recent_df.iterrows():
                    code = str(row.get("defect_desc", "")).strip()
                    rate = row.get("defect_rate", 0.0)
                    spec_dict = warning_lines.get(code)
                    if not spec_dict:
                        continue
                    spec_limit = spec_dict.get("upper", 1.0)
                    if rate > spec_limit:
                        w_time = row.get("warehousing_time")
                        w_time_str = w_time.strftime("%Y/%m/%d") if pd.notna(w_time) else "-"
                        a_time = pd.to_datetime(row.get("array_input_time"), errors="coerce")
                        a_time_str = a_time.strftime("%Y/%m/%d") if pd.notna(a_time) else "-"
                        defect_count = int(row.get("defect_panel_count", 0)) if pd.notna(row.get("defect_panel_count")) else 0
                        spec_str = f"{(spec_limit or 0) * 100:.2f}%" if spec_limit < 1.0 else "无限制"
                        rate_str = f"{(rate or 0) * 100:.2f}%"
                        oos_records.append({
                            "超规 Lot ID": row.get("lot_id", "Unknown"),
                            "异常 Code": code,
                            "管控规格线": spec_str,
                            "实际不良率": rate_str,
                            "不良panel数": defect_count,
                            "入库时间": w_time_str,
                            "阵列投入时间": a_time_str,
                        })

    return oos_records, total_recent_lots


