"""Plan/apply Task0–Task2 using local facts; database access is never implicit."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src.inline_domain.application.monitor.capability_history_rebuild import closed_week_labels, compute_weekly_records
from src.inline_domain.application.monitor.capability_rebuild_provenance import (
    validate_rebuild_manifest, workbook_signature, write_rebuild_manifest,
)
from src.inline_domain.core.monitor.capability_trend import (
    HISTORY_KEYS, capability_history_for_storage, normalize_capability_history,
    parse_capability_history_column, rollup_calendar_history, week_owner_month,
)
from src.inline_domain.core.monitor.cpk_summary import capability_summary_columns
from src.inline_domain.infrastructure.monitor.capability_metadata_snapshot import CapabilityMetadataSnapshot
from src.inline_domain.infrastructure.monitor.capability_snapshot_source import SnapshotCapabilitySource
from src.inline_domain.infrastructure.monitor.capability_workbook_migration import split_summary_workbook
from src.inline_domain.infrastructure.monitor.cpk_latest_excel_store import CpkLatestExcelStore
from src.inline_domain.infrastructure.monitor.cpm_latest_excel_store import CpmLatestExcelStore
from src.inline_domain.infrastructure.monitor.excel_alarm_store import read_cached_alarm_workbook
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.snapshot_paths import inline_measurement_directory


def validate_weekly_scope(frame: pd.DataFrame, *, products: list[str], weeks: list[str], metric: str) -> pd.DataFrame:
    """Require every requested product/week, exactly once, before reuse/publication."""
    rows = normalize_capability_history(frame, metric=metric)
    if (not rows["周期类型"].eq("周度").all() or not rows["监控类型"].eq("SPC").all()
            or not rows["厂别"].eq("ALL").all()):
        raise ValueError("Prepared weekly data must contain all-factory SPC weeks only")
    expected = {(product, week) for product in products for week in weeks}
    actual = list(rows[["产品", "时间标签"]].itertuples(index=False, name=None))
    if len(actual) != len(expected) or set(actual) != expected:
        raise ValueError("Prepared weekly data does not cover every requested product/week exactly once")
    total, rate = capability_summary_columns(metric)[6:8]
    expected_rate = rows["达标项目数"] / rows[total].replace(0, 1)
    expected_rate = expected_rate.where(rows[total].ne(0), 1)
    if (rows[rate] - expected_rate).abs().gt(1e-12).any():
        raise ValueError("Prepared weekly rates disagree with project counts")
    return rows


def anomalous_history_weeks(sheets: dict[str, pd.DataFrame], *, metric: str,
                            base_weeks: list[str], products: list[str],
                            as_of: object, include_missing_coverage: bool = True) -> dict[str, list[str]]:
    """Select only closed historical week rows with an insufficient denominator.

    Selection is per product: a bad week for one product does not authorize
    recalculating that week for every other product. Blank/zero totals are not
    by themselves anomalies when the maintained warning count is also zero.
    Missing stored weeks within an already-anomalous month are explicit
    coverage prerequisites: their values must be computed, never assumed zero.
    """
    total_column = capability_summary_columns(metric)[6]
    result: dict[str, list[str]] = {}
    today = pd.Timestamp(as_of).normalize()
    monday = today - pd.Timedelta(days=today.weekday())
    for name, original in sheets.items():
        if not name.endswith(f" {metric.upper()}"):
            continue
        product = name.removesuffix(f" {metric.upper()}").strip()
        if product not in products:
            continue
        frame = original.rename(columns={old: total_column for old in
            (f"{metric.upper()}总项目", f"{metric.upper()}总项数")
            if old in original and total_column not in original}).copy()
        if frame.empty:
            continue
        frame = frame.loc[frame["周期类型"].eq("周度")].copy()
        if frame.empty:
            continue
        if not frame["产品"].astype(str).eq(product).all():
            raise ValueError(f"Historical summary sheet contains another product: {name}")
        if frame["时间标签"].duplicated().any():
            raise ValueError(f"Historical summary contains duplicate weekly identities: {name}")
        totals = parse_capability_history_column(
            frame.get(total_column, pd.Series(pd.NA, index=frame.index)), total_column,
        ).fillna(0)
        warnings = parse_capability_history_column(
            frame.get("预警项目数", pd.Series(pd.NA, index=frame.index)), "预警项目数",
        ).fillna(0)
        labels = []
        for label in frame.loc[warnings.gt(totals), "时间标签"].astype(str):
            week_owner_month(label)  # Validate the canonical ISO week identity.
            year, week = label.split("-W")
            start = pd.Timestamp.fromisocalendar(int(year), int(week), 1)
            if label not in base_weeks and start < monday:
                labels.append(label)
        if labels:
            if include_missing_coverage:
                from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
                retained = set(frame["时间标签"].astype(str))
                for month in {week_owner_month(label) for label in labels}:
                    labels.extend(label for label in CpkWorkbookMonitorService._owned_completed_weeks(month, today)
                                  if label not in retained and label not in base_weeks)
            result[product] = sorted(labels)
    return result


def validate_anomalous_scope(frame: pd.DataFrame, *, expected: dict[str, list[str]],
                             metric: str) -> pd.DataFrame:
    """Require exactly the explicitly authorized sparse product/week keys."""
    rows = normalize_capability_history(frame, metric=metric)
    actual = set(rows[["产品", "时间标签"]].itertuples(index=False, name=None))
    wanted = {(product, week) for product, weeks in expected.items() for week in weeks}
    if actual != wanted or len(rows) != len(wanted):
        raise ValueError("Anomalous history results exceed or miss the authorized product/week scope")
    if not wanted:
        return rows
    checked = [validate_weekly_scope(rows.loc[rows["产品"].eq(product)], products=[product],
                                    weeks=weeks, metric=metric)
               for product, weeks in sorted(expected.items())]
    return pd.concat(checked, ignore_index=True)


def plan_history(sheets: dict[str, pd.DataFrame], weekly: pd.DataFrame, *, metric: str,
                 as_of: object, additional_weekly: pd.DataFrame | None = None) -> pd.DataFrame:
    """Repair fields and calendar baselines, then replace precisely selected weeks.

    Contradictory pre-existing counts fail explicitly: filling missing totals
    does not authorize discarding known alerts or inventing historical totals.
    """
    suffix = f" {metric.upper()}"
    total_column, rate_column = capability_summary_columns(metric)[6:8]
    aliases = {f"{metric.upper()}总项目": total_column, f"{metric.upper()}总项数": total_column,
               f"{metric.upper()}≥1.33达标率": rate_column}
    frames = [frame.rename(columns={old: new for old, new in aliases.items()
                                  if old in frame and new not in frame}).copy()
              for name, frame in sheets.items() if name.endswith(suffix)]
    history = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=weekly.columns)
    additional = normalize_capability_history(pd.DataFrame(), metric=metric)
    if additional_weekly is not None:
        expected = anomalous_history_weeks(sheets, metric=metric,
            base_weeks=weekly["时间标签"].unique().tolist(),
            products=weekly["产品"].unique().tolist(), as_of=as_of)
        additional = validate_anomalous_scope(additional_weekly, expected=expected, metric=metric)
    updates = weekly if additional.empty else pd.concat([weekly, additional], ignore_index=True)
    # Replacement weeks and their fully covered monthly parents supersede old
    # incomplete values before validation. Old quarter/year cells are outputs
    # of Task0, not authoritative inputs to that same aggregation.
    for column, default in (("监控类型", "SPC"), ("厂别", "ALL")):
        if column not in history:
            history[column] = default
    new_keys = pd.MultiIndex.from_frame(updates[HISTORY_KEYS])
    old_keys = pd.MultiIndex.from_frame(history[HISTORY_KEYS])
    history = pd.concat([history.loc[~old_keys.isin(new_keys)], updates], ignore_index=True)
    months = updates.assign(_month=updates["时间标签"].map(week_owner_month))
    monthly = history.loc[history["周期类型"].eq("月度"), [*HISTORY_KEYS[:3], "时间标签"]]
    replacement_months = months[[*HISTORY_KEYS[:3], "_month"]].rename(columns={"_month": "时间标签"})
    monthly = pd.concat([monthly, replacement_months], ignore_index=True).drop_duplicates()
    quarter_keys = {(*scope, f"{month[:4]}-Q{(int(month[-2:]) - 1) // 3 + 1}")
                    for *scope, month in monthly.itertuples(index=False, name=None)}
    year_keys = {(*key[:3], key[3][:4]) for key in quarter_keys}
    remove = [(kind == "季度" and (*scope, label) in quarter_keys)
              or (kind == "年度" and (*scope, label) in year_keys)
              for *scope, kind, label in history[HISTORY_KEYS].itertuples(index=False, name=None)]
    history = history.loc[~pd.Series(remove, index=history.index)].copy()
    for column in (total_column, "达标项目数", "预警项目数", rate_column):
        if column not in history:
            history[column] = pd.NA
        history[column] = parse_capability_history_column(history[column], column, rate=column == rate_column)
    history[total_column] = history[total_column].fillna(0)
    history["达标项目数"] = history["达标项目数"].fillna(history[total_column])
    history["预警项目数"] = history["预警项目数"].fillna(history[total_column] - history["达标项目数"])
    from src.inline_domain.application.monitor.cpk_workbook_service import CpkWorkbookMonitorService
    for key, _ in months.groupby(["产品", "监控类型", "厂别", "_month"]):
        product, scope, factory, month = key
        expected = CpkWorkbookMonitorService._owned_completed_weeks(month, pd.Timestamp(as_of))
        selected = history.loc[history["产品"].eq(product) & history["监控类型"].eq(scope)
                               & history["厂别"].eq(factory) & history["周期类型"].eq("周度")
                               & history["时间标签"].isin(expected)]
        full_calendar = set(expected).issubset(set(selected["时间标签"]))
        if not full_calendar and product != "Z622":
            raise ValueError(f"Complete monthly weekly history required for {product}/{month}")
        total, warning = int(selected[total_column].sum()), int(selected["预警项目数"].sum())
        month_row = {"产品": product, "监控类型": scope, "厂别": factory, "周期类型": "月度",
                     "时间标签": month, total_column: total, "预警项目数": warning,
                     "达标项目数": total - warning, rate_column: (total - warning) / total if total else 1.0}
        old = (history["产品"].eq(product) & history["监控类型"].eq(scope) & history["厂别"].eq(factory)
               & history["周期类型"].eq("月度") & history["时间标签"].eq(month))
        history = pd.concat([history.loc[~old], pd.DataFrame([month_row])], ignore_index=True)
    contradiction = history["预警项目数"].gt(history[total_column])
    if contradiction.any():
        keys = history.loc[contradiction, ["产品", "周期类型", "时间标签"]].to_dict("records")
        raise ValueError(f"Historical totals unavailable outside replacement scope: {keys}")
    # Known total/warning counts establish qualified counts independently of
    # rounded historical rates. No additional denominator is inferred.
    history["达标项目数"] = history[total_column] - history["预警项目数"]
    history[rate_column] = history["达标项目数"] / history[total_column].replace(0, 1)
    history.loc[history[total_column].eq(0), rate_column] = 1.0
    history = rollup_calendar_history(normalize_capability_history(history, metric=metric, repair_missing=True), metric=metric)
    return history


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-workbook", type=Path, required=True)
    parser.add_argument("--as-of", default=pd.Timestamp.today().date().isoformat())
    parser.add_argument("--output", type=Path, default=ROOT / "output/test-results/capability-summary")
    parser.add_argument("--prepared-weekly", type=Path,
                        help="Reuse reviewed cpk-weekly.parquet/cpm-weekly.parquet from a prior calculation")
    parser.add_argument("--repair-anomalous-history", action="store_true",
                        help="Recompute only anomalous older product/weeks and missing owning-month coverage")
    parser.add_argument("--extra-snapshot-directory", type=Path,
                        help="Explicit isolated historical inputs for authorized weeks; never queries a database")
    parser.add_argument("--prepared-anomalous", type=Path,
                        help="Reuse version-bound sparse historical results stored under product directories")
    parser.add_argument("--apply", action="store_true", help="Back up and split only after all plans validate")
    args = parser.parse_args()
    if not args.repair_anomalous_history and (args.extra_snapshot_directory or args.prepared_anomalous):
        parser.error("Historical inputs require --repair-anomalous-history")
    args.output.mkdir(parents=True, exist_ok=True)
    source = args.source_workbook.resolve()
    stat = source.stat()
    sheets = read_cached_alarm_workbook(str(source), stat.st_mtime_ns, stat.st_size)
    baseline_signature = workbook_signature(sheets)
    products = ConfigLoader.get_enabled_products()
    as_of = pd.Timestamp(args.as_of).normalize()
    first_month = as_of.replace(day=1) - pd.DateOffset(months=2)
    totals_source = SnapshotCapabilitySource(
        inline_measurement_directory(ROOT / "data"), CapabilityMetadataSnapshot(),
    )
    weeks_by_metric = {"cpk": closed_week_labels(as_of, count=5),
                       "cpm": closed_week_labels(as_of, from_month=first_month)}
    source_signature = totals_source.source_signature(products)
    ledger_path = ConfigLoader.get_domain_resource_path("inline_domain", "spc_cpk_cpm_decoration")
    ledger_stat = ledger_path.stat()
    ledger = read_cached_alarm_workbook(str(ledger_path.resolve()), ledger_stat.st_mtime_ns, ledger_stat.st_size)
    ledger_signature = workbook_signature(ledger)
    provenance = dict(as_of=as_of, products=products, weeks_by_metric=weeks_by_metric,
                      source_signature=source_signature, ledger_signature=ledger_signature)
    if args.prepared_weekly:
        validate_rebuild_manifest(args.prepared_weekly, **provenance)
    weekly_results = {}
    for metric, reader_type in (("cpk", CpkLatestExcelStore), ("cpm", CpmLatestExcelStore)):
        weeks = weeks_by_metric[metric]
        if args.prepared_weekly:
            weekly = pd.read_parquet(args.prepared_weekly / f"{metric}-weekly.parquet")
        else:
            reader = reader_type(ledger_path, workbook_reader=lambda *_args: ledger)
            weekly = compute_weekly_records(totals_source, reader, products=products,
                                            weeks=weeks, metric=metric, as_of=as_of)
        weekly = validate_weekly_scope(weekly, products=products, weeks=weeks, metric=metric)
        weekly_results[metric] = weekly
        if not args.prepared_weekly:
            weekly.to_parquet(args.output / f"{metric}-weekly.parquet", index=False)
    additional_scope = {metric: anomalous_history_weeks(sheets, metric=metric,
        base_weeks=weeks, products=products, as_of=as_of) if args.repair_anomalous_history else {}
        for metric, weeks in weeks_by_metric.items()}
    additional_results = {metric: [] for metric in weeks_by_metric}
    pending_extra_manifests = []
    extra_source = totals_source
    authorized = {}
    for per_product in additional_scope.values():
        for product, weeks in per_product.items():
            authorized[product] = sorted(set(authorized.get(product, [])) | set(weeks))
    if authorized and args.extra_snapshot_directory is not None:
        from src.inline_domain.infrastructure.monitor.capability_historical_source import HistoricalCapabilitySource
        extra_source = HistoricalCapabilitySource(totals_source, args.extra_snapshot_directory, authorized)
    for product in sorted(authorized):
        product_scope = {metric: per_product[product] for metric, per_product in additional_scope.items()
                         if product in per_product}
        extra_signature = extra_source.source_signature([product])
        extra_provenance = dict(as_of=as_of, products=[product], weeks_by_metric=product_scope,
                                source_signature=extra_signature, ledger_signature=ledger_signature)
        directory = (args.prepared_anomalous if args.prepared_anomalous else args.output / "anomalous") / product
        if args.prepared_anomalous:
            validate_rebuild_manifest(directory, **extra_provenance)
        else:
            directory.mkdir(parents=True, exist_ok=True)
        for metric, weeks in product_scope.items():
            if args.prepared_anomalous:
                rows = pd.read_parquet(directory / f"{metric}-weekly.parquet")
            else:
                reader_type = CpkLatestExcelStore if metric == "cpk" else CpmLatestExcelStore
                reader = reader_type(ledger_path, workbook_reader=lambda *_args: ledger)
                rows = compute_weekly_records(extra_source, reader, products=[product],
                                               weeks=weeks, metric=metric, as_of=as_of)
            rows = validate_weekly_scope(rows, products=[product], weeks=weeks, metric=metric)
            additional_results[metric].append(rows)
            if not args.prepared_anomalous:
                rows.to_parquet(directory / f"{metric}-weekly.parquet", index=False)
        pending_extra_manifests.append((directory, product, extra_signature, extra_provenance))
    if totals_source.source_signature(products) != source_signature:
        raise RuntimeError("Capability inputs changed during historical calculation; recompute before publication")
    for _directory, product, signature, _provenance in pending_extra_manifests:
        if extra_source.source_signature([product]) != signature:
            raise RuntimeError("Historical capability inputs changed during calculation; recompute before publication")
    current_ledger_stat = ledger_path.stat()
    current_ledger = read_cached_alarm_workbook(str(ledger_path.resolve()), current_ledger_stat.st_mtime_ns,
                                               current_ledger_stat.st_size)
    if workbook_signature(current_ledger) != ledger_signature:
        raise RuntimeError("Published alert history changed during historical calculation; recompute before publication")
    if not args.prepared_weekly:
        write_rebuild_manifest(args.output, **provenance)
    if not args.prepared_anomalous:
        for directory, _product, _signature, extra_provenance in pending_extra_manifests:
            write_rebuild_manifest(directory, **extra_provenance)
    final_sheets, report = {}, {"as_of": str(as_of.date()), "source": str(source), "metrics": {},
                               "additional_history": additional_scope}
    failures = []
    for metric, weekly in weekly_results.items():
        weeks = weeks_by_metric[metric]
        try:
            additional = (pd.concat(additional_results[metric], ignore_index=True)
                          if additional_results[metric] else normalize_capability_history(pd.DataFrame(), metric=metric))
            history = plan_history(sheets, weekly, metric=metric, as_of=as_of,
                                   additional_weekly=additional if args.repair_anomalous_history else None)
        except ValueError as exc:
            failures.append({"metric": metric, "reason": str(exc), "applied": False})
            continue
        history.to_parquet(args.output / f"{metric}-planned-history.parquet", index=False)
        report["metrics"][metric] = {"weeks": weeks, "products": products, "rows": len(history)}
        for product, frame in history.groupby("产品", sort=False):
            final_sheets[f"{product} {metric.upper()}"] = capability_history_for_storage(frame, metric=metric)
    if failures:
        report["failures"] = failures
        (args.output / "rebuild-plan.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise ValueError(f"Historical baseline repair requires review: {failures}")
    if args.apply:
        parent = source.parent
        targets = {"alarm": parent / "北极星报警率汇总.xlsx", "cpk": parent / "北极星CPK汇总.xlsx",
                   "cpm": parent / "北极星CPM汇总.xlsx"}
        report["migration"] = split_summary_workbook(source, targets, revised_sheets=final_sheets,
                                                       expected_source_signature=baseline_signature)
    (args.output / "rebuild-plan.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
