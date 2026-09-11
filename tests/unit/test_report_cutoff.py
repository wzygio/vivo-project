import pandas as pd
import pytest

from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.report_cutoff import ReportCutoffPolicy


@pytest.mark.parametrize("offset", [0, 4])
def test_latest_day_is_clock_based_and_history_keeps_afternoon(offset):
    policy = ReportCutoffPolicy()
    display = pd.to_datetime([
        "2026-09-04 23:59:59", "2026-09-10 18:00:00",
        "2026-09-11 11:59:59", "2026-09-11 12:00:00",
        "2026-09-11 12:00:00.000001", "2026-09-11 23:59:59",
    ], format="mixed")
    source = pd.DataFrame({"time": display - pd.Timedelta(days=offset)})
    original = source.copy(deep=True)
    forward = DataForwardPolicy(enabled=bool(offset), offset_days=offset)
    shifted = forward.shift_frame(source, ["time"])
    actual = policy.filter_frame(shifted, "time", now="2026-09-11 08:00:00")
    assert actual.time.tolist() == display[:4].tolist()
    pd.testing.assert_frame_equal(source, original)
    tomorrow = policy.filter_frame(shifted, "time", now="2026-09-12")
    assert len(tomorrow) == len(source)


def test_sql_cap_keeps_historical_end_and_accepts_configuration():
    policy = ReportCutoffPolicy("10:30:00")
    assert policy.cap_end("2026-09-04 23:59:59", now="2026-09-11") == pd.Timestamp("2026-09-04 23:59:59")
    assert policy.cap_end("2026-09-11 23:59:59", now="2026-09-11") == pd.Timestamp("2026-09-11 10:30:00")


@pytest.mark.parametrize("value", [None, 12, "25:00:00", "12:00", "12:00:00+08:00"])
def test_invalid_cutoff_fails_explicitly(value):
    with pytest.raises(ValueError, match="HH:MM:SS"):
        ReportCutoffPolicy(value)


def test_config_and_clock_changes_invalidate_signature(monkeypatch):
    from src.shared_kernel.config import ConfigLoader

    settings = {"report_cutoff": {"latest_day_time": "12:00:00"}}
    monkeypatch.setattr(ConfigLoader, "_load_yaml", lambda path: settings)
    first = ConfigLoader.get_report_cutoff_policy().signature
    settings["report_cutoff"]["latest_day_time"] = "10:30:00"
    second = ConfigLoader.get_report_cutoff_policy().signature
    assert first != second
    today = pd.Timestamp.now().normalize()
    monkeypatch.setattr(ReportCutoffPolicy, "boundary", lambda self, now=None: today + pd.Timedelta(days=1, hours=10, minutes=30))
    assert ConfigLoader.get_report_cutoff_policy().signature != second


def test_cutoff_preserves_data_health_and_empty_contract():
    frame = pd.DataFrame({"time": pd.to_datetime(["2026-09-11 13:00:00"])})
    frame.attrs["data_health"] = {"status": "stale"}
    result = ReportCutoffPolicy().filter_frame(frame, "time", now="2026-09-11")
    assert result.empty
    assert result.attrs == frame.attrs
    assert result.time.dtype == frame.time.dtype
