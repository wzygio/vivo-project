"""Cold processes share result files; concurrent publishers retain both products."""
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore

ROOT = Path(__file__).resolve().parents[2]


def test_concurrent_publishers_and_cold_reader(tmp_path):
    worker = tmp_path / "publish.py"
    worker.write_text('''from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore
root, product = Path(sys.argv[1]), sys.argv[2]
service = YieldAlertResults(YieldAlertWorkbookStore({"lot":root/"lots.xlsx", "trend":root/"trends.xlsx"}),
    policy_signature=lambda product:"fixture", ttl_seconds=43200)
day = date.today()
for kind in ("lot", "trend"):
    service.publish(product, kind, [], reference_date=day, coverage_start=str(day-timedelta(days=30)),
        coverage_end=str(day), has_data=True, health={"status":"fresh", "refreshed_at":datetime.now().astimezone().isoformat()},
        calculation_signature="fixture")
''', encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT/"src")))}
    children = []
    try:
        for product in ("P1", "P2"):
            children.append(subprocess.Popen([sys.executable, str(worker), str(tmp_path), product],
                                             cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        for child in children:
            _, errors = child.communicate(timeout=40)
            assert child.returncode == 0, errors.decode(errors="replace")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)
    reader = tmp_path / "read.py"
    reader.write_text('''from datetime import date
import json
from pathlib import Path
import sys
import pandas as pd
from src.yield_domain.application.alert_results import YieldAlertResults
from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore
from app.sections.inline_domain.monitor.alert_matrix_service import AlertMatrixContext, MATRIX_ROW_MAP
def fail(*args, **kwargs): raise AssertionError("SQL must not run")
pd.read_sql=fail
root=Path(sys.argv[1])
service=YieldAlertResults(YieldAlertWorkbookStore({"lot":root/"lots.xlsx","trend":root/"trends.xlsx"}),
    policy_signature=lambda product:"fixture", ttl_seconds=43200)
ctx=AlertMatrixContext(date.today(), yield_result_loader=lambda product,kind:service.read(product,kind,date.today()))
print(json.dumps([MATRIX_ROW_MAP[key].evaluator(product,ctx)["state"]
    for key in ("yield_lot_oos","yield_trend_fluctuation") for product in ("P1","P2")]))
''', encoding="utf-8")
    completed = subprocess.run([sys.executable, str(reader), str(tmp_path)], cwd=ROOT, env=env,
                               capture_output=True, text=True, timeout=40, check=True)
    assert json.loads(completed.stdout) == ["ok"] * 4


def test_warmup_publishes_file_only_yield_statuses(tmp_path, monkeypatch):
    from app.sections.inline_domain.monitor import alert_matrix_cache as cache
    from app.sections.inline_domain.monitor import alert_matrix_service as matrix
    from app.sections.inline_domain.monitor.alert_matrix_snapshot import DailyMatrixSnapshot
    from src.yield_domain import composition
    from tools.warm_alert_matrix import main
    service = YieldAlertResults(YieldAlertWorkbookStore({"lot":tmp_path/"lots.xlsx", "trend":tmp_path/"trends.xlsx"}),
                               policy_signature=lambda product: "fixture", ttl_seconds=43200)
    day = date.today()
    for kind in ("lot", "trend"):
        service.publish("P1", kind, [], reference_date=day, coverage_start=str(day-timedelta(days=30)),
            coverage_end=str(day), has_data=True, health={"status":"fresh", "refreshed_at":datetime.now().astimezone().isoformat()},
            calculation_signature="fixture")
    monkeypatch.setattr(composition, "build_yield_alert_results", lambda: service)
    monkeypatch.setattr(matrix, "MATRIX_ROWS", tuple(matrix.MATRIX_ROW_MAP[key]
        for key in ("yield_lot_oos", "yield_trend_fluctuation")))
    cache._cached_alert_matrix_cell.clear()
    store = DailyMatrixSnapshot(tmp_path / "daily.json")
    assert main([], loader=lambda **kwargs: cache.get_cached_alert_matrix(products=["P1"],
        _snapshot_store=store, **kwargs), store=store, log_dir=tmp_path/"logs") == 0
    assert {cell["state"] for cell in store.read(day, 43200)["cells"].values()} == {"ok"}
    cache._cached_alert_matrix_cell.clear()
