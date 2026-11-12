"""Produce domain alerts, then warm the matrix: python -m api.inspection [--check]."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime
import json
from io import StringIO
import logging
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
# Existing domain imports include both src.foo and src-layout top-level packages.
for path in (ROOT, ROOT/'src'):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def main(argv=None) -> int:
    from api.inspection.application import MODULES, run_inspection
    from src.shared_kernel.config import ConfigLoader
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Validate scope only, without database or business writes')
    args = parser.parse_args(argv)
    try:
        products = tuple(ConfigLoader.get_enabled_products(strict=True))
        if not products:
            raise ValueError('Empty product scope')
        if args.check:
            print(json.dumps({'status':'checked', 'products':products, 'modules':MODULES,
                              'post_production':['matrix_warmup']}))
            return 0
        from api.inspection.composition import build_jobs
        from api.inspection.database import read_only_sessions
        day = datetime.now(ZoneInfo('Asia/Shanghai')).date()
        with read_only_sessions():
            report = run_inspection(products, day, build_jobs(products, day), ROOT/'output/logs/alert_inspection',
                                    overlap_lock=ROOT/'output/logs/alert_result_publication/publication-job')
        from api.inspection.matrix_warmup import main as warm_matrix
        with redirect_stdout(StringIO()):
            warmup_code = warm_matrix([], reference_date=day, products=products)
        success = report['status'] == 'success' and warmup_code == 0
        print(json.dumps({'status':'success' if success else 'partial',
                          'production_status':report['status'],
                          'matrix_warmup_exit_code':warmup_code, 'run_id':report['run_id'],
                          'completed':sum(row['status']=='success' for row in report['outcomes']),
                          'total':len(report['outcomes'])}))
        return 0 if success else 1
    except Exception:
        print(json.dumps({'status':'failed', 'code':'INSPECTION_FAILED'}))
        return 2


if __name__ == '__main__':
    os.chdir(ROOT)
    logging.basicConfig(handlers=[logging.NullHandler()], force=True)
    # Domain drivers may log raw SQL/connection exceptions. The owned receipt
    # retains fixed module/error codes instead of those external messages.
    logging.disable(logging.CRITICAL)
    raise SystemExit(main())
