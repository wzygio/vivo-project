"""Prepare in-memory upload bytes and verify the admin E2E downloaded ledger."""

import base64
from io import BytesIO
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

import pandas as pd
from fr_file_decryption import decrypt_file

OUTPUT = Path(__file__).resolve().parents[3] / 'output/test-results/qtime-chamber'


def prepare() -> None:
    records = [dict(prodcode=product, step_desc='M3_DE->M3_STR', lot_id='L3MY67005AA',
                    timekey='20260802020000', flag=True) for product in ('M626', 'M678')]
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        pd.DataFrame(records).to_excel(writer, sheet_name='决策台账', index=False)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / 'admin-decision-upload.json').write_text(
        json.dumps({'base64': base64.b64encode(buffer.getvalue()).decode('ascii')}), encoding='utf-8',
    )


def verify_downloads() -> None:
    for name, expected in [('before', False), ('after', True)]:
        with TemporaryDirectory(prefix='admin-ledger-', dir=OUTPUT) as directory:
            result = decrypt_file(OUTPUT / f'admin-{name}.xlsx', output_dir=directory, strategy='excel-values')
            ledger = pd.read_excel(result.output_path, sheet_name='决策台账')
        selected = ledger.loc[ledger['lot_id'].eq('L3MY67005AA')]
        assert len(selected) == 2 and selected['flag'].eq(expected).all(), (name, selected.to_dict('records'))
    print('PASS: downloaded flags changed from False to True for both products')


if __name__ == '__main__':
    if '--verify-downloads' in sys.argv:
        verify_downloads()
    else:
        prepare()
