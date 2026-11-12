"""One scheduled run: python -m api.notification [--send] [--test-mode]."""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

from api.notification.application import DeliveryFailure, run_notification
from api.notification.core import NotificationError
from api.notification.infrastructure import HttpAlertSource, SmtpDelivery
from api.notification.settings import load_environment, load_mail_credentials, load_settings
from tools.fetch_sheet_anomalies import ApiCallError, SafeArgumentParser

ROOT = Path(__file__).resolve().parents[2]

def main(argv: list[str] | None = None) -> int:
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=ROOT / 'config/domain/api_domain.yaml')
    parser.add_argument('--mail-config', type=Path, help='Explicit private reference-format credential YAML; never supplies recipients')
    parser.add_argument('--env-file', type=Path, help='Explicit API/mail environment file; process environment takes precedence')
    parser.add_argument('--send', action='store_true', help='Actually send; omitted means preview counts only')
    parser.add_argument('--test-mode', action='store_true', help='Override ALL recipients to wangzy03@visionox.com')
    args = parser.parse_args(argv)
    try:
        settings = load_settings(args.config)
        env = load_environment(args.env_file)
        delivery = SmtpDelivery(settings, load_mail_credentials(args.mail_config, environ=env)) if args.send else None
        report = run_notification(HttpAlertSource(settings, env['API_KEY'] or env['VIVO_API_KEY']), delivery, settings,
                                  test_mode=args.test_mode)
    except DeliveryFailure as exc:
        print(json.dumps({'status': 'failed', **asdict(exc.report), 'message': str(exc)}, ensure_ascii=False))
        return 2
    except (NotificationError, ApiCallError) as exc:
        print(json.dumps({'status': 'failed', 'message': str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps({'status': 'complete' if report.complete else 'partial', **asdict(report)}, ensure_ascii=False))
    return 0 if report.complete else 3

if __name__ == '__main__':
    raise SystemExit(main())
