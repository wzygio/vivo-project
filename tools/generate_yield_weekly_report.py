"""Generate the previous ISO week's Yield PowerPoint report."""
import argparse
import logging
from pathlib import Path
import sys
from tempfile import NamedTemporaryFile

ROOT = Path(__file__).resolve().parents[1]
for directory in (ROOT, ROOT / "src"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

from app.sections.yield_domain.weekly_export import generate_weekly_yield_export


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output" / "reports",
                        help="Directory for the generated PPTX")
    args = parser.parse_args()
    try:
        report = generate_weekly_yield_export()
        args.output_dir.mkdir(parents=True, exist_ok=True)
        target = args.output_dir / report.filename
        with NamedTemporaryFile(dir=args.output_dir, suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(report.data)
                stream.close()
                temporary.replace(target)
            finally:
                stream.close()
                temporary.unlink(missing_ok=True)
    except Exception:
        logging.exception("Yield weekly PPT generation failed")
        return 1
    print(f"{report.week.label} | {len(report.products)} products | {target.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
