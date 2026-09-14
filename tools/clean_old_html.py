#!/usr/bin/env python3
"""Offline HTML cleanup, Python 3.8+, standard library only.

Default: preview root/child/*.html and root/child/Download/*.html.
Use --delete for permanent deletion; --recursive explicitly includes deeper paths.
"""
import argparse
from datetime import datetime
import math
import os
from pathlib import Path
import stat
import sys
import time
from typing import Iterator


def previous_month_start(now: datetime) -> datetime:
    """First day of previous calendar month, in the machine's local time."""
    year, month = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    return datetime(year, month, 1)


def is_link(info: os.stat_result) -> bool:
    # Windows junctions and other reparse points must not redirect traversal.
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_file_attributes', 0) & 0x400)


def safe_candidate(path: Path, root: Path, cutoff: float) -> bool:
    """Recheck scope, ancestors, type and mtime immediately before unlink."""
    relative = path.relative_to(root)
    if len(relative.parts) < 2 or path.suffix.lower() != '.html':
        return False
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        info = current.lstat()
        if is_link(info) or not stat.S_ISDIR(info.st_mode):
            return False
    info = path.lstat()
    return not is_link(info) and stat.S_ISREG(info.st_mode) and info.st_mtime < cutoff


class Scan:
    def __init__(self) -> None:
        self.errors = 0
        self.entries = 0
        self.last_report = time.monotonic()

    def error(self, path: Path, exc: OSError) -> None:
        self.errors += 1
        print('ERROR: {}: {}'.format(path, exc), file=sys.stderr, flush=True)

    def files(self, directory: Path, recursive: bool, depth: int = 0) -> Iterator[Path]:
        try:
            if is_link(directory.lstat()):
                return
            with os.scandir(directory) as entries:
                for entry in entries:
                    self.entries += 1
                    if time.monotonic() - self.last_report >= 5:
                        print('Scanning: {} entries checked'.format(self.entries), flush=True)
                        self.last_report = time.monotonic()
                    path = Path(entry.path)
                    try:
                        info = entry.stat(follow_symlinks=False)
                        if is_link(info):
                            continue
                        if stat.S_ISDIR(info.st_mode):
                            if depth == 0 or recursive or (depth == 1 and entry.name.lower() == 'download'):
                                yield from self.files(path, recursive, depth + 1)
                        elif depth > 0 and stat.S_ISREG(info.st_mode) and path.suffix.lower() == '.html':
                            yield path
                    except OSError as exc:
                        self.error(path, exc)
        except OSError as exc:
            self.error(directory, exc)


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number


def nonnegative_seconds(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError('must be a finite, nonnegative number')
    return number


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd(), help='scan root (default: current working directory)')
    parser.add_argument('--delete', action='store_true', help='permanently delete; default is preview only')
    parser.add_argument('--recursive', action='store_true', help='include ALL deeper subdirectories')
    parser.add_argument('--batch-size', type=positive_int, default=200, help='successful deletes per batch (default: 200)')
    parser.add_argument('--pause', type=nonnegative_seconds, default=1.0, help='seconds to rest after each batch (default: 1)')
    parser.add_argument('--limit', type=positive_int, help='maximum matches to preview/delete in this run')
    parser.add_argument('--verbose', action='store_true', help='print every matching path (default: first 20 only)')
    args = parser.parse_args(argv)
    try:
        if is_link(args.root.lstat()):
            parser.error('root must not be a symbolic link or junction')
        root = args.root.resolve(strict=True)
        if not root.is_dir():
            parser.error('root must be a directory')
    except OSError as exc:
        parser.error(str(exc))
    cutoff = previous_month_start(datetime.now())
    print('Mode: {}'.format('PERMANENT DELETE' if args.delete else 'PREVIEW (no files changed)'))
    print('Root: {}'.format(root))
    print('Modified strictly before: {} (local time)'.format(cutoff.isoformat(' ')))
    print('Scope: {}'.format('all descendants; root files excluded' if args.recursive else 'child/*.html and child/Download/*.html'))
    scan = Scan()
    matched = deleted = skipped = 0
    interrupted = False
    try:
        for path in scan.files(root, args.recursive):
            try:
                if not safe_candidate(path, root, cutoff.timestamp()):
                    continue
                matched += 1
                if args.verbose or matched <= 20:
                    print('{} {}'.format('DELETE' if args.delete else 'WOULD DELETE', path), flush=True)
                if args.delete:
                    # A file may have been updated while output was being written.
                    if not safe_candidate(path, root, cutoff.timestamp()):
                        skipped += 1
                        continue
                    path.unlink()  # No shell, no recycle bin, no directory deletion.
                    deleted += 1
                    if deleted % args.batch_size == 0:
                        print('Batch done: {} deleted, {} errors'.format(deleted, scan.errors), flush=True)
                        if args.pause:
                            time.sleep(args.pause)
            except FileNotFoundError:
                skipped += 1
            except OSError as exc:
                scan.error(path, exc)
            if args.limit and (deleted if args.delete else matched) >= args.limit:
                break
    except KeyboardInterrupt:
        interrupted = True
        print('\nInterrupted. Re-run to process remaining files.')
    print('Summary: matched={}, deleted={}, skipped={}, errors={}, entries_checked={}'.format(
        matched, deleted, skipped, scan.errors, scan.entries), flush=True)
    return 130 if interrupted else (1 if scan.errors else 0)


if __name__ == '__main__':
    sys.exit(main())
