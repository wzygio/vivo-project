"""Standalone regression tests; run with Python's built-in unittest."""
import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

import clean_old_html as cleaner


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cutoff = cleaner.previous_month_start(datetime.now()).timestamp()

    def file(self, relative, age=-60):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('test', encoding='utf-8')
        os.utime(path, (self.cutoff + age, self.cutoff + age))
        return path

    def run_tool(self, *args):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return cleaner.main(['--root', str(self.root), '--pause', '0', *args])

    def test_january_rollover(self):
        self.assertEqual(cleaner.previous_month_start(datetime(2026, 1, 15)), datetime(2025, 12, 1))
        self.assertEqual(cleaner.previous_month_start(datetime(2026, 9, 14)), datetime(2026, 8, 1))

    def test_preview_and_exact_scope_and_boundary(self):
        targets = [self.file('A/old.HTML'), self.file('A/Download/old.html')]
        preserved = [self.file('root.html'), self.file('A/exact.html', 0),
                     self.file('A/new.html', 60), self.file('A/old.htm'),
                     self.file('A/data.csv'), self.file('A/deep/old.html')]
        self.assertEqual(self.run_tool(), 0)
        self.assertTrue(all(p.exists() for p in targets + preserved))
        self.assertEqual(self.run_tool('--delete'), 0)
        self.assertTrue(all(not p.exists() for p in targets))
        self.assertTrue(all(p.exists() for p in preserved))

    def test_limit_and_resume(self):
        paths = [self.file('A/%s.html' % i) for i in range(5)]
        self.assertEqual(self.run_tool('--delete', '--limit', '2'), 0)
        self.assertEqual(sum(p.exists() for p in paths), 3)
        self.assertEqual(self.run_tool('--delete'), 0)
        self.assertFalse(any(p.exists() for p in paths))

    def test_recursive_and_batch_pause(self):
        paths = [self.file('A/deep/%s.html' % i) for i in range(3)]
        with patch.object(cleaner.time, 'sleep') as sleep:
            self.assertEqual(self.run_tool('--recursive', '--delete', '--batch-size', '2', '--pause', '1'), 0)
            sleep.assert_called_once_with(1)
        self.assertFalse(any(p.exists() for p in paths))

    def test_delete_failure_is_reported(self):
        path = self.file('A/old.html')
        with patch.object(Path, 'unlink', side_effect=PermissionError('locked')):
            self.assertEqual(self.run_tool('--delete'), 1)
        self.assertTrue(path.exists())

    def test_updated_file_is_rechecked_before_delete(self):
        path = self.file('A/old.html')
        with patch.object(cleaner, 'safe_candidate', side_effect=[True, False]):
            self.assertEqual(self.run_tool('--delete'), 0)
        self.assertTrue(path.exists())

    def test_interrupt_preserves_remaining_files(self):
        paths = [self.file('A/%s.html' % i) for i in range(3)]
        with patch.object(cleaner.time, 'sleep', side_effect=KeyboardInterrupt):
            self.assertEqual(self.run_tool('--delete', '--batch-size', '1', '--pause', '1'), 130)
        self.assertEqual(sum(p.exists() for p in paths), 2)
        self.assertEqual(self.run_tool('--delete'), 0)
        self.assertFalse(any(p.exists() for p in paths))

    def test_scan_permission_failure_is_reported(self):
        with patch.object(cleaner.os, 'scandir', side_effect=PermissionError('denied')):
            self.assertEqual(self.run_tool(), 1)

    def test_reparse_marker(self):
        from types import SimpleNamespace
        self.assertTrue(cleaner.is_link(SimpleNamespace(st_mode=0, st_file_attributes=0x400)))

    def test_symlink_is_skipped(self):
        target = self.file('outside/keep.html')
        link = self.root / 'A' / 'alias.html'
        link.parent.mkdir()
        try:
            link.symlink_to(target)
        except OSError:
            self.skipTest('Creating symlinks requires Windows privilege')
        self.assertFalse(cleaner.safe_candidate(link, self.root, self.cutoff))


if __name__ == '__main__':
    unittest.main()
