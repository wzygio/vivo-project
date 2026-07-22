from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OutputLayout:
    """Canonical locations for rebuildable project artifacts."""

    root: Path
    reports: Path
    downloads: Path
    decrypted_files: Path
    rpa_downloads: Path
    screenshots: Path
    test_results: Path
    logs: Path
    tmp: Path

    @classmethod
    def from_project_root(cls, project_root: str | Path) -> "OutputLayout":
        root = Path(project_root) / "output"
        return cls(
            root=root,
            reports=root / "reports",
            downloads=root / "downloads",
            decrypted_files=root / "decrypted_files",
            rpa_downloads=root / "rpa_downloads",
            screenshots=root / "screenshots",
            test_results=root / "test-results",
            logs=root / "logs",
            tmp=root / "tmp",
        )

    def directories(self) -> tuple[Path, ...]:
        return (
            self.root,
            self.reports,
            self.downloads,
            self.decrypted_files,
            self.rpa_downloads,
            self.screenshots,
            self.test_results,
            self.logs,
            self.tmp,
        )

    def ensure(self) -> "OutputLayout":
        for directory in self.directories():
            directory.mkdir(parents=True, exist_ok=True)
        return self
