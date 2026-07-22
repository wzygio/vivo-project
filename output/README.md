# Output directory

`output/` is the only root for rebuildable runtime and development artifacts.
Source business inputs and reusable fixtures remain under `resources/`.

| Directory | Ownership |
| --- | --- |
| `reports/` | Generated business reports, analysis tables, charts, and conversion results |
| `downloads/` | User-initiated or tool-generated downloads |
| `decrypted_files/` | Temporary decrypted or normalized working copies |
| `rpa_downloads/` | Files downloaded by browser/RPA workflows |
| `screenshots/` | Browser screenshots and document-preview images |
| `test-results/` | Test reports, traces, videos, and coverage artifacts |
| `logs/` | Runtime logs, rotated logs, probes, and diagnostic exports |
| `tmp/` | Short-lived intermediate files that do not fit another category |

All children except this README are ignored by Git and may be cleaned or rebuilt.
Code that writes artifacts should create its destination directory before writing.
