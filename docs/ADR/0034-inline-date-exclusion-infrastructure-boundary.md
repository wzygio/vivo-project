# ADR-0034: Inline Date Exclusion at Infrastructure Outputs

- Status: Accepted
- Date: 2026-10-10

## Context

Inline factory/date exclusions previously ran in report preparation and alarm
projections after some decoration and feature computation. Excluded measurements
could affect latest-point selection or generate an exception ledger before being
removed from the visible report. The user confirmed these dates must be excluded
from downstream processing and requested infrastructure ownership.

## Decision

- Infrastructure adapters return facts filtered by the configured factory and
  inclusive display-date interval, before deduplication, feature aggregation,
  OOS/OOC decoration and capability computation.
- Apply the same boundary to fresh reads, cached snapshots, forced refreshes,
  database fallbacks, historical alarms, throughput and workbook fallbacks.
  Split permitted Particle Size query windows before source SQL aggregation.
- Preserve source snapshots and maintained decisions. Filter output copies;
  merge history updates with unfiltered stored history outside the replacement
  window. Existing replacement-window and generated-workbook semantics remain.
- Keep resolved exclusions in result/cache signatures, including AOI workbook
  readers. Infer event columns from schemas rather than configurable filenames.
- Core retains the policy type and period-aggregate availability rules. Point
  filtering belongs to infrastructure; Core introduces no outward dependencies.

## Consequences

Excluded measurements cannot participate in decoration, statistics or capability
inputs. New generated OOS/OOC facts do not include them. Retained source rows can
be used again if policy changes. Maintained period totals still follow their
existing selective zeroing and preservation rules, because they do not contain
individual event timestamps.

The authoritative rule is [Inline date exclusion](../../references/domain/inline_domain/shared/rules-inline-date-exclusion.md).
