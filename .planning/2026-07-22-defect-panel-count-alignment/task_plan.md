# Task Plan: Defect Panel Count Alignment

## Approved Scope

- Implement integer monthly calibration for Code-level MWD.
- Apply monthly, weekly, and daily manual overrides after calibration.
- Reaggregate final weekly and monthly outputs from final daily data.
- Preserve Mapping cascade behavior without adding MWD dependencies.
- Document current MWD and Mapping algorithms at function granularity.

## Checklist

1. [completed] Add RED coverage for integer monthly calibration.
2. [completed] Implement calibration and edge-case allocation.
3. [completed] Add RED coverage for post-calibration manual override precedence.
4. [completed] Implement unified manual overrides and final reaggregation.
5. [completed] Lock Mapping cascade behavior with regression coverage.
6. [completed] Run focused tests, Yield smoke audit, compile, coverage availability, and diff checks.
7. [completed] Write function-level design documentation.

## Non-goals

- No MWD/Mapping alignment ratio or strict correspondence.
- No Mapping algorithm changes or monthly baseline dependency.
- No floating-point business counts.
