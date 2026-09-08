# Findings

- Inline raw v3-calendar files for seven products are covered_from Jan1; AOI_RS v2-calendar for M626/M678/Z571 also annual.
- Other AOI_RS v1 files are normal; Yield/Equipment/QTime must not be deleted.
- Prior repos actually reload full window on TTL, not incremental; max fact timestamp incorrectly invalidates no-activity days.
- Seven input Excel files inspected in about6 seconds via fr-file-decryption; product sheets include flags and time fields.
- Summary legacy sheet has rates/throughput but lacks exact alarm counts; user permits rounded rate×volume baseline.
- CPK local workbooks contain day/week/month (not year/quarter); cannot derive annual capability from weekly CPK.
- User modified production summary workbook since earlier inspection; preserve it and reload latest before dashboard work.
- Dirty equipment application/updater files/tests and Yield input workbook are unrelated user changes.
