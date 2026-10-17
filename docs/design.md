# FlightOps design notes

## Key decisions

**Raw as all-VARCHAR.** Raw CSVs are read with `all_varchar = true`, so a malformed timestamp is a value that a
rule can catch, not a load failure. Parsing uses `try_strptime` and `try_cast`, and a NULL parse result on a
non-empty value means `INVALID_TIMESTAMP`.

**Validate first, then deduplicate.** Validation records every failing rule as a `reasons` list, so a row with
two defects is one rejected row with two reasons. That keeps the accounting exact:
`raw = accepted + duplicates + rejected`. `reconcile` raises an error if the equation fails. Deduplicating only
valid rows means a defective copy never displaces a valid original.

**Business key for duplicates.** `record_id` identifies a transmission, not a flight. The flight identity is
`(flight_number, flight_date, origin)`. The earliest batch wins, with ties broken by `record_id`. Re-sends with
different content are flagged, not silently merged.

**Separate defect labels.** The generator writes `injected_defects.csv` beside the raw file. The pipeline never
reads it. Only `metrics.defect_detection` and the tests use it, to measure the rules. This mirrors having
an audit sample without leaking answers into processing.

**Turnaround correctness over coverage.** Consecutive operated legs of one aircraft count as a turn only if the
airports chain and the gap is positive and at most 240 min. Gaps caused by rejected legs appear as
`STATION_MISMATCH` instead of producing a wrong turn time.

**Idempotency at three levels:** the file hash, `record_id`, and deterministic `CREATE OR REPLACE` rebuilds.

**Parameterised filters.** Dashboard filters become `?` parameters with `list_contains`, so user input is never
pasted into SQL text.

## Trade-offs

- Full rebuilds are simple and correct at 50k rows. At much larger volumes, incremental models keyed on `batch_id`
  would be needed.
- A single DuckDB file is easy to ship but supports only one writer. The dashboard opens it read-only.
- The generator carries delays across an aircraft's rotation and across days. It does not model crew legality,
  weather or airport capacity.

## Failure cases

| Case | Behaviour |
|---|---|
| Same file ingested twice | Skipped by SHA-256 (`"skipped": true`) |
| New file with records already present | Only new `record_id`s inserted; the count is logged |
| Unexpected raw columns | `ValueError`; the transaction is rolled back |
| Defect in a duplicated row | Both copies rejected; counted once each |
| Accounting imbalance | `AssertionError` from `reconcile` |
| Filters match nothing | The dashboard shows a warning instead of empty charts |
| Warehouse missing | The dashboard shows an error naming the command to run |
