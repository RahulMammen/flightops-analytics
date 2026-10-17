# FlightOps interview guide

## The workflow in plain words

1. Generate about 50,000 fictional flights, then deliberately break some of them: copies, blanks, bad dates and
   impossible flight times. The list of what was broken goes in a separate file.
2. Load the raw file into DuckDB without changing the file, and remember its fingerprint so it is never loaded
   twice.
3. Check every row against ten rules. Any row that fails goes to a quarantine table with all its reasons.
4. Among good rows, keep one row per flight. Extra copies go to a duplicates table.
5. Build analytics with SQL: on-time rate, delay percentiles, cancellations, routes and aircraft turnarounds.
6. Prove that nothing was lost: raw = accepted + duplicates + rejected.

## The algorithm

- **Validation:** one SQL `SELECT` builds `list_filter([CASE ... END, ...])` to produce a reason list per row.
- **Deduplication:** `row_number() OVER (PARTITION BY flight_number, flight_date, origin ORDER BY batch_id,
  record_id)`.
- **Turnarounds:** `lag()` over each aircraft's operated legs, ordered by actual departure, followed by a status
  `CASE` (airport chain, positive gap, ≤ 240 min).
- **Metrics:** `count(*) FILTER (WHERE ...)` for rates with explicit denominators, and `quantile_cont` for
  percentiles.

## Likely questions

**1. How do you guarantee no records are lost or double-counted?**
Each raw row is placed in exactly one of three tables. `reconcile` checks raw = accepted + duplicates + rejected
and fails loudly otherwise. Rows with several defects are rejected once but keep every reason, so reason counts can
exceed rejected rows. Both numbers are reported.

**2. Why validate before deduplicating?**
If you deduplicate first, a broken copy might be kept and the good original thrown away. Validating first means
only valid rows compete for the key.

**3. What is your on-time definition, and why is the denominator important?**
On-time means departing no more than 15 min late, measured over **operated** flights. Including cancelled flights
would make the on-time rate depend on the cancellation rate. Cancellation rate uses all scheduled flights.

**4. How is a turnaround computed, and what can go wrong?**
It is the gap between an aircraft's arrival and its next departure. If a leg was rejected, the next "consecutive"
leg starts at a different airport. That pair is flagged `STATION_MISMATCH`, not reported as a turn. Overnight stops
over 240 min are also excluded.

**5. What does catching 100% of injected defects prove?**
Only that the rules cover the defect types the generator injects, and that no clean rows are falsely rejected.
Real data has unknown failure modes, so the label check is a regression test for the rules, not a measure of
real-world quality.

## A useful extension

Add incremental processing. Transform only new batches, and resolve late re-transmissions with field-level
"latest non-null wins" rules. Store a data-quality history per batch, so that trends such as a rising invalid
timestamp rate can trigger alerts.
