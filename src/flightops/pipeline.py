"""DuckDB pipeline: ingest raw CSV, validate, quarantine, deduplicate, model.

Every raw row ends up in exactly one bucket:

    raw rows = accepted (flights_clean) + duplicates + rejected (quarantine)

Validation runs first and records *every* failing rule per row, so a row with
overlapping defects is rejected once with several reason codes. Deduplication
runs only on rows that passed validation.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb

from .generate import STATIONS

TS = "%Y-%m-%d %H:%M"
DEDUP_KEY = ("flight_number", "flight_date", "origin")
ON_TIME_THRESHOLD_MIN = 15
MAX_TURN_GAP_MIN = 240
MAX_BLOCK_MIN = 18 * 60
MIN_BLOCK_MIN = 15
EARLY_DEP_LIMIT_MIN = 60
LATE_DEP_LIMIT_MIN = 24 * 60

REASON_CODES = {
    "MISSING_REQUIRED": "a required field is empty",
    "INVALID_TIMESTAMP": "a timestamp or flight_date is present but not parseable as a real date/time",
    "INVALID_CANCELLED_FLAG": "cancelled is not 0 or 1",
    "UNKNOWN_STATION": "origin or destination not in the station reference table",
    "SAME_ORIGIN_DESTINATION": "origin equals destination",
    "SCHEDULE_INCONSISTENT": (
        "scheduled arrival not after departure, or flight_date differs from scheduled departure date"
    ),
    "MISSING_ACTUALS": "operated flight (cancelled = 0) lacks an actual departure or arrival",
    "CANCELLED_WITH_ACTUALS": "cancelled flight (cancelled = 1) has actual times",
    "IMPOSSIBLE_DURATION": f"actual block time not within {MIN_BLOCK_MIN}..{MAX_BLOCK_MIN} minutes",
    "DEPARTURE_OUT_OF_WINDOW": (
        f"actual departure more than {EARLY_DEP_LIMIT_MIN} min early or {LATE_DEP_LIMIT_MIN} min late"
    ),
}

RAW_COLS = [
    "record_id", "flight_number", "flight_date", "aircraft_id", "origin", "destination",
    "sched_dep_utc", "sched_arr_utc", "actual_dep_utc", "actual_arr_utc", "cancelled",
]


def connect(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    if str(db_path) != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    cols = ", ".join(f"{c} VARCHAR" for c in RAW_COLS[1:])
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS raw_flights (
            record_id VARCHAR PRIMARY KEY, {cols}, batch_id INTEGER NOT NULL, source_file VARCHAR NOT NULL)
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            batch_id INTEGER PRIMARY KEY, file_sha256 VARCHAR UNIQUE NOT NULL, file_name VARCHAR,
            rows_in_file INTEGER, rows_inserted INTEGER, rows_already_present INTEGER,
            ingested_at TIMESTAMP DEFAULT current_timestamp)
    """)
    con.execute("CREATE OR REPLACE TABLE stations AS SELECT unnest(?::VARCHAR[]) AS code", [STATIONS])
    return con


def file_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ingest(con: duckdb.DuckDBPyConnection, csv_path: Path) -> dict:
    """Append a raw CSV to raw_flights.

    Idempotent at two levels: a file whose SHA-256 is already logged is
    skipped entirely, and individual record_ids already present are ignored.
    The source CSV itself is never modified.
    """
    csv_path = Path(csv_path)
    digest = file_sha256(csv_path)
    if con.execute("SELECT 1 FROM ingestion_log WHERE file_sha256 = ?", [digest]).fetchone():
        return {"file": csv_path.name, "skipped": True, "reason": "file already ingested"}
    batch = con.execute("SELECT coalesce(max(batch_id), 0) + 1 FROM ingestion_log").fetchone()[0]
    con.execute("BEGIN")
    try:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE incoming AS SELECT * FROM read_csv(?, all_varchar = true, header = true)",
            [str(csv_path)],
        )
        cols = con.execute("SELECT * FROM incoming LIMIT 0").description
        names = [c[0] for c in cols]
        if names != RAW_COLS:
            raise ValueError(f"unexpected raw columns: {names}")
        n_file = con.execute("SELECT count(*) FROM incoming").fetchone()[0]
        before = con.execute("SELECT count(*) FROM raw_flights").fetchone()[0]
        con.execute(f"""
            INSERT INTO raw_flights
            SELECT DISTINCT ON (record_id) {", ".join(RAW_COLS)}, ? AS batch_id, ? AS source_file
            FROM incoming
            WHERE record_id IS NOT NULL AND record_id NOT IN (SELECT record_id FROM raw_flights)
            ORDER BY record_id
        """, [batch, csv_path.name])
        inserted = con.execute("SELECT count(*) FROM raw_flights").fetchone()[0] - before
        con.execute(
            "INSERT INTO ingestion_log (batch_id, file_sha256, file_name, rows_in_file, rows_inserted,"
            " rows_already_present) VALUES (?, ?, ?, ?, ?, ?)",
            [batch, digest, csv_path.name, n_file, inserted, n_file - inserted],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return {"file": csv_path.name, "skipped": False, "batch_id": batch,
            "rows_in_file": n_file, "rows_inserted": inserted, "rows_already_present": n_file - inserted}


def _blank(col: str) -> str:
    return f"(nullif(trim({col}), '') IS NULL)"


def transform(con: duckdb.DuckDBPyConnection) -> dict:
    """Rebuild all derived tables from raw_flights (deterministic, so re-runs are idempotent)."""
    required = ["flight_number", "flight_date", "aircraft_id", "origin", "destination",
                "sched_dep_utc", "sched_arr_utc", "cancelled"]
    ts_cols = ["sched_dep_utc", "sched_arr_utc", "actual_dep_utc", "actual_arr_utc"]
    missing = " OR ".join(_blank(c) for c in required)
    bad_ts = " OR ".join(f"(NOT {_blank(c)} AND {c[:-4]} IS NULL)" for c in ts_cols)
    con.execute(f"""
        CREATE OR REPLACE TABLE stg_flights AS
        WITH p AS (
            SELECT r.*,
                try_strptime(nullif(trim(sched_dep_utc), ''), '{TS}') AS sched_dep,
                try_strptime(nullif(trim(sched_arr_utc), ''), '{TS}') AS sched_arr,
                try_strptime(nullif(trim(actual_dep_utc), ''), '{TS}') AS actual_dep,
                try_strptime(nullif(trim(actual_arr_utc), ''), '{TS}') AS actual_arr,
                try_cast(nullif(trim(flight_date), '') AS DATE) AS fdate,
                CASE trim(cancelled) WHEN '0' THEN false WHEN '1' THEN true END AS is_cancelled
            FROM raw_flights r
        )
        SELECT *, list_filter([
            CASE WHEN {missing} THEN 'MISSING_REQUIRED' END,
            CASE WHEN {bad_ts} OR (NOT {_blank('flight_date')} AND fdate IS NULL) THEN 'INVALID_TIMESTAMP' END,
            CASE WHEN NOT {_blank('cancelled')} AND is_cancelled IS NULL THEN 'INVALID_CANCELLED_FLAG' END,
            CASE WHEN (NOT {_blank('origin')} AND origin NOT IN (SELECT code FROM stations))
                   OR (NOT {_blank('destination')} AND destination NOT IN (SELECT code FROM stations))
                 THEN 'UNKNOWN_STATION' END,
            CASE WHEN origin = destination THEN 'SAME_ORIGIN_DESTINATION' END,
            CASE WHEN sched_arr <= sched_dep OR CAST(sched_dep AS DATE) <> fdate THEN 'SCHEDULE_INCONSISTENT' END,
            CASE WHEN is_cancelled = false AND ({_blank('actual_dep_utc')} OR {_blank('actual_arr_utc')})
                 THEN 'MISSING_ACTUALS' END,
            CASE WHEN is_cancelled = true AND NOT ({_blank('actual_dep_utc')} AND {_blank('actual_arr_utc')})
                 THEN 'CANCELLED_WITH_ACTUALS' END,
            CASE WHEN date_diff('minute', actual_dep, actual_arr) NOT BETWEEN {MIN_BLOCK_MIN} AND {MAX_BLOCK_MIN}
                 THEN 'IMPOSSIBLE_DURATION' END,
            CASE WHEN date_diff('minute', sched_dep, actual_dep)
                      NOT BETWEEN -{EARLY_DEP_LIMIT_MIN} AND {LATE_DEP_LIMIT_MIN}
                 THEN 'DEPARTURE_OUT_OF_WINDOW' END
        ], x -> x IS NOT NULL) AS reasons
        FROM p
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE quarantine AS
        SELECT record_id, reasons, reasons[1] AS primary_reason, len(reasons) AS n_reasons,
               batch_id, source_file, {", ".join(RAW_COLS[1:])}
        FROM stg_flights WHERE len(reasons) > 0
    """)
    key = ", ".join(c if c != "flight_date" else "fdate" for c in DEDUP_KEY)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ranked AS
        SELECT *,
            row_number() OVER (PARTITION BY {key} ORDER BY batch_id, record_id) AS rn,
            first_value(record_id) OVER (PARTITION BY {key} ORDER BY batch_id, record_id) AS kept_record_id,
            md5(concat_ws('|', {", ".join(RAW_COLS[1:])})) AS content_hash
        FROM stg_flights WHERE len(reasons) = 0
    """)
    con.execute("""
        CREATE OR REPLACE TABLE duplicates AS
        SELECT d.record_id, d.kept_record_id, d.flight_number, d.fdate AS flight_date, d.origin,
               d.content_hash = k.content_hash AS identical_content, d.batch_id
        FROM ranked d JOIN ranked k ON k.record_id = d.kept_record_id
        WHERE d.rn > 1
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE flights_clean AS
        SELECT record_id, flight_number, fdate AS flight_date, aircraft_id, origin, destination,
               origin || '-' || destination AS route,
               sched_dep, sched_arr, actual_dep, actual_arr, is_cancelled AS cancelled,
               date_diff('minute', sched_dep, actual_dep) AS dep_delay_min,
               date_diff('minute', sched_arr, actual_arr) AS arr_delay_min,
               date_diff('minute', sched_dep, sched_arr) AS sched_block_min,
               date_diff('minute', actual_dep, actual_arr) AS actual_block_min,
               CASE WHEN NOT is_cancelled
                    THEN date_diff('minute', sched_dep, actual_dep) <= {ON_TIME_THRESHOLD_MIN} END AS on_time_dep
        FROM ranked WHERE rn = 1
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE turnarounds AS
        WITH ops AS (
            SELECT aircraft_id, record_id, flight_number, origin, destination, actual_dep, actual_arr,
                   lag(record_id) OVER w AS prev_record_id,
                   lag(destination) OVER w AS prev_destination,
                   lag(actual_arr) OVER w AS prev_actual_arr
            FROM flights_clean WHERE NOT cancelled
            WINDOW w AS (PARTITION BY aircraft_id ORDER BY actual_dep, record_id)
        )
        SELECT aircraft_id, prev_record_id AS inbound_record_id, record_id AS outbound_record_id,
               origin AS station, prev_actual_arr AS inbound_arr, actual_dep AS outbound_dep,
               CAST(prev_actual_arr AS DATE) AS turn_date,
               date_diff('minute', prev_actual_arr, actual_dep) AS ground_min,
               CASE
                   WHEN prev_destination <> origin THEN 'STATION_MISMATCH'
                   WHEN actual_dep <= prev_actual_arr THEN 'NON_POSITIVE_GAP'
                   WHEN date_diff('minute', prev_actual_arr, actual_dep) > {MAX_TURN_GAP_MIN} THEN 'LONG_GROUND_STOP'
                   ELSE 'OK'
               END AS status
        FROM ops WHERE prev_record_id IS NOT NULL
    """)
    return reconcile(con)


def reconcile(con: duckdb.DuckDBPyConnection) -> dict:
    q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    out = {
        "raw_rows": q("SELECT count(*) FROM raw_flights"),
        "accepted": q("SELECT count(*) FROM flights_clean"),
        "duplicates_removed": q("SELECT count(*) FROM duplicates"),
        "duplicates_with_differing_content": q("SELECT count(*) FROM duplicates WHERE NOT identical_content"),
        "rejected": q("SELECT count(*) FROM quarantine"),
        "rejected_with_multiple_reasons": q("SELECT count(*) FROM quarantine WHERE n_reasons > 1"),
    }
    out["balanced"] = out["raw_rows"] == out["accepted"] + out["duplicates_removed"] + out["rejected"]
    reasons = con.execute("""
        SELECT reason, count(*) FROM (SELECT unnest(reasons) AS reason FROM quarantine)
        GROUP BY reason ORDER BY reason
    """).fetchall()
    out["reason_counts"] = {r: n for r, n in reasons}
    if not out["balanced"]:
        raise AssertionError(f"reconciliation failed: {out}")
    return out


def export_parquet(con: duckdb.DuckDBPyConnection, out_dir: Path) -> list[str]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for table in ("flights_clean", "quarantine", "duplicates", "turnarounds"):
        path = out_dir / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT PARQUET)")
        written.append(path.name)
    return written
