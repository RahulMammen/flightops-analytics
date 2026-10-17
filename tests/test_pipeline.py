"""Hand-checked expectations for tests/fixtures/tiny_raw.csv (15 rows).

Accepted (9): R01 R02 R03 R04 R05 R06 R08 R12 R15
Duplicate (1): R07 (exact copy of R01)
Rejected (5): R09 missing aircraft; R10 missing aircraft + invalid timestamp;
              R11 arrival before departure; R13 copy of R10; R14 cancelled with actuals
Operated departure delays (min): R01 10, R02 20, R03 0, R06 45, R08 25, R12 -5, R15 0
"""

from pathlib import Path

import pytest

from flightops import metrics
from flightops.metrics import Filters
from flightops.pipeline import connect, ingest, reconcile, transform

FIX = Path(__file__).parent / "fixtures"


@pytest.fixture()
def con():
    c = connect(":memory:")
    ingest(c, FIX / "tiny_raw.csv")
    transform(c)
    yield c
    c.close()


def ids(con, table):
    return sorted(r[0] for r in con.execute(f"SELECT record_id FROM {table}").fetchall())


def test_reconciliation_buckets(con):
    r = reconcile(con)
    assert (r["raw_rows"], r["accepted"], r["duplicates_removed"], r["rejected"]) == (15, 9, 1, 5)
    assert r["balanced"]
    assert ids(con, "flights_clean") == ["R01", "R02", "R03", "R04", "R05", "R06", "R08", "R12", "R15"]
    assert ids(con, "duplicates") == ["R07"]
    assert ids(con, "quarantine") == ["R09", "R10", "R11", "R13", "R14"]


def test_quality_reason_codes(con):
    got = dict(con.execute("SELECT record_id, reasons FROM quarantine").fetchall())
    assert got["R09"] == ["MISSING_REQUIRED"]
    assert got["R10"] == ["MISSING_REQUIRED", "INVALID_TIMESTAMP"]
    assert got["R11"] == ["IMPOSSIBLE_DURATION"]
    assert got["R13"] == ["MISSING_REQUIRED", "INVALID_TIMESTAMP"]
    assert got["R14"] == ["CANCELLED_WITH_ACTUALS"]
    r = reconcile(con)
    assert r["rejected_with_multiple_reasons"] == 2
    # reason counts exceed rejected rows because of overlapping defects
    assert sum(r["reason_counts"].values()) == 7


def test_duplicate_keeps_first_and_flags_identical(con):
    row = con.execute("SELECT kept_record_id, identical_content FROM duplicates").fetchone()
    assert row == ("R01", True)


def test_summary_metrics(con):
    s = metrics.summary(con)
    assert (s["scheduled"], s["cancelled"], s["operated"], s["on_time"]) == (9, 2, 7, 4)
    assert s["cancellation_rate"] == pytest.approx(2 / 9)
    assert s["on_time_dep_rate"] == pytest.approx(4 / 7)
    assert s["dep_delay_p50"] == pytest.approx(10)
    assert s["dep_delay_p90"] == pytest.approx(33)  # 25 + 0.4 * (45 - 25)


def test_cancelled_flights_excluded_from_delay_and_on_time(con):
    rows = con.execute(
        "SELECT dep_delay_min, on_time_dep FROM flights_clean WHERE cancelled ORDER BY record_id"
    ).fetchall()
    assert rows == [(None, None), (None, None)]


def test_filters(con):
    s = metrics.summary(con, Filters(airports=["VELM"]))
    assert (s["scheduled"], s["cancelled"], s["operated"], s["on_time"]) == (5, 2, 3, 1)
    s = metrics.summary(con, Filters(routes=["NORA-KRIS"]))
    assert s["scheduled"] == 1
    import datetime as dt
    s = metrics.summary(con, Filters(date_from=dt.date(2026, 1, 2), date_to=dt.date(2026, 1, 2)))
    assert (s["scheduled"], s["cancelled"], s["on_time"]) == (3, 0, 1)


def test_route_performance(con):
    rp = metrics.route_performance(con).set_index("route")
    assert rp.loc["NORA-VELM", "scheduled"] == 2
    assert rp.loc["NORA-VELM", "cancellation_rate"] == pytest.approx(0.5)
    assert rp.loc["NORA-KRIS", "on_time_dep_rate"] == pytest.approx(1.0)


def test_turnarounds(con):
    t = con.execute(
        "SELECT aircraft_id, inbound_record_id, outbound_record_id, ground_min, status "
        "FROM turnarounds ORDER BY aircraft_id, outbound_dep"
    ).fetchall()
    assert t == [
        ("FX-001", "R01", "R02", 60, "OK"),
        ("FX-001", "R02", "R03", 30, "OK"),
        ("FX-001", "R03", "R15", 95, "STATION_MISMATCH"),
        ("FX-002", "R06", "R08", 30, "OK"),
    ]
    stats = metrics.turnaround_stats(con)
    assert stats["ok"]["n"] == 3 and stats["ok"]["p50"] == 30


def test_ingest_same_file_twice_is_skipped(con):
    again = ingest(con, FIX / "tiny_raw.csv")
    assert again["skipped"]
    before = {t: ids(con, t) for t in ("flights_clean", "duplicates", "quarantine")}
    transform(con)
    after = {t: ids(con, t) for t in ("flights_clean", "duplicates", "quarantine")}
    assert before == after
    assert reconcile(con)["raw_rows"] == 15


def test_overlapping_file_inserts_only_new_records(con):
    res = ingest(con, FIX / "tiny_retransmit.csv")
    assert (res["rows_in_file"], res["rows_inserted"], res["rows_already_present"]) == (2, 1, 1)
    r = transform(con)
    assert (r["raw_rows"], r["accepted"], r["duplicates_removed"]) == (16, 9, 2)
    row = con.execute("SELECT kept_record_id, identical_content FROM duplicates WHERE record_id='R16'").fetchone()
    assert row == ("R02", False)  # earlier batch wins; differing content is flagged


def test_persistent_db_reopen_is_idempotent(tmp_path):
    db = tmp_path / "w.duckdb"
    for _ in range(2):
        c = connect(db)
        ingest(c, FIX / "tiny_raw.csv")
        r = transform(c)
        c.close()
    assert (r["raw_rows"], r["accepted"]) == (15, 9)


def test_raw_file_is_not_modified(tmp_path):
    src = FIX / "tiny_raw.csv"
    before = src.read_bytes()
    c = connect(tmp_path / "w.duckdb")
    ingest(c, src)
    transform(c)
    c.close()
    assert src.read_bytes() == before
