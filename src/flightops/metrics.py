"""Analytics queries over flights_clean and turnarounds, with optional filters.

Definitions (all times UTC, all durations in minutes):

- scheduled flights: accepted rows (flights_clean), cancelled or not
- operated flights: accepted rows with cancelled = false
- departure delay: actual_dep - sched_dep (negative = early)
- on-time departure: departure delay <= 15 min; rate = on-time / operated
- cancellation rate: cancelled / scheduled
- delay percentiles: continuous quantiles of departure delay over operated flights
- turnaround: ground time between an aircraft's consecutive operated flights,
  counted only when the inbound destination equals the outbound origin, the
  gap is positive and at most 240 min (status = 'OK')

Airport filter: a flight matches if its origin OR destination is selected.
Route filter: exact 'ORIG-DEST' match. Filters combine with AND.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import duckdb
import pandas as pd


@dataclass
class Filters:
    date_from: date | None = None
    date_to: date | None = None
    airports: list[str] = field(default_factory=list)
    routes: list[str] = field(default_factory=list)

    def where(self, date_col: str = "flight_date", airport_cols=("origin", "destination"),
              route_col: str | None = "route") -> tuple[str, list]:
        clauses, params = [], []
        if self.date_from:
            clauses.append(f"{date_col} >= ?")
            params.append(self.date_from)
        if self.date_to:
            clauses.append(f"{date_col} <= ?")
            params.append(self.date_to)
        if self.airports:
            clauses.append("(" + " OR ".join(f"list_contains(?, {c})" for c in airport_cols) + ")")
            params += [list(self.airports)] * len(airport_cols)
        if self.routes and route_col:
            clauses.append(f"list_contains(?, {route_col})")
            params.append(list(self.routes))
        return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def summary(con: duckdb.DuckDBPyConnection, f: Filters | None = None) -> dict:
    f = f or Filters()
    where, params = f.where()
    row = con.execute(f"""
        SELECT count(*) AS scheduled,
               count(*) FILTER (WHERE cancelled) AS cancelled,
               count(*) FILTER (WHERE NOT cancelled) AS operated,
               count(*) FILTER (WHERE on_time_dep) AS on_time,
               quantile_cont(dep_delay_min, 0.5) FILTER (WHERE NOT cancelled) AS dep_delay_p50,
               quantile_cont(dep_delay_min, 0.9) FILTER (WHERE NOT cancelled) AS dep_delay_p90,
               quantile_cont(dep_delay_min, 0.95) FILTER (WHERE NOT cancelled) AS dep_delay_p95,
               avg(arr_delay_min) FILTER (WHERE NOT cancelled) AS arr_delay_mean
        FROM flights_clean {where}
    """, params).fetchdf().iloc[0].to_dict()
    sched, operated = int(row["scheduled"]), int(row["operated"])
    out = {k: (None if pd.isna(v) else float(v)) for k, v in row.items()}
    out.update(scheduled=sched, cancelled=int(row["cancelled"]), operated=operated,
               on_time=int(row["on_time"]))
    out["cancellation_rate"] = row["cancelled"] / sched if sched else None
    out["on_time_dep_rate"] = row["on_time"] / operated if operated else None
    return out


def route_performance(con: duckdb.DuckDBPyConnection, f: Filters | None = None) -> pd.DataFrame:
    f = f or Filters()
    where, params = f.where()
    return con.execute(f"""
        SELECT route, origin, destination,
               count(*) AS scheduled,
               count(*) FILTER (WHERE NOT cancelled) AS operated,
               round(count(*) FILTER (WHERE cancelled) / count(*), 4) AS cancellation_rate,
               round(count(*) FILTER (WHERE on_time_dep)
                     / nullif(count(*) FILTER (WHERE NOT cancelled), 0), 4) AS on_time_dep_rate,
               round(quantile_cont(dep_delay_min, 0.5) FILTER (WHERE NOT cancelled), 1) AS dep_delay_p50,
               round(quantile_cont(dep_delay_min, 0.9) FILTER (WHERE NOT cancelled), 1) AS dep_delay_p90,
               round(avg(actual_block_min) FILTER (WHERE NOT cancelled), 1) AS mean_block_min
        FROM flights_clean {where}
        GROUP BY ALL ORDER BY scheduled DESC, route
    """, params).fetchdf()


def daily(con: duckdb.DuckDBPyConnection, f: Filters | None = None) -> pd.DataFrame:
    f = f or Filters()
    where, params = f.where()
    return con.execute(f"""
        SELECT flight_date, count(*) AS scheduled,
               count(*) FILTER (WHERE cancelled) AS cancelled,
               count(*) FILTER (WHERE on_time_dep) / nullif(count(*) FILTER (WHERE NOT cancelled), 0)
                   AS on_time_dep_rate
        FROM flights_clean {where} GROUP BY flight_date ORDER BY flight_date
    """, params).fetchdf()


def delays(con: duckdb.DuckDBPyConnection, f: Filters | None = None) -> pd.DataFrame:
    f = f or Filters()
    where, params = f.where()
    extra = "AND NOT cancelled" if where else "WHERE NOT cancelled"
    return con.execute(f"SELECT route, dep_delay_min FROM flights_clean {where} {extra}", params).fetchdf()


def turnaround_stats(con: duckdb.DuckDBPyConnection, f: Filters | None = None) -> dict:
    """Turnaround statistics. Airport filter applies to the turn station; route filter is not applied."""
    f = f or Filters()
    where, params = f.where(date_col="turn_date", airport_cols=("station",), route_col=None)
    status = con.execute(
        f"SELECT status, count(*) AS n FROM turnarounds {where} GROUP BY status ORDER BY status", params
    ).fetchdf()
    extra = "AND status = 'OK'" if where else "WHERE status = 'OK'"
    ok = con.execute(f"""
        SELECT count(*) AS n, quantile_cont(ground_min, 0.5) AS p50, quantile_cont(ground_min, 0.9) AS p90,
               avg(ground_min) AS mean, min(ground_min) AS min, max(ground_min) AS max
        FROM turnarounds {where} {extra}
    """, params).fetchdf().iloc[0].to_dict()
    by_station = con.execute(f"""
        SELECT station, count(*) AS turns, quantile_cont(ground_min, 0.5) AS p50_min,
               quantile_cont(ground_min, 0.9) AS p90_min
        FROM turnarounds {where} {extra} GROUP BY station ORDER BY station
    """, params).fetchdf()
    values = con.execute(f"SELECT ground_min FROM turnarounds {where} {extra}", params).fetchdf()
    return {"status_counts": status, "ok": ok, "by_station": by_station, "values": values}


def filtered_flights(con: duckdb.DuckDBPyConnection, f: Filters | None = None) -> pd.DataFrame:
    f = f or Filters()
    where, params = f.where()
    return con.execute(f"SELECT * FROM flights_clean {where} ORDER BY sched_dep, flight_number", params).fetchdf()


def defect_detection(con: duckdb.DuckDBPyConnection, labels_csv: str) -> pd.DataFrame:
    """Compare injected-defect labels (evaluation only) with pipeline outcomes."""
    return con.execute("""
        WITH lab AS (SELECT * FROM read_csv(?, all_varchar = true, header = true)),
        outcome AS (
            SELECT record_id, 'rejected' AS outcome FROM quarantine
            UNION ALL SELECT record_id, 'duplicate' FROM duplicates
            UNION ALL SELECT record_id, 'accepted' FROM flights_clean
        )
        SELECT lab.defect_type, count(*) AS injected,
               count(*) FILTER (WHERE outcome <> 'accepted') AS caught,
               count(*) FILTER (WHERE outcome = 'rejected') AS rejected,
               count(*) FILTER (WHERE outcome = 'duplicate') AS removed_as_duplicate,
               count(*) FILTER (WHERE outcome = 'accepted') AS missed
        FROM lab JOIN outcome USING (record_id)
        GROUP BY lab.defect_type ORDER BY lab.defect_type
    """, [labels_csv]).fetchdf()
