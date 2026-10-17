"""FlightOps dashboard.

    streamlit run app/dashboard.py

Reads the DuckDB warehouse built by ``python -m flightops.cli all``.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from flightops import metrics
from flightops.metrics import Filters
from flightops.pipeline import ON_TIME_THRESHOLD_MIN, REASON_CODES, reconcile

ROOT = Path(__file__).resolve().parents[1]
DB = Path(os.environ.get("FLIGHTOPS_DB", ROOT / "data" / "warehouse" / "flightops.duckdb"))
LABELS = Path(os.environ.get("FLIGHTOPS_LABELS", ROOT / "data" / "raw" / "injected_defects.csv"))

st.set_page_config(page_title="FlightOps", layout="wide")
st.title("FlightOps — Flight Operations Analytics")
st.caption("All data is **synthetic**, generated for a fictional airline. "
           "Figures describe the generated dataset only, not any real carrier.")

if not DB.exists():
    st.error(f"Warehouse not found at {DB}. Run `python -m flightops.cli all` first.")
    st.stop()


@st.cache_resource
def get_con(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path, read_only=True)


con = get_con(str(DB))
lo, hi = con.execute("SELECT min(flight_date), max(flight_date) FROM flights_clean").fetchone()
stations = [r[0] for r in con.execute(
    "SELECT DISTINCT origin FROM flights_clean UNION SELECT DISTINCT destination FROM flights_clean ORDER BY 1"
).fetchall()]

with st.sidebar:
    st.header("Filters")
    dates = st.date_input("Flight date range (UTC)", (lo, hi), min_value=lo, max_value=hi)
    d_from, d_to = (dates if isinstance(dates, tuple) and len(dates) == 2 else (lo, hi))
    airports = st.multiselect("Airports (origin or destination)", stations)
    route_sql = "SELECT DISTINCT route FROM flights_clean"
    route_params: list = []
    if airports:
        route_sql += " WHERE list_contains(?, origin) OR list_contains(?, destination)"
        route_params = [airports, airports]
    route_opts = [r[0] for r in con.execute(route_sql + " ORDER BY 1", route_params).fetchall()]
    routes = st.multiselect("Routes", route_opts)
    st.caption("Filters combine with AND. Turnarounds use date and airport (turn station) only.")

f = Filters(date_from=d_from, date_to=d_to, airports=airports, routes=routes)
s = metrics.summary(con, f)

tab_ops, tab_routes, tab_turns, tab_quality = st.tabs(
    ["Operations", "Routes", "Turnarounds", "Data quality"])

with tab_ops:
    c = st.columns(5)
    c[0].metric("Scheduled flights", f"{s['scheduled']:,}")
    c[1].metric("Cancellation rate", "–" if s["cancellation_rate"] is None else f"{s['cancellation_rate']:.2%}")
    c[2].metric(f"On-time departure (≤{ON_TIME_THRESHOLD_MIN} min)",
                "–" if s["on_time_dep_rate"] is None else f"{s['on_time_dep_rate']:.2%}")
    c[3].metric("Dep. delay p50 / p90", "–" if s["dep_delay_p50"] is None
                else f"{s['dep_delay_p50']:.0f} / {s['dep_delay_p90']:.0f} min")
    c[4].metric("Dep. delay p95", "–" if s["dep_delay_p95"] is None else f"{s['dep_delay_p95']:.0f} min")
    st.caption("Cancellation rate = cancelled / scheduled accepted flights. On-time rate and delay "
               "percentiles use operated (non-cancelled) flights only. Delay = actual − scheduled departure.")
    if s["scheduled"] == 0:
        st.warning("No flights match the current filters.")
    else:
        d = metrics.daily(con, f)
        fig = px.line(d, x="flight_date", y="on_time_dep_rate", title="Daily on-time departure rate")
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, width="stretch")
        dl = metrics.delays(con, f)
        h = px.histogram(dl[dl.dep_delay_min <= 240], x="dep_delay_min", nbins=60,
                         title="Departure delay distribution (operated flights, ≤ 240 min shown)",
                         labels={"dep_delay_min": "departure delay (min)"})
        h.add_vline(x=ON_TIME_THRESHOLD_MIN, line_dash="dot")
        st.plotly_chart(h, width="stretch")
        flights = metrics.filtered_flights(con, f)
        st.download_button("Download filtered flights (CSV)", flights.to_csv(index=False),
                           "flights_filtered.csv", "text/csv")

with tab_routes:
    rp = metrics.route_performance(con, f)
    if rp.empty:
        st.warning("No routes match the current filters.")
    else:
        top = rp.head(20)
        fig = px.bar(top, x="route", y="on_time_dep_rate", hover_data=["scheduled", "cancellation_rate"],
                     title="On-time departure rate by route (20 busiest in selection)")
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, width="stretch")
        st.dataframe(rp, hide_index=True)
        st.download_button("Download route performance (CSV)", rp.to_csv(index=False),
                           "route_performance.csv", "text/csv")

with tab_turns:
    t = metrics.turnaround_stats(con, f)
    ok = t["ok"]
    c = st.columns(4)
    c[0].metric("Valid turnarounds", f"{int(ok['n']):,}")
    c[1].metric("Median ground time", "–" if pd.isna(ok["p50"]) else f"{ok['p50']:.0f} min")
    c[2].metric("p90 ground time", "–" if pd.isna(ok["p90"]) else f"{ok['p90']:.0f} min")
    c[3].metric("Mean ground time", "–" if pd.isna(ok["mean"]) else f"{ok['mean']:.1f} min")
    st.caption("Turnaround = next actual departure − previous actual arrival for the same aircraft, counted "
               "only if the airports match, the gap is positive and ≤ 240 min. Other pairs are classified below.")
    if int(ok["n"]):
        st.plotly_chart(px.histogram(t["values"], x="ground_min", nbins=50, title="Ground time (valid turns)",
                                     labels={"ground_min": "minutes"}), width="stretch")
    a, b = st.columns(2)
    a.dataframe(t["status_counts"], hide_index=True)
    b.dataframe(t["by_station"], hide_index=True)

with tab_quality:
    r = reconcile(con)
    st.subheader("Record accounting (whole dataset, unfiltered)")
    acc = pd.DataFrame([
        {"bucket": "raw rows ingested", "rows": r["raw_rows"]},
        {"bucket": "accepted", "rows": r["accepted"]},
        {"bucket": "duplicates removed", "rows": r["duplicates_removed"]},
        {"bucket": "rejected (quarantine)", "rows": r["rejected"]},
    ])
    st.dataframe(acc, hide_index=True)
    st.write(f"Balanced: **{r['balanced']}** (raw = accepted + duplicates + rejected). "
             f"{r['rejected_with_multiple_reasons']} rejected rows have more than one reason; "
             f"{r['duplicates_with_differing_content']} duplicates differ in content from the kept row.")
    rc = pd.DataFrame([{"reason": k, "rows": v, "definition": REASON_CODES[k]}
                       for k, v in r["reason_counts"].items()])
    st.plotly_chart(px.bar(rc, x="reason", y="rows", title="Quarantine reason counts (a row can have several)"),
                    width="stretch")
    st.dataframe(rc, hide_index=True)
    log = con.execute("SELECT * FROM ingestion_log ORDER BY batch_id").fetchdf()
    st.subheader("Ingestion log")
    st.dataframe(log, hide_index=True)
    if LABELS.exists():
        st.subheader("Rule check against injected-defect labels (evaluation only)")
        st.dataframe(metrics.defect_detection(con, str(LABELS)), hide_index=True)
    q = con.execute("SELECT record_id, array_to_string(reasons, ';') AS reasons, flight_number, flight_date, "
                    "aircraft_id, origin, destination, sched_dep_utc, sched_arr_utc, actual_dep_utc, "
                    "actual_arr_utc, cancelled FROM quarantine ORDER BY record_id").fetchdf()
    st.subheader("Quarantined records")
    st.dataframe(q.head(200), hide_index=True)
    st.download_button("Download quarantine (CSV)", q.to_csv(index=False), "quarantine.csv", "text/csv")
    st.download_button("Download data-quality summary (CSV)", pd.concat(
        [acc.rename(columns={"bucket": "item"}), rc[["reason", "rows"]].rename(columns={"reason": "item"})]
    ).to_csv(index=False), "data_quality_summary.csv", "text/csv")
