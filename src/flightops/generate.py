"""Deterministic generator of SYNTHETIC flight operations for a fictional airline.

Nothing here is real: the carrier, station codes, aircraft tails and times are
invented. The generator builds plausible aircraft rotations (hub out-and-back
pairs), propagates delays along each rotation, cancels some pairs, and then
injects data defects. Defect labels are written to a separate file and are
never read by the pipeline; they exist only to measure the quality rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

HUBS = ["NORA", "VELM"]
SPOKES = ["KRIS", "TALO", "PEXA", "MIRO", "ZENT", "OKAB", "LUMA", "DRAV", "SOLE", "BRIX"]
STATIONS = HUBS + SPOKES
TS_FMT = "%Y-%m-%d %H:%M"
RAW_COLUMNS = [
    "record_id", "flight_number", "flight_date", "aircraft_id", "origin", "destination",
    "sched_dep_utc", "sched_arr_utc", "actual_dep_utc", "actual_arr_utc", "cancelled",
]


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 20260101
    n_aircraft: int = 84
    days: int = 120
    start_date: str = "2026-01-01"
    cancel_pair_prob: float = 0.015
    duplicate_frac: float = 0.015
    missing_frac: float = 0.008
    invalid_ts_frac: float = 0.008
    impossible_duration_frac: float = 0.008
    overlap_frac: float = 0.002


def _block_minutes(rng: np.random.Generator) -> dict[tuple[str, str], int]:
    blocks = {}
    for hub in HUBS:
        for dest in STATIONS:
            if dest != hub:
                m = int(rng.integers(10, 37) * 5)  # 50..180 minutes
                blocks[(hub, dest)] = blocks[(dest, hub)] = blocks.get((dest, hub), m)
    return blocks


def _dep_delay(rng: np.random.Generator) -> int:
    u = rng.random()
    if u < 0.65:
        return int(round(rng.normal(-2, 4)))
    if u < 0.95:
        return int(round(rng.exponential(25)))
    return int(round(rng.exponential(90)))


def generate_clean(cfg: GeneratorConfig) -> pd.DataFrame:
    """Generate defect-free scheduled flights with actuals or cancellations."""
    rng = np.random.default_rng(cfg.seed)
    blocks = _block_minutes(rng)
    start = datetime.fromisoformat(cfg.start_date)
    rows = []
    for a in range(cfg.n_aircraft):
        tail = f"FX-{a + 1:03d}"
        base = HUBS[a % len(HUBS)]
        # Carried across days so a late-night delay also delays the next morning.
        prev_actual_arr = None
        for d in range(cfg.days):
            day = start + timedelta(days=d)
            t = day + timedelta(hours=6, minutes=int(rng.integers(0, 12)) * 5)
            leg = 0
            # The last pair departs before 17:00, so every rotation ends the same day.
            while t.hour < 17 and t.date() == day.date():
                dest = str(rng.choice([s for s in STATIONS if s != base]))
                cancelled = rng.random() < cfg.cancel_pair_prob
                for org, dst in ((base, dest), (dest, base)):
                    block = blocks[(org, dst)]
                    sched_dep, sched_arr = t, t + timedelta(minutes=block)
                    row = {
                        "flight_number": f"FX{1000 + a * 20 + leg}",
                        "flight_date": sched_dep.strftime("%Y-%m-%d"),
                        "aircraft_id": tail, "origin": org, "destination": dst,
                        "sched_dep_utc": sched_dep.strftime(TS_FMT),
                        "sched_arr_utc": sched_arr.strftime(TS_FMT),
                        "actual_dep_utc": "", "actual_arr_utc": "", "cancelled": int(cancelled),
                    }
                    if not cancelled:
                        dep = sched_dep + timedelta(minutes=_dep_delay(rng))
                        if prev_actual_arr is not None:
                            dep = max(dep, prev_actual_arr + timedelta(minutes=30))
                        actual_block = max(int(block * 0.85), int(round(rng.normal(block, 6))))
                        arr = dep + timedelta(minutes=actual_block)
                        row["actual_dep_utc"] = dep.strftime(TS_FMT)
                        row["actual_arr_utc"] = arr.strftime(TS_FMT)
                        prev_actual_arr = arr
                    rows.append(row)
                    leg += 1
                    t = sched_arr + timedelta(minutes=45)
    return pd.DataFrame(rows)


def inject_defects(clean: pd.DataFrame, cfg: GeneratorConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (raw rows, defect labels). Labels have one row per injected defect."""
    rng = np.random.default_rng(cfg.seed + 1)
    df = clean.copy()
    n = len(df)
    labels: list[tuple[int, str]] = []
    operated = np.flatnonzero(df["cancelled"].to_numpy() == 0)
    pool = rng.permutation(n)
    take = iter(pool)

    def pick(k: int, from_idx: np.ndarray | None = None) -> list[int]:
        if from_idx is None:
            return [next(take) for _ in range(k)]
        chosen = rng.choice(np.setdiff1d(from_idx, [i for i, _ in labels]), size=k, replace=False)
        return chosen.tolist()

    for i in pick(int(n * cfg.missing_frac)):
        df.loc[i, str(rng.choice(["aircraft_id", "origin", "sched_dep_utc"]))] = ""
        labels.append((i, "missing_field"))
    for i in pick(int(n * cfg.invalid_ts_frac)):
        col = str(rng.choice(["sched_arr_utc", "actual_dep_utc"]))
        df.loc[i, col] = str(rng.choice(["2026-02-30 10:00", "not-a-time", "2026-03-01 24:61"]))
        labels.append((i, "invalid_timestamp"))
    for i in pick(int(n * cfg.impossible_duration_frac), operated):
        dep = datetime.strptime(df.loc[i, "actual_dep_utc"], TS_FMT)
        if rng.random() < 0.5:
            df.loc[i, "actual_arr_utc"] = (dep - timedelta(minutes=30)).strftime(TS_FMT)
        else:
            df.loc[i, "actual_arr_utc"] = (dep + timedelta(hours=30)).strftime(TS_FMT)
        labels.append((i, "impossible_duration"))
    # overlapping defects: a missing field AND an invalid timestamp on one row
    for i in pick(int(n * cfg.overlap_frac)):
        df.loc[i, "aircraft_id"] = ""
        df.loc[i, "sched_arr_utc"] = "not-a-time"
        labels += [(i, "missing_field"), (i, "invalid_timestamp")]

    # duplicates are copies of random rows (some of which are already defective)
    dup_src = rng.choice(n, size=int(n * cfg.duplicate_frac), replace=False)
    dups = df.iloc[dup_src].copy()
    dups["_dup_of"] = dup_src
    df["_dup_of"] = -1
    # Duplicates behave like later re-transmissions: they get later record_ids.
    shuffled = [part.sample(frac=1.0, random_state=cfg.seed % (2**32)) for part in (df, dups)]
    raw = pd.concat(shuffled, ignore_index=False)
    raw["_src"] = raw.index
    raw = raw.reset_index(drop=True)
    raw.insert(0, "record_id", [f"R{i + 1:06d}" for i in range(len(raw))])

    src_to_rid = raw[raw["_dup_of"] == -1].set_index("_src")["record_id"]
    label_rows = [{"record_id": src_to_rid[i], "defect_type": t} for i, t in labels]
    # every copy of a defective source row carries the same defect
    for _, r in raw[raw["_dup_of"] >= 0].iterrows():
        label_rows.append({"record_id": r["record_id"], "defect_type": "duplicate"})
        for i, t in labels:
            if i == r["_src"]:
                label_rows.append({"record_id": r["record_id"], "defect_type": t})
    labels_df = pd.DataFrame(label_rows).sort_values(["record_id", "defect_type"]).reset_index(drop=True)
    return raw[RAW_COLUMNS], labels_df


def write_dataset(out_dir: Path, cfg: GeneratorConfig | None = None) -> dict:
    cfg = cfg or GeneratorConfig()
    out_dir.mkdir(parents=True, exist_ok=True)
    clean = generate_clean(cfg)
    raw, labels = inject_defects(clean, cfg)
    raw.to_csv(out_dir / "flights_raw.csv", index=False)
    labels.to_csv(out_dir / "injected_defects.csv", index=False)
    return {"clean_rows": len(clean), "raw_rows": len(raw), "defect_labels": len(labels),
            "defect_counts": labels["defect_type"].value_counts().sort_index().to_dict()}
