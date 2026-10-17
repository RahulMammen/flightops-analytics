import pandas as pd

from flightops import metrics
from flightops.generate import GeneratorConfig, generate_clean, inject_defects, write_dataset
from flightops.pipeline import connect, ingest, transform

SMALL = GeneratorConfig(n_aircraft=6, days=10, duplicate_frac=0.05, missing_frac=0.03,
                        invalid_ts_frac=0.03, impossible_duration_frac=0.03, overlap_frac=0.01)


def test_generator_is_deterministic():
    a, la = inject_defects(generate_clean(SMALL), SMALL)
    b, lb = inject_defects(generate_clean(SMALL), SMALL)
    pd.testing.assert_frame_equal(a, b)
    pd.testing.assert_frame_equal(la, lb)


def test_clean_rotations_are_continuous():
    clean = generate_clean(SMALL)
    assert clean.groupby(["flight_number", "flight_date", "origin"]).size().max() == 1
    ops = clean[clean.cancelled == 0].sort_values(["aircraft_id", "actual_dep_utc"])
    for _, g in ops.groupby("aircraft_id"):
        assert (g["origin"].iloc[1:].to_numpy() == g["destination"].iloc[:-1].to_numpy()).all()
        assert (g["actual_dep_utc"].iloc[1:].to_numpy() > g["actual_arr_utc"].iloc[:-1].to_numpy()).all()


def test_labels_are_separate_from_raw(tmp_path):
    write_dataset(tmp_path, SMALL)
    raw = pd.read_csv(tmp_path / "flights_raw.csv")
    assert "defect_type" not in raw.columns
    labels = pd.read_csv(tmp_path / "injected_defects.csv")
    assert set(labels.record_id) <= set(raw.record_id)


def test_rules_catch_every_injected_defect(tmp_path):
    write_dataset(tmp_path, SMALL)
    con = connect(":memory:")
    ingest(con, tmp_path / "flights_raw.csv")
    r = transform(con)
    assert r["balanced"]
    det = metrics.defect_detection(con, str(tmp_path / "injected_defects.csv")).set_index("defect_type")
    assert (det["missed"] == 0).all()
    # unlabelled rows must all be accepted (no false rejections on clean data)
    labelled = set(pd.read_csv(tmp_path / "injected_defects.csv").record_id)
    rejected = {x[0] for x in con.execute("SELECT record_id FROM quarantine").fetchall()}
    assert rejected <= labelled
