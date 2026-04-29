"""
Phase 1 smoke tests - no network required.

Run from project root:
    python -m pytest tests/ -v
or simply:
    python tests/test_smoke.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow tests to import from scripts/ without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))


def test_config_loads():
    import config
    assert config.PROJECT_ROOT.exists()
    assert config.RAW_TRIPS_DIR.is_dir()
    assert config.ZONES_DIR.is_dir()
    assert config.WEATHER_DIR.is_dir()


def test_logger_creates_handlers():
    from logger import get_logger
    log = get_logger("test_smoke_logger")
    assert len(log.handlers) >= 2  # console + file
    log.info("smoke test - this should appear in logs/test_smoke_logger.*.log")


def test_download_tlc_month_parsing():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    from download_tlc import _parse_month, _month_iter, _latest_available_month
    from datetime import date

    assert _parse_month("2024-03") == (2024, 3)
    assert _parse_month("2019-02") == (2019, 2)

    bad_inputs = ["2024-13", "abc", "2024", "2024/03"]
    for s in bad_inputs:
        try:
            _parse_month(s)
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {s!r}")

    months = list(_month_iter((2024, 11), (2025, 2)))
    assert months == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]

    # Check lag behavior wraps year boundary correctly
    latest = _latest_available_month(date(2025, 2, 15))
    assert latest == (2024, 12)


def test_weather_payload_parser():
    from fetch_weather import _parse_owm_payload
    payload = {
        "dt": 1714000000,  # arbitrary unix ts
        "main": {"temp": 22.5, "humidity": 60},
        "wind": {"speed": 3.4},
        "weather": [{"main": "Clouds"}],
        "rain": {"1h": 0.5},
    }
    rec = _parse_owm_payload(payload, "KNYC")
    assert rec["station_id"] == "KNYC"
    assert rec["temperature_c"] == 22.5
    assert rec["humidity_pct"] == 60
    assert rec["wind_speed_ms"] == 3.4
    assert rec["weather_condition"] == "Clouds"
    assert rec["precipitation_mm"] == 0.5
    # observation_time is rounded to the hour
    assert rec["observation_time"].endswith("00:00+00:00")


def test_validate_filename_regex():
    from validate_data import _expected_month_from_name
    p = Path("/x/fhvhv_tripdata_2024-07.parquet")
    assert _expected_month_from_name(p) == (2024, 7)
    assert _expected_month_from_name(Path("/x/garbage.parquet")) is None


if __name__ == "__main__":
    # Tiny manual runner so this works without pytest.
    failures = 0
    funcs = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for fn in funcs:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except Exception as e:
            failures += 1
            print(f"FAIL  {fn.__name__}: {e}")
    sys.exit(failures)
