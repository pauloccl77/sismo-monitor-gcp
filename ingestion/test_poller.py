import os

os.environ.setdefault("GCP_PROJECT_ID", "ci-test-project")

from poller import build_message, is_in_chile  # noqa: E402


def test_is_in_chile_true_for_valparaiso():
    assert is_in_chile(lon=-71.6, lat=-33.0) is True


def test_is_in_chile_false_outside_bounding_box():
    assert is_in_chile(lon=-58.4, lat=-34.6) is False  # Buenos Aires


def _feature(mag=5.2, updated=1700000001000):
    return {
        "id": "us1234abcd",
        "properties": {
            "mag": mag,
            "place": "45km NW of Valparaiso, Chile",
            "time": 1700000000000,
            "updated": updated,
            "url": "https://earthquake.usgs.gov/test",
        },
        "geometry": {"coordinates": [-71.6, -33.0, 23.4]},
    }


def test_build_message_maps_fields():
    msg = build_message(_feature())

    assert msg["id"] == "us1234abcd"
    assert msg["magnitude"] == 5.2
    assert msg["lon"] == -71.6
    assert msg["lat"] == -33.0
    assert msg["depth_km"] == 23.4
    assert msg["timestamp_event"] == 1700000000000
    assert msg["timestamp_updated"] == 1700000001000


def test_build_message_handles_null_magnitude():
    msg = build_message(_feature(mag=None))

    assert msg["magnitude"] is None
