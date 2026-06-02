from datetime import date

from ingestion.generate_playback_events import EVENT_TYPES, generate_events


def test_generates_requested_number_of_events():
    events = generate_events(
        day=date(2024, 1, 1), n=100, movie_ids=[1, 2, 3], user_ids=[10, 11], seed=42
    )
    assert len(events) == 100


def test_events_have_required_schema():
    e = generate_events(day=date(2024, 1, 1), n=1, movie_ids=[1], user_ids=[10], seed=1)[0]
    assert set(e.keys()) == {
        "event_id",
        "user_id",
        "movie_id",
        "event_type",
        "event_ts",
        "device",
        "position_seconds",
    }
    assert e["event_type"] in EVENT_TYPES
    assert e["movie_id"] == 1 and e["user_id"] == 10


def test_is_deterministic_for_a_seed():
    a = generate_events(day=date(2024, 1, 1), n=50, movie_ids=[1, 2], user_ids=[10, 11], seed=7)
    b = generate_events(day=date(2024, 1, 1), n=50, movie_ids=[1, 2], user_ids=[10, 11], seed=7)
    assert a == b


def test_all_timestamps_fall_on_the_requested_day():
    events = generate_events(day=date(2024, 3, 5), n=200, movie_ids=[1], user_ids=[10], seed=3)
    assert all(e["event_ts"].date() == date(2024, 3, 5) for e in events)
