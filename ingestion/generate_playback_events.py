"""Deterministic synthetic playback-events generator.

Writes one daily batch as Parquet to data/raw/playback_events/date=YYYY-MM-DD/.
Determinism (seeded) keeps demos reproducible.
"""
from __future__ import annotations

import argparse
import random
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path

EVENT_TYPES = ("play", "pause", "seek", "complete")
DEVICES = ("tv", "mobile", "web", "tablet")


def generate_events(
    day: date, n: int, movie_ids: list[int], user_ids: list[int], seed: int
) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    for _ in range(n):
        secs = rng.randint(0, 86_399)
        ts = datetime.combine(day, time.min) + timedelta(seconds=secs)
        events.append(
            {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "user_id": rng.choice(user_ids),
                "movie_id": rng.choice(movie_ids),
                "event_type": rng.choice(EVENT_TYPES),
                "event_ts": ts,
                "device": rng.choice(DEVICES),
                "position_seconds": rng.randint(0, 7200),
            }
        )
    return events


def write_batch(
    day: date, n: int, movie_ids: list[int], user_ids: list[int], seed: int
) -> Path:
    import pandas as pd

    events = generate_events(day, n, movie_ids, user_ids, seed)
    out_dir = Path("data/raw/playback_events") / f"date={day.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "events.parquet"
    # Spark's Parquet reader rejects nanosecond timestamps; pandas/pyarrow default to
    # TIMESTAMP(NANOS). Coerce event_ts down to microseconds so Spark can read it.
    pd.DataFrame(events).to_parquet(
        out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True
    )
    return out


def _movie_ids_from_movielens() -> list[int]:
    import pandas as pd

    df = pd.read_csv("data/raw/movielens/movies.csv")
    return df["movieId"].tolist()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--per-day", type=int, default=2000)
    p.add_argument("--users", type=int, default=500)
    args = p.parse_args()

    movie_ids = _movie_ids_from_movielens()
    user_ids = list(range(1, args.users + 1))
    start = date.fromisoformat(args.start)
    for i in range(args.days):
        d = start + timedelta(days=i)
        out = write_batch(d, args.per_day, movie_ids, user_ids, seed=1000 + i)
        print(f"wrote {args.per_day} events for {d} -> {out}")
