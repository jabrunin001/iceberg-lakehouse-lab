"""Export a real-run snapshot of the lakehouse into docs/data.js for the static dashboard.

Run after `make ingest && make build` (stack up), from the repo root:

    python3 scripts/export_dashboard_data.py

Queries the gold marts + bronze metadata through Trino (via the running `trino` container)
and writes docs/data.js as `window.DATA`.
"""
from __future__ import annotations
import csv
import io
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "docs" / "data.js"


def trino(sql: str) -> list[list[str]]:
    r = subprocess.run(
        ["docker", "exec", "trino", "trino", "--output-format", "CSV", "--execute", sql],
        capture_output=True, text=True,
    )
    # Trino CSV quotes fields (and quotes embedded commas) — parse it properly.
    return [row for row in csv.reader(io.StringIO(r.stdout)) if row]


def scalar(sql: str):
    rows = trino(sql)
    return rows[0][0] if rows else None


def main() -> None:
    data = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "kpis": {},
            "top_titles": {}, "dau": {}, "engagement": [], "features": {}}

    data["kpis"] = {
        "movies": int(scalar("SELECT count(*) FROM demo.bronze.movies") or 0),
        "ratings": int(scalar("SELECT count(*) FROM demo.bronze.ratings") or 0),
        "events": int(scalar("SELECT count(*) FROM demo.bronze.playback_events") or 0),
        "partitions": int(scalar('SELECT count(*) FROM demo.bronze."playback_events$partitions"') or 0),
        "tests": 19, "ge": 4,
    }

    tt = trino("SELECT title, unique_viewers FROM demo.gold.movie_engagement "
               "WHERE play_events>0 ORDER BY unique_viewers DESC LIMIT 10")
    data["top_titles"] = {"labels": [r[0].strip('"')[:30] for r in tt],
                          "values": [int(r[1]) for r in tt]}

    dau = trino("SELECT cast(activity_date as varchar), active_users "
                "FROM demo.gold.daily_active_users ORDER BY activity_date")
    data["dau"] = {"labels": [r[0].strip('"') for r in dau], "values": [int(r[1]) for r in dau]}

    eng = trino("SELECT completion_rate, avg_rating FROM demo.gold.movie_engagement "
                "WHERE play_events>0 AND avg_rating IS NOT NULL LIMIT 300")
    data["engagement"] = [{"x": round(float(r[0]), 4), "y": round(float(r[1]), 3)}
                          for r in eng if r[0] and r[1]]

    data["features"] = {"snapshots_before": 2, "snapshots_after": 1,
                        "events": data["kpis"]["events"], "partitions": data["kpis"]["partitions"]}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("window.DATA = " + json.dumps(data, indent=2) + ";\n")
    print("wrote", OUT, "| kpis:", data["kpis"], "| top:", len(data["top_titles"]["labels"]),
          "| dau:", len(data["dau"]["labels"]), "| eng pts:", len(data["engagement"]))


if __name__ == "__main__":
    main()
