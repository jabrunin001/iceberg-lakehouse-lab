"""Great Expectations validation of the gold movie_engagement table.

Reads the table via Trino (DBAPI) into pandas and validates it. Keeps GE decoupled
from Spark so it runs on the host venv and in CI.
"""
from __future__ import annotations

import sys


def build_suite() -> list[dict]:
    """The expectation set for gold.movie_engagement (also unit-testable)."""
    return [
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1, "max_value": 100000},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "movie_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "completion_rate", "min_value": 0, "max_value": 1},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "avg_rating",
                "min_value": 0.5,
                "max_value": 5.0,
                "mostly": 1.0,
                "row_condition": "avg_rating == avg_rating",
                "condition_parser": "pandas",
            },
        },
    ]


def _load_table():
    import pandas as pd

    from trino.dbapi import connect

    conn = connect(host="localhost", port=8085, user="ge", catalog="demo", schema="gold")
    return pd.read_sql("SELECT * FROM demo.gold.movie_engagement", conn)


def validate() -> bool:
    import great_expectations as gx

    df = _load_table()
    ctx = gx.get_context(mode="ephemeral")
    validator = ctx.sources.add_pandas("gold").read_dataframe(
        df, asset_name="movie_engagement"
    )
    for exp in build_suite():
        getattr(validator, exp["expectation_type"])(**exp["kwargs"])
    result = validator.validate()
    print(
        f"GE success={result.success}; "
        f"{result.statistics['successful_expectations']}/"
        f"{result.statistics['evaluated_expectations']} expectations passed"
    )
    return bool(result.success)


if __name__ == "__main__":
    sys.exit(0 if validate() else 1)
