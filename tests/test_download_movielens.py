from pathlib import Path

from ingestion.download_movielens import expected_files, target_dir


def test_expected_files_are_the_four_movielens_csvs():
    assert set(expected_files()) == {"movies.csv", "ratings.csv", "tags.csv", "links.csv"}


def test_target_dir_is_under_data_raw():
    assert target_dir() == Path("data/raw/movielens")
