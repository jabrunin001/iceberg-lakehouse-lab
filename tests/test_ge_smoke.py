from quality.ge_validate import build_suite


def test_suite_has_core_expectations():
    names = {e["expectation_type"] for e in build_suite()}
    assert "expect_column_values_to_be_between" in names
    assert "expect_table_row_count_to_be_between" in names
    assert "expect_column_values_to_not_be_null" in names
