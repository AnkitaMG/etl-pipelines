from datetime import datetime, timedelta

from etl_ingestion.src.utils import (
    to_snake_case,
    find_date_ranges
)

def test_to_snake_case():
    test_cases = [
        ("ColumnName", "column_name"),
        ("columnName", "column_name"),
        ("Column Name", "column_name"),
        ("column name", "column_name"),
        ("simplecolumn", "simplecolumn")
    ]

    for input_column, expected_output in test_cases:
        assert to_snake_case(input_column) == expected_output, \
            f"Failed for input: '{input_column}'. Expected: '{expected_output}', Found: '{to_snake_case(input_column)}'"


def test_find_date_ranges():
    latest_date = "2024-12-07"
    expected_last_month = datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=30)
    expected_last_six_months = datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=180)
    expected_last_twelve_months = datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=365)

    result = find_date_ranges(latest_date)

    # Asserts
    assert result["last_month"] == expected_last_month, \
        f"Failed for last_month: Expected {expected_last_month}, Found {result['last_month']}"
    assert result["last_six_months"] == expected_last_six_months, \
        f"Failed for last_six_months: Expected {expected_last_six_months}, Found {result['last_six_months']}"
    assert result["last_twelve_months"] == expected_last_twelve_months, \
        f"Failed for last_twelve_months: Expected {expected_last_twelve_months}, Found {result['last_twelve_months']}"
