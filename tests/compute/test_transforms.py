import pyspark.sql.functions as F

from etl_ingestion.src.utils import to_snake_case
from etl_ingestion.src.compute.transforms import (
    metadata_addition,
    snake_case_conversion,
    standardize_date_columns,
    split_customer_name
)


def test_metadata_addition_csv(test_retail_sales_df):
    run_date = "20241207"
    test_retail_sales = test_retail_sales_df.withColumn("run_date", F.lit(run_date))

    metadata_df = metadata_addition(test_retail_sales)
    expected_columns = test_retail_sales.columns + ['source_file_path', 'execution_datetime', 'run_date']

    assert set(metadata_df.columns) == set(expected_columns), \
        f"Columns do not match. Expected: {expected_columns}, Found: {metadata_df.columns}"

    assert metadata_df.filter(F.col("source_file_path").isNotNull()).count() == metadata_df.count(), \
        "Some rows have null 'source_file_path'"

    assert metadata_df.filter(F.col("run_date") == run_date).count() == metadata_df.count(), \
        "The 'run_date' column does not contain the correct run date"


def test_metadata_addition_parquet(test_bronze_df):
    # Test data in bronze folder is for 07 Dec 2024
    run_date = "20241207"
    test_bronze = test_bronze_df.filter(F.col("run_date") == run_date)

    metadata_df = metadata_addition(test_bronze)
    expected_columns = metadata_df.columns

    assert set(metadata_df.columns) == set(expected_columns)

    assert metadata_df.filter(F.col("source_file_path").isNotNull()).count() == metadata_df.count(), \
        "Some rows have null 'source_file_path'"

    assert metadata_df.filter(F.col("run_date") == run_date).count() == metadata_df.count(), \
        "The 'run_date' column does not contain the correct run date"


def test_snake_case_conversion(test_bronze_df):
    # Test data in bronze folder is for 07 Dec 2024
    run_date = "20241207"
    test_bronze = test_bronze_df.filter(F.col("run_date") == run_date)
    metadata_df = metadata_addition(test_bronze)

    original_columns = test_bronze.columns
    expected_columns = [to_snake_case(col) for col in original_columns]

    snake_case_df = snake_case_conversion(metadata_df)
    transformed_columns = snake_case_df.columns

    assert transformed_columns == expected_columns, \
        f"Columns do not match. Expected: {expected_columns}, Found: {transformed_columns}"

    assert snake_case_df.count() == metadata_df.count(), \
        "Row count mismatch after snake_case conversion."

    assert snake_case_df.collect() == metadata_df.collect(), \
        "Data mismatch after snake_case conversion."


def test_standardize_date_columns(test_bronze_df):
    # Test data in bronze folder is for 07 Dec 2024
    run_date = "20241207"
    test_bronze = test_bronze_df.filter(F.col("run_date") == run_date)
    metadata_df = metadata_addition(test_bronze)
    snake_case_df = snake_case_conversion(metadata_df)

    # date columns and expected format
    date_columns = ["order_date", "ship_date"]
    expected_date_format = "yyyy-MM-dd"

    # Applying the date standardization
    standardized_df = standardize_date_columns(snake_case_df, date_columns)

    # Asserts that all values in the date columns are in the correct format
    for col in date_columns:
        invalid_dates = standardized_df.filter(~F.col(col).rlike(r"^\d{4}-\d{2}-\d{2}$"))
        assert invalid_dates.count() == 0, \
            f"Column '{col}' contains invalid dates. Expected format: {expected_date_format}"

    # Asserts that the schema includes the date columns
    for col in date_columns:
        assert col in standardized_df.columns, \
            f"Column '{col}' missing in the standardized DataFrame"

    # Asserts row counts are unchanged
    assert standardized_df.count() == snake_case_df.count(), \
        "Row count mismatch after date standardization"

    # data_io.sink_data(standardized_df, "file:///Users/ankitamanral/Documents/pp/learnings/etl-ingestion/data/output/silver/", "run_date", "overwrite")


def test_split_customer_name(test_bronze_df):
    # Test data in bronze folder is for 07 Dec 2024
    run_date = "20241207"
    test_bronze = test_bronze_df.filter(F.col("run_date") == run_date)
    metadata_df = metadata_addition(test_bronze)
    snake_case_df = snake_case_conversion(metadata_df)
    standardized_df = standardize_date_columns(snake_case_df, ["order_date", "ship_date"])

    # Apply the split_customer_name function
    split_df = split_customer_name(standardized_df)

    # Assert that the 'customer_first_name' and 'customer_last_name' columns exist in the schema
    expected_columns = ["customer_first_name", "customer_last_name"]
    for col in expected_columns:
        assert col in split_df.columns, \
            f"Column '{col}' missing in the split DataFrame"

    # Assert that the row count remains the same after the transformation
    assert split_df.count() == standardized_df.count(), \
        "Row count mismatch after customer name splitting"


