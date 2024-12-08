from chispa.schema_comparer import assert_schema_equality
from chispa.dataframe_comparer import assert_df_equality

from etl_ingestion.src.compute.aggregations import compute_order_quantities
from etl_ingestion.src.utils import find_date_ranges

def test_compute_order_quantities(test_silver_df, expected_aggregations_df):
    date_ranges = find_date_ranges("2018-12-30")
    result_df = compute_order_quantities(test_silver_df, date_ranges)
    result_df = result_df.select(*expected_aggregations_df.columns)

    # Ensure the schema matches, ignoring nullability
    assert_schema_equality(result_df.schema, expected_aggregations_df.schema, ignore_nullable=True)

    # Assert the DataFrames are equal (ignoring row order)
    assert_df_equality(
        result_df,
        expected_aggregations_df,
        ignore_nullable=True,
        ignore_column_order=True,
        ignore_row_order=True
    )