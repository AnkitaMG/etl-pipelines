import os
import pytest

@pytest.fixture
def test_retail_sales_df(spark, request):
    rootdir = request.config.rootdir
    # Constructing the path to the test data file
    retail_sales_path = os.path.join(rootdir, 'data', 'input', 'raw')
    return spark.read.csv(retail_sales_path, header=True, inferSchema=True)


@pytest.fixture
def test_bronze_df(spark, request):
    rootdir = request.config.rootdir
    # Constructing the path to the test data file
    bronze_path = os.path.join(rootdir, 'data', 'input', 'bronze')
    return spark.read.parquet(bronze_path)


@pytest.fixture
def test_silver_df(spark, request):
    rootdir = request.config.rootdir
    # Constructing the path to the test data file
    silver_path = os.path.join(rootdir, 'data', 'input', 'silver')
    return spark.read.parquet(silver_path)


@pytest.fixture
def expected_aggregations_df(spark, request):
    rootdir = request.config.rootdir
    # Constructing the path to the test data file
    aggregated_path = os.path.join(rootdir, 'data', 'output', 'aggregated.json')
    return spark.read.json(aggregated_path)