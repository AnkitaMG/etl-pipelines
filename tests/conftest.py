import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    spark = (
        SparkSession.builder.master("local[1]") # local machine with 1 thread
        .appName("local-tests")
        .config("spark.executor.cores", "1") # set number of cores
        .config("spark.executor.instances", "1") # set executors to one
        .config("spark.sql.shuffle.partitions", "1") # set max number of partitions to one
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    yield spark
    spark.stop()