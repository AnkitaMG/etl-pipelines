import contextlib
import datetime
import time
from collections.abc import Iterator
from typing import Any

from pyspark.sql import SparkSession

def spark_and_dbutils() -> tuple[SparkSession, Any]:
    """
    Returns `(spark, dbutils)` tuple when executed inside databricks notebook.

    This way, if you add `spark, dbutils = spark_and_dbutils()` at the top of your notebook,
    various tools won't complain about those variables being undefined.
    """
    try:
        import IPython

        user_ns = IPython.get_ipython().user_ns
        return user_ns["spark"], user_ns["dbutils"]
    except (ImportError, KeyError):
        raise NotImplementedError(
            "spark_and_dbutils() called outside of databricks notebook"
        ) from None


def initialize(spark: SparkSession, dbutils: Any) -> None:
    """
    Set up AWS credentials for Spark to access S3 and other AWS services.
       - Fetch AWS credentials from Databricks secrets or environment variables
       - Configure Spark with the required AWS credentials
    """
    # Get secrets
    aws_access_key_id = dbutils.secrets.get("aws-secrets", "AWS_ACCESS_KEY_ID")
    aws_secret_access_key = dbutils.secrets.get("aws-secrets", "AWS_SECRET_ACCESS_KEY")

    # Setting Spark conf for AWS S3 access
    spark.conf.set("fs.s3a.access.key", aws_access_key_id)
    spark.conf.set("fs.s3a.secret.key", aws_secret_access_key)
    spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("fs.s3a.connection.maximum", "1000")
    spark.conf.set("fs.s3a.path.style.access", "true")

    spark.conf.set("spark.databricks.io.cache.enabled", "true")
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Enable schema merging while reading from Parquet
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")


def start_processing(spark: SparkSession, dbutils: Any) -> float:
    start = time.perf_counter()
    initialize(spark, dbutils)
    return start

def finish_processing(spark: SparkSession, start_time: float) -> None:
    end = datetime.timedelta(seconds=time.perf_counter() - start_time)
    print(f"Finished in {end}")
    spark.catalog.clearCache()

@contextlib.contextmanager
def context(spark: SparkSession, dbutils: Any) -> Iterator[None]:
    start = start_processing(spark, dbutils)
    try:
        yield
    finally:
        finish_processing(spark, start)