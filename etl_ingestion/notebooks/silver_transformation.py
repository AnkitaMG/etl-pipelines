import logging
import dbutils
from datetime import datetime

import pyspark.sql.functions as F

from etl_ingestion.src import initializer
from etl_ingestion.src.compute import (
    data_io,
    transforms
)

logger = logging.getLogger(__name__)

dbutils.widgets.text("BRONZE_PATH", "s3://data-lake/bronze/retail_sales/")
bronze_path = dbutils.widgets.get("BRONZE_PATH")
logger.info(f"Bronze path: {bronze_path}")

dbutils.widgets.text("SILVER_PATH", "s3://data-lake/silver/retail_sales/")
silver_path = dbutils.widgets.get("SILVER_PATH")
logger.info(f"Silver path: {silver_path}")

dbutils.widgets.text("RUN_DATE", "")
run_date = dbutils.widgets.get("RUN_DATE")
logger.info(f"Run date: {run_date}")

if not run_date:
    run_date = datetime.now().strftime('%Y%m%d')

# Initializing Spark session
spark, dbutils = initializer.spark_and_dbutils()
start_time = initializer.start_processing(spark, dbutils)

# Loading data from Bronze layer
bronze_df = data_io.load_data(spark, bronze_path, "parquet").filter(F.col("run_date") == run_date)

# Adding metadata columns: source file path, execution datetime
metadata_df = transforms.metadata_addition(bronze_df)

# Applying transformations
snakecased_df = transforms.snake_case_conversion(metadata_df)

# Standardizing date formats for columns
standardized_df = transforms.standardize_date_columns(snakecased_df, ["order_date", "ship_date"])

# Splitting customer names
splitted_df = transforms.split_customer_name(standardized_df)

# Writing transformed data to Silver layer
data_io.sink_data(splitted_df, silver_path, "order_date", "overwrite")

initializer.finish_processing(spark, start_time)
logger.info("Silver Layer Transformation Completed!")