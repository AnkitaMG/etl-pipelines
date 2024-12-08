import logging
import dbutils
from datetime import datetime

import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

dbutils.widgets.text("SOURCE_PATH", "s3://data-lake/source/retail_sales/")
source_path = dbutils.widgets.get("SOURCE_PATH")
logger.info(f"Source path: {source_path}")

dbutils.widgets.text("BRONZE_PATH", "s3://data-lake/bronze/retail_sales/")
bronze_path = dbutils.widgets.get("BRONZE_PATH")
logger.info(f"Bronze path: {bronze_path}")

print("source_path: ", source_path)
print("bronze_path: ", bronze_path)


from etl_ingestion.src import initializer
from etl_ingestion.src.compute import (
    data_io,
    transforms
)

# Initializing Spark session
spark, dbutils = initializer.spark_and_dbutils()
start_time = initializer.start_processing(spark, dbutils)

# current date for partitioning in bronze layer
run_date = datetime.now().strftime('%Y%m%d')

# Loading retails sales csv data into a DataFrame from AWS S3 source path
retail_sales_df = data_io.load_data(spark, source_path, "csv").withColumn("run_date", F.lit(run_date))

# Adding metadata columns: source file path, execution datetime
metadata_df = transforms.metadata_addition(retail_sales_df)

# Writing into Bronze layer i.e Raw Data with added metadata information partitioned by run_date
data_io.sink_data(metadata_df, bronze_path, "run_date", "append")

initializer.finish_processing(spark, start_time)
print("Bronze Layer Ingestion Completed!")