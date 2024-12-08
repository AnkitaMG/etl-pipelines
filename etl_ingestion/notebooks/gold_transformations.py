import logging
import dbutils

from etl_ingestion.src import initializer, utils
from etl_ingestion.src.compute import (
    data_io,
    aggregations
)

logger = logging.getLogger(__name__)

dbutils.widgets.text("SILVER_PATH", "s3://data-lake/silver/retail_sales/")
silver_path = dbutils.widgets.get("SILVER_PATH")
logger.info(f"Silver path: {silver_path}")

dbutils.widgets.text("GOLD_PATH", "s3://data-lake/gold/retail_sales/")
gold_path = dbutils.widgets.get("GOLD_PATH")
logger.info(f"Gold path: {gold_path}")

# Paths for the Sales and Customer datasets in the gold layer
sales_gold_path = f"{gold_path}/sales"
customer_gold_path = f"{gold_path}/customer"

dbutils.widgets.text("DATASET_LATEST_DATE", "2018-12-30")
latest_date = dbutils.widgets.get("DATASET_LATEST_DATE")
logger.info(f"Starting from dataset’s latest day which is 30th Dec 2018: {latest_date}")

# Initializing Spark session
spark, dbutils = initializer.spark_and_dbutils()
start_time = initializer.start_processing(spark, dbutils)

# Loading data from Silver layer
silver_df = data_io.load_data(spark, silver_path, "parquet")

# ------------------ Sales Dataset ------------------
sales_df = silver_df.select(
    "order_id", "order_date", "ship_date", "ship_mode", "city", "run_date", "source_file_path", "execution_datetime"
)

# Writing Sales Dataset to Gold layer
data_io.sink_data(sales_df, sales_gold_path, "order_date", "append")


# ------------------ Customer Dataset ------------------
# Finding the date ranges for last month, last 6 months, and last 12 months from a given latest date
date_ranges = utils.find_date_ranges(latest_date)

# Computing Customer Order Aggregations
customer_df = aggregations.compute_order_quantities(silver_df, date_ranges)

# Writing Customer Dataset to Gold layer
data_io.sink_data(customer_df, customer_gold_path, "customer_id", "overwrite")

# Finish the process
initializer.finish_processing(spark, start_time)
logger.info("Gold Layer Transformation Completed!")
