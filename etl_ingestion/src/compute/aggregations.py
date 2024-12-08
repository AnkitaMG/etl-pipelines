import logging

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

def compute_order_quantities(
        df: DataFrame,
        date_ranges: dict
) -> DataFrame:
    """
    Computes customer statistics based on order date and customer data.

    :parameter:
        df: The input DataFrame
        date_ranges: Dictionary containing date ranges for last month, last 6 months, and last 12 months

    :return:
        A DataFrame containing the aggregated orders statistics
    """
    return (
        df.groupBy("customer_id", "customer_first_name", "customer_last_name", "segment", "country")
        .agg(
            F.count(F.when(F.col("order_date") >= F.lit(date_ranges["last_month"].strftime('%Y-%m-%d')), 1)).alias(
                "orders_last_month"),
            F.count(F.when(F.col("order_date") >= F.lit(date_ranges["last_six_months"].strftime('%Y-%m-%d')), 1)).alias(
                "orders_last_6_months"),
            F.count(
                F.when(F.col("order_date") >= F.lit(date_ranges["last_twelve_months"].strftime('%Y-%m-%d')), 1)).alias(
                "orders_last_12_months"),
            F.count("*").alias("total_orders")
        )
    )