import logging
from datetime import datetime

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

from etl_ingestion.src.utils import to_snake_case


def metadata_addition(
        df: DataFrame
) -> DataFrame:
    """
    Adds metadata columns to the DataFrame: source file path, execution datetime.

    :parameter:
        df (DataFrame): The DataFrame to which metadata will be added.

    :return:
        df (DataFrame): The DataFrame with the added metadata columns.
    """
    try:
        execution_datetime = datetime.now()
        metadata_df = df.withColumn("source_file_path", F.input_file_name()) \
            .withColumn("execution_datetime", F.lit(execution_datetime))

        return metadata_df
    except Exception as ex:
        logger.error(f"An error occurred while adding metadata columns: {ex}")
        raise


def snake_case_conversion(
        df: DataFrame
) -> DataFrame:
    """
    Renames the columns of the DataFrame to snake_case format.

    :parameter:
        df (DataFrame): The DataFrame whose columns need to be renamed.

    :return:
        DataFrame: The DataFrame with columns renamed to snake_case format.
    """
    try:
        renamed_columns = [F.col(col).alias(to_snake_case(col)) for col in df.columns]

        return df.select(*renamed_columns)
    except Exception as ex:
        logger.error(f"An error occurred while renaming columns: {ex}")
        raise


def standardize_date_columns(
        df: DataFrame,
        date_columns: list,
        date_format: str = "yyyy-MM-dd"
) -> DataFrame:
    """
    Standardizes date formats in specified columns of a DataFrame.

    :parameter:
        df (DataFrame): The DataFrame with date columns to standardize.
        date_columns (list): A list of column names containing dates to be standardized.
        date_format (str): The desired date format (default is 'yyyy-MM-dd').

    :return:
        DataFrame: The DataFrame with standardized date formats for the specified columns.
    """
    try:
        for col in date_columns:
            df = df.withColumn(
                col,
                F.date_format(F.to_date(F.col(col), "dd/MM/yyyy"), date_format)
            )
        return df
    except Exception as ex:
        logger.error(f"An error occurred while standardizing date columns: {ex}")
        raise


def split_customer_name(
        df: DataFrame
) -> DataFrame:
    """
    Splits a single column containing customer full names into separate first and last name columns.

    :parameter:
        df: Input DataFrame

    :return:
        DataFrame: Transformed DataFrame with first and last name columns
    """
    return (
        df.withColumn("customer_first_name", F.split(F.col("customer_name"), " ").getItem(0))
          .withColumn("customer_last_name", F.split(F.col("customer_name"), " ").getItem(1))
        .drop("customer_name")
    )