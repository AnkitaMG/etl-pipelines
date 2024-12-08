import logging
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)

def load_data(
        spark: SparkSession,
        file_path: str,
        file_format: str
) -> DataFrame:
    """
    Loads CSV files from the given path into a DataFrame.

    :parameter:
        spark (SparkSession): The active Spark session.
        file_path (str): The path where CSV files are located.
        file_format (str): The format of the files.

    :return:
        DataFrame: The loaded DataFrame containing data from CSV files.
    """
    try:
        if file_format.lower() == "csv":
            return spark.read.csv(file_path + "*.csv", header=True)
        elif file_format.lower() == "parquet":
            # option to ensure schema evolution & to read files dynamically
            return spark.read.option("mergeSchema", "true").parquet(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    except Exception as ex:
        logger.error(f"An error occurred while loading data: {ex}")
        raise


def sink_data(
        final_df: DataFrame,
        path: str,
        partition_col: str,
        write_mode
):
    """
    Writes the DataFrame to the specified path in Parquet format, partitioned by the specified column.

    :parameter:
        final_df (DataFrame): The DataFrame to be written.
        path (str): The location where the DataFrame will be stored.
        partition_col (str): The column by which to partition the data.
        mode (str): The overwrite mode.
    """
    try:
        final_df.write.partitionBy(partition_col) \
                .mode(write_mode) \
                .parquet(path)
        logger.info(f"Data successfully written to {path}, partitioned by {partition_col}, with mode: {write_mode}.")
    except Exception as ex:
        logger.error(f"An error occurred while writing to Parquet: {ex}")
        raise