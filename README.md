# etl-ingestion

## Case
A retail sales dataset was made available to new data engineer to ingest into the company's Data lake.
It is expected that some business rules will be added to the final dataset, in addition to some transformations.
This dataset may contain new attributes in the short term, requiring your pipeline to be flexible to support this schema evolution and display it transparently to users.

1. ETL pipeline: to assess your level with coding ETL pipeline in real-life scenarios using
PySpark.

## Description

* The Bronze layer Ingestion starting point is
```
bronze_ingestion.py
Path: etl_ingestion/notebooks/bronze_ingestion.py
```
* The Silver layer Transformation starting point is
```
silver_transformation.py
Path: etl_ingestion/notebooks/silver_transformation.py
```
* The Gold layer Aggregation/Segregation for Sales & customer Dataset starting point is
```
gold_transformations.py
Path: etl_ingestion/notebooks/gold_transformations.py
```

* Test folder written in pytest : test
* Test data folders with csv file : data

## Used technologies

* [Python](https://www.python.org/ "Official Python website") - by Spark supported language
* [Poetry](https://python-poetry.org// "Official Poetry website") - build system for Python and dependency management
* [Apache Spark](https://spark.apache.org/ "Official Spark Spark website") - distributed multi-language runtime engine optimized for data
  processing
* [Azure Databricks](https://www.databricks.com/ "Official Azure Databricks website") - managed Spark service in Azure

### Prerequisites
All you need is the following configuration already installed:

* Oracle/Open JDK version = 8
* Python version >=3.12
* Poetry version >=1.8
* Pyspark version >=3.2.0
* Pytest version >=7.1.0

## How to build / poetry commands

* Install Dependencies
```shell
poetry install
```
* Run all Test
```shell
poetry run pytest
```
* Run specific Tests
```shell
poetry run pytest tests/compute/test_transforms.py -s
```
* Run a particular test function within a file
```shell
poetry run pytest tests/compute/test_transforms.py::test_split_customer_name -s
```
* Build python wheel
```shell
poetry build
```