from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.snowflake_credentials import snowflake_options

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()



def test_raw_environment():
    result = snowflake_options("raw")

    # Make sure out is correct
    assert result["sfSchema"] == "CORE"

    print(result)


def test_non_raw_environment(): 
    result = snowflake_options("refined")

    # Make sure out is correct
    assert result["sfSchema"] == "CONFORMED"

    print(result)
