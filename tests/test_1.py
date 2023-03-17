from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()


def test_read_file_from_src_path():
    a,b = 1,1
    assert a == b