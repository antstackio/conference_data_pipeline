from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import union_dataframes

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()


def test_with_same_schema():
    data_1 = [
        [1,"a",20],
        [2,"b",21],
        [3,"c",22],
        [4,"d",23],
        [5,"e",24],
    ]

    df1 = spark.createDataFrame(data_1, ['id','name','age'])

    data_2 = [
        [6, "f", 40],
        [7, "g", 41],
    ]

    df2 = spark.createDataFrame(data_2, ['id','name','age'])

    df = union_dataframes(df1,df2)

    assert isinstance(df, DataFrame)
    assert df.count() == 7