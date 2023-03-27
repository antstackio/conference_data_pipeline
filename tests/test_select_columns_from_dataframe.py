from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import select_columns_from_dataframe

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

@pytest.fixture
def ref_dataframe():
    data = [
        [1,"a",20, "aa"],
        [2,"b",21, "bb"],
        [3,"c",22, "cc"],
        [4,"d",23, "dd"],
        [5,"e",24, "ee"],
    ]

    df = spark.createDataFrame(data, ['id','name','age', "address"])
    return df


def test_with_valid_columns(ref_dataframe):
    column_list = ['id','name','age']

    df = select_columns_from_dataframe(ref_dataframe, column_list)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure the required columns returned
    assert set(df.columns) == set(['id','name','age'])

    #Make sure all records are returned
    assert df.count() == 5
    
    df.show()


def test_with_invalid_columns(ref_dataframe):
    column_list = ['id','name','marks']

    with pytest.raises(Exception) as e_info:
        df = select_columns_from_dataframe(ref_dataframe, column_list)