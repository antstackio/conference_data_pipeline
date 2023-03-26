from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import union_dataframes

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()



@pytest.fixture
def ref_dataframe():
    data = [
        [1,"a",20],
        [2,"b",21],
        [3,"c",22],
        [4,"d",23],
        [5,"e",24],
    ]

    df = spark.createDataFrame(data, ['id','name','age'])
    return df
                    

def test_with_same_schema(ref_dataframe):
    data = [
        [6, "f", 40],
        [7, "g", 41],
    ]

    df2 = spark.createDataFrame(data, ['id','name','age'])

    df = union_dataframes(ref_dataframe,df2)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)
    
    # Make sure the count of the union df matches the sum of count
    assert df.count() == 7

    df.show()



def test_with_different_schema(ref_dataframe):
    data = [
        [6, "f", "ff", 40],
        [7, "g", "gg", 41],
    ]

    df2 = spark.createDataFrame(data, ['id','name', 'address', 'age'])

    with pytest.raises(Exception) as e_info:
        df = union_dataframes(ref_dataframe,df2)

    print(e_info)