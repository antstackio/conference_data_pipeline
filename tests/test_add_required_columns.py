from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import add_required_columns

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


def test_if_required_columns_get_added(ref_dataframe):
    current_user = 'my_user'
    df = add_required_columns(ref_dataframe, current_user)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure the required columns were added
    assert set(df.columns) == set(['id','name','age', 'address',  'file_name', 'create_user', 'create_date', 'modified_user', 'modified_date', 'is_processed'])

    #Make sure value of create_user is set correct
    assert df.select('create_user').collect()[0]['create_user'] == current_user

    #Make sure is_processed is set to false
    assert df.filter(df.is_processed == True).count() == 0
