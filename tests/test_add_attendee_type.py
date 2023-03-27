from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import add_attendee_type

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
    attendee_type = 'my_user'
    df = add_attendee_type(ref_dataframe, attendee_type)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure the attendee_type column is added
    assert 'attendee_type' in df.columns

    #Make sure value of attendee_type is set correct
    assert df.select('attendee_type').collect()[0]['attendee_type'] == attendee_type

    df.show()

