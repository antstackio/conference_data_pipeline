from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import create_timestamp_str

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

@pytest.fixture
def ref_dataframe():
    data = [
        [1,"a",20, "aa", "01/01/2023", "11:0"],
        [2,"b",21, "bb", "01/01/2023", "10:41"],
        [3,"c",22, "cc", "01/01/2023", "10:46"],
        [4,"d",23, "dd", "01/01/2023", "10:41"],
        [5,"e",24, "ee", "01/01/2023", "10:58"],
    ]

    df = spark.createDataFrame(data, ['id','name','age', "address", "session_date", "login_time"])
    return df


def test_new_column_is_generated(ref_dataframe):
    new_column = 'login_time_'
    df = create_timestamp_str(ref_dataframe, "session_date", "login_time", new_column)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure the new column has bee added
    assert 'login_time_' in df.columns

    #Make sure new column has the expected result
    assert df.filter(df.id == 1).select('login_time_').collect()[0]['login_time_'] == "01/01/2023 11:0"

    df.show()