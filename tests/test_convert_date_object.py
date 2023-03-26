from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import convert_date_object

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

@pytest.fixture
def ref_dataframe():
    data = [
        [1,"a",20, "aa", "01/01/2023"],
        [2,"b",21, "bb", "2023-01-01"],
        [3,"c",22, "cc", "2023-01-01"],
        [4,"d",23, "dd", "01/01/2023"],
        [5,"e",24, "ee", "01/01/2023"],
    ]

    df = spark.createDataFrame(data, ['id','name','age', "address", "session_date"])
    return df


def test_if_formated_string_gets_converted_to_date(ref_dataframe):
    current_date_format = 'dd/MM/yyyy'
    df = convert_date_object(ref_dataframe, "session_date", current_date_format)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure the session_date column is of DateType
    assert df.dtypes[4][1] == 'date'

    #Make sure there are no null values in session_date column
    assert df.filter(df.session_date.isNull()).count() == 0

    df.show()


def test_with_multiple_formats(ref_dataframe):
    current_date_format = 'dd/MM/yyyy'

    data = [
        [6,"f",26, "ff", "01-01-2023"],
    ]

    df1 = spark.createDataFrame(data, ['id','name','age', "address", "session_date"])
    ref_dataframe = ref_dataframe.union(df1)

    df = convert_date_object(ref_dataframe, "session_date", current_date_format)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure the session_date column is of DateType
    assert df.dtypes[4][1] == 'date'

    # Test if the value for the extra formated string is null
    assert df.filter(df.id == 6).select('session_date').collect()[0]["session_date"] == None

    df.show()
