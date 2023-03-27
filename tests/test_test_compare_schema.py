from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

from src.main import compare_schema, convert_schema_to_list

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

@pytest.fixture
def ref_schema():
    data = [
        [1,"a",20, "aa"],
        [2,"b",21, "bb"],
        [3,"c",22, "cc"],
        [4,"d",23, "dd"],
        [5,"e",24, "ee"],
    ]

    df = spark.createDataFrame(data, ['id','name','age', "address"])
    return [i.name for i in df.schema]


def test_with_matching_schema(ref_schema):
    filename = 'filename'

    struct_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("address", StringType(), True),
        ]
    )

    defined_schema = convert_schema_to_list(struct_schema)

    result = compare_schema(ref_schema, defined_schema, filename)

    # Make sure schemas match
    assert result["valid_schema"] == True

    # Make sure there are no missing columns
    assert result["missing_columns"] == None

    # Make sure the filename is correct
    assert result['filename'] == filename


def test_with_less_columns(ref_schema):
    filename = 'filename'

    struct_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    defined_schema = convert_schema_to_list(struct_schema)

    result = compare_schema(ref_schema, defined_schema, filename)

    # Make sure infered shema is valid
    assert result["valid_schema"] == True

    # Make sure there are no missing columns
    assert result["missing_columns"] == None

    # Make sure the filename is correct
    assert result['filename'] == filename


def test_with_more_columns(ref_schema):
    filename = 'filename'

    struct_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("marks", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("grade", StringType(), True),
        ]
    )

    defined_schema = convert_schema_to_list(struct_schema)

    result = compare_schema(ref_schema, defined_schema, filename)

    # Make sure schemas doesn't match
    assert result["valid_schema"] == False

    # Make sure the missing columns are recognized
    assert set(result["missing_columns"]) == set(['marks', 'grade']) 

    # Make sure the filename is correct
    assert result['filename'] == filename