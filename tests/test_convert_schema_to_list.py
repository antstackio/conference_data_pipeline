from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

from src.main import convert_schema_to_list

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()


def test_if_schema_gets_converted():
    struct_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("marks", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("grade", StringType(), True),
        ]
    )

    result = convert_schema_to_list(struct_schema)

    # Make sure the function returns a dataframepython list
    assert isinstance(result, list)

    #Make sure the columns and order are correct
    assert result == [
        "id",
        "name",
        "marks",
        "address",
        "grade",
    ]

    print({"list_schema": result})
