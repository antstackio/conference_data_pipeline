from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
import pytest

from src.main import read_from_raw_layer

spark = SparkSession.builder \
                    .appName('unit-tests') \
                    .getOrCreate()

@pytest.fixture
def ref_table():
    spark.sql("create database if not exists conference_raw")

    data = [
        [1,"a",20, "aa", "false"],
        [2,"b",21, "bb", "true"],
        [3,"c",22, "cc", "true"],
        [4,"d",23, "dd", "true"],
        [5,"e",24, "ee", "false"],
    ]

    df = spark.createDataFrame(data, ['id','name','age', "address", "is_processed"])

    db = 'conference_raw'
    tablename = 'test_read_data_from_raw'
    df.write.mode("overwrite").saveAsTable(f"{db}.{tablename}")

    return tablename


def test_with_existent_table(ref_table):
    df = read_from_raw_layer(spark, ref_table)

    # Make sure the function returns a dataframe
    assert isinstance(df, DataFrame)

    #Make sure only unprocessed records are returned
    assert df.filter(df.is_processed != "false").count() == 0

    assert df.select('is_processed').distinct().collect()[0]['is_processed'] == "false"

    assert df.count() == 2
    
    df.show()


def test_with_non_existing_table(ref_table):
    spark.sql(f"truncate table conference_raw.{ref_table}")
    spark.sql(f"drop table conference_raw.{ref_table}")

    with pytest.raises(Exception) as e_info:
        df = read_from_raw_layer(spark, ref_table)

    print(e_info)
