from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from datetime import datetime
from pytz import timezone
from pyspark.sql.functions import (
    lit,
    current_timestamp,
    input_file_name,
    from_utc_timestamp,
    to_date,
    when,
    DataFrame,
    current_date,
)


def read_file_from_src_path(
    spark: SparkSession, src_path: str, filename: str, schema: StructType
):
    """Reads input csv files from source file path (s3) to databricks"""
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("escapeQuotes", "true")
        .option("multiline", "true")
        .schema(schema)
        .load(f"s3a://{src_path}/{filename}")
    )
    df.printSchema()
    return df


def add_required_columns(df: DataFrame):
    """Adds required columns to input DataFrame"""
    current_user = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .userName()
        .get()
    )
    target_df = (
        df.withColumn("file_name", lit(input_file_name()))
        .withColumn("create_user", lit(current_user))
        .withColumn("create_date", lit(current_date()))
        .withColumn("modified_user", lit(None).cast(StringType()))
        .withColumn("modified_date", lit(None).cast(TimestampType()))
        .withColumn("is_processed", lit(False))
    )
    target_df.printSchema()
    return target_df


def file_exists(spark: SparkSession, src_file_path: str, filename: str) -> dict:
    return_object = {"filename": filename, "file_exists": False, "schema": None}
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("escapeQuotes", "true")
            .option("multiline", "true")
            .option("inferSchema", "true")
            .load(f"s3a://{src_file_path}/{filename}")
        )
        inferred_schema_obj = df.schema
        field_names = [i.name for i in inferred_schema_obj]
        return_object["file_exists"] = True
        return_object["schema"] = field_names
        return return_object
    except Exception as e:
        print(e)
        return return_object


def compare_schema(inferred_schema: list, defined_schema: list, filename: str) -> dict:
    valid_schema = False
    missing_columns = None
    return_object = {
        "valid_schema": valid_schema,
        "missing_columns": missing_columns,
        "filename": filename,
    }
    try:
        if len(inferred_schema) == len(defined_schema):
            result = all(i == j for i, j in zip(inferred_schema, defined_schema))
            if result:
                valid_schema = True
            else:
                valid_schema = False
        elif len(inferred_schema) > len(defined_schema):
            # extra columns
            count = 0
            for i, v in enumerate(defined_schema):
                if inferred_schema[i] == v:
                    count += 1
            valid_schema = True if count == len(defined_schema) else False
        else:
            # missing columns
            valid_schema = False
            set_inferred_schema = set(inferred_schema)
            set_defined_schema = set(defined_schema)
            missing_columns = list(set_defined_schema.difference(set_inferred_schema))

        return_object["valid_schema"] = valid_schema
        return_object["missing_columns"] = missing_columns

        return return_object
    except Exception as e:
        print(e)
        return return_object


def convert_schema_to_list(schema: StructType) -> list:
    result = [i.name for i in schema]
    return result