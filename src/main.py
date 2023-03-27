from pyspark.sql.types import StructType, StringType, DateType
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
    concat
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


def add_required_columns(df: DataFrame, current_user: str):
    """Adds required columns to input DataFrame"""
    target_df = (
        df.withColumn("file_name", lit(input_file_name()))
        .withColumn("create_user", lit(current_user))
        .withColumn("create_date", lit(current_date()))
        .withColumn("modified_user", lit(None).cast(StringType()))
        .withColumn("modified_date", lit(None).cast(DateType()))
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


def convert_date_object(
    df: DataFrame, column_name: str, current_date_format: str
) -> DataFrame:
    target_df = df.withColumn(
        column_name,
        when(
            to_date(df[column_name], current_date_format).isNotNull(),
            to_date(df[column_name], current_date_format),
        ).otherwise(to_date(df[column_name])),
    )

    return target_df

def read_from_raw_layer(spark:SparkSession, table_name: str) -> DataFrame:
    df = spark.read.table(f'conference_raw.{table_name}')
    df = df.filter(df.is_processed == 'false')
    df.show()
    return df

def read_data_from_raw(spark:SparkSession, schema: str, table_name: str) -> DataFrame:
    df = spark.read.table(f"{schema}.{table_name}")
    df = df.filter(df.is_processed == "false")
    df.printSchema()
    return df

def add_attendee_type(df: DataFrame, attendee_type: str) -> DataFrame:
    result_df = df.withColumn("attendee_type", lit(attendee_type))
    return result_df


def create_timestamp_str(df: DataFrame, date_column_name: str, column_name: str, new_column_name: str) -> DataFrame:
    result_df = df.withColumn(
        new_column_name,
        concat(
            df[date_column_name].cast(StringType()),
            lit(' '),
            df[column_name]
        )
    )

    result_df.printSchema()

    return result_df

def select_columns_from_dataframe(df: DataFrame, columns: list) -> DataFrame:
    result_df = df.select(*columns)
    
    result_df.printSchema()

    return result_df

def union_dataframes(df1: DataFrame, df2: DataFrame) -> DataFrame:
    result_df = df1.union(df2)

    result_df.printSchema()
    return result_df

def add_columns_to_refined_tables(df: DataFrame) -> DataFrame:
    """Adds required columns to input DataFrame"""
    target_df = (
        df.withColumn("create_user", lit(current_user))
        .withColumn("create_date", lit(current_date()))
        .withColumn("modified_user", lit(None).cast(StringType()))
        .withColumn("modified_date", lit(None).cast(DateType()))
        .withColumn("is_processed", lit(False))
    )
    target_df.printSchema()
    return target_df

def add_columns_to_refined_tables(df: DataFrame) -> DataFrame:
    """Adds required columns to input DataFrame"""
    target_df = (
        df.withColumn("create_user", lit(current_user))
        .withColumn("create_date", lit(current_date()))
        .withColumn("modified_user", lit(None).cast(StringType()))
        .withColumn("modified_date", lit(None).cast(DateType()))
        .withColumn("is_processed", lit(False))
    )
    target_df.printSchema()
    return target_df

def add_columns_to_refined_tables(df: DataFrame) -> DataFrame:
    """Adds required columns to input DataFrame"""
    target_df = (
        df.withColumn("create_user", lit(current_user))
        .withColumn("create_date", lit(current_date()))
        .withColumn("modified_user", lit(None).cast(StringType()))
        .withColumn("modified_date", lit(None).cast(DateType()))
        .withColumn("is_processed", lit(False))
    )
    target_df.printSchema()
    return target_df


