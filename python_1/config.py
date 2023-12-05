""" This module contains schemas and configs used in this task """
from pyspark.sql.types import StringType, StructField, StructType

STATES_SCHEMA = StructType(
    [
        StructField("state_abbr", StringType(), True),
        StructField("state_name", StringType(), True),
    ]
)

CENSUS_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("email", StringType(), True),
        StructField("bio", StringType(), True),
        StructField("job", StringType(), True),
        StructField("start_date", StringType(), True),
    ]
)
