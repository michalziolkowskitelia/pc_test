""" This module contains common functions used in this task"""
from pyspark.sql.functions import coalesce, to_date, regexp_replace, trim, when, col
from pyspark.sql import Column


def standarize_date_col(
    col_: Column, formats: list = ("MM/dd/yyyy", "yyyy-MM-dd", "MMMM d, yyyy")
) -> Column:
    """
    This function casts
    :param col_: pySpark column to cleanse
    :return: new pySpark column with replaced values
    """
    return coalesce(*[to_date(col_, f) for f in formats])


def cleanse_text_col(col_: Column) -> Column:
    """
    This function replaces all consecutive white spaces,
    carriage returns, tabs and newline characters with spaces.
    :param col_: pySpark column to cleanse
    :return: new pySpark column with replaced values
    """
    return trim(regexp_replace(col_, r"\s+", " "))


def validate_col_value(col_name: str) -> Column:
    """
    This function checks if the provided column
    contains null values.
    :param col_: pySpark column to validate
    :return: new pySpark column with replaced values
    """

    return when(col(col_name).isNull(), "invalid").otherwise("valid")
