""" This module contains test cases defined for this task """
import unittest
import datetime

from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from common import standarize_date_col, cleanse_text_col


class PySparkTestCase(unittest.TestCase):
    """
    Parent pySpark testing class.
    Handles SparkSession creation & deletion for child test cases.
    """

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("Testing suite")
            .master("local[2]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestCommonFunctions(PySparkTestCase):
    """
    A Class for functions defined in common module
    """

    def test_cleanse_text_col(self):
        """
        Tests common.cleanse_text_col function
        """
        sample_data = [
            {"bio": "Consequuntur ratione modi ut placeat.\n\t"},
            {"bio": "Laborum pariatur neque rerum molestiae rerum illo.           "},
            {"bio": "Dicta est minus rerum.   \t"},
            {"bio": "\n Nihil ut perferendis vero neque quaerat culpa.\t"},
        ]

        original_df = self.spark.createDataFrame(sample_data)

        transformed_df = original_df.withColumn("bio_cleansed", cleanse_text_col("bio"))

        expected_data = [
            {
                "bio": "Consequuntur ratione modi ut placeat.\n\t",
                "bio_cleansed": "Consequuntur ratione modi ut placeat.",
            },
            {
                "bio": "Laborum pariatur neque rerum molestiae rerum illo.           ",
                "bio_cleansed": "Laborum pariatur neque rerum molestiae rerum illo.",
            },
            {
                "bio": "Dicta est minus rerum.   \t",
                "bio_cleansed": "Dicta est minus rerum.",
            },
            {
                "bio": "\n Nihil ut perferendis vero neque quaerat culpa.\t",
                "bio_cleansed": "Nihil ut perferendis vero neque quaerat culpa.",
            },
        ]

        expected_df = self.spark.createDataFrame(expected_data)
        assertDataFrameEqual(transformed_df, expected_df)

    def test_standarize_date_col(self):
        """
        Tests common.standarize_date_col function
        """
        sample_data = [
            {"start_date": "02/05/2001"},
            {"start_date": "December 25, 2013"},
            {"start_date": "1998-04-08"},
            {"start_date": "Et quia sequi quos."},
        ]

        original_df = self.spark.createDataFrame(sample_data)

        transformed_df = original_df.withColumn(
            "start_date_standarized", standarize_date_col("start_date")
        )

        expected_data = [
            {
                "start_date": "02/05/2001",
                "start_date_standarized": datetime.date(2001, 2, 5),
            },
            {
                "start_date": "December 25, 2013",
                "start_date_standarized": datetime.date(2013, 12, 25),
            },
            {
                "start_date": "1998-04-08",
                "start_date_standarized": datetime.date(1998, 4, 8),
            },
            {"start_date": "Et quia sequi quos.", "start_date_standarized": None},
        ]
        expected_df = self.spark.createDataFrame(expected_data)
        assertDataFrameEqual(transformed_df, expected_df)
