# tests/test_with_chispa.py
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.transformations import filter_engineers

def test_filter_engineers_with_chispa(spark, sample_data, sample_schema):
    # Arrange
    input_df = spark.createDataFrame(sample_data, sample_schema)
    
    expected_data = [
        ("Alice", 25, "Engineer"),
        ("Diana", 28, "Engineer")
    ]
    expected_df = spark.createDataFrame(expected_data, sample_schema)
    
    # Act
    result_df = filter_engineers(input_df)
    
    # Assert
    assert_df_equality(result_df, expected_df)
