# tests/test_transformations.py
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from src.transformations import categorize_employees, filter_engineers

class TestTransformations:
    
    def test_categorize_employees(self, spark, sample_data, sample_schema):
        # Arrange
        input_df = spark.createDataFrame(sample_data, sample_schema)
        
        # Act
        result_df = categorize_employees(input_df)
        
        # Assert
        result_count = result_df.count()
        assert result_count > 0
        
        # Check schema
        expected_columns = {"role", "age_category", "avg_age"}
        actual_columns = set(result_df.columns)
        assert expected_columns == actual_columns
        
        # Check specific values
        engineer_young = result_df.filter(
            (col("role") == "Engineer") & 
            (col("age_category") == "Young")
        ).collect()
        
        assert len(engineer_young) == 1
        assert engineer_young[0]["avg_age"] == ( 25.0 + 28.0 ) / 2

    def test_filter_engineers(self, spark, sample_data, sample_schema):
        # Arrange
        input_df = spark.createDataFrame(sample_data, sample_schema)
        
        # Act
        result_df = filter_engineers(input_df)
        
        # Assert
        assert result_df.count() == 2  # Alice and Diana
        roles = [row["role"] for row in result_df.collect()]
        assert all(role == "Engineer" for role in roles)
