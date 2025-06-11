# src/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, avg

def categorize_employees(df: DataFrame) -> DataFrame:
    """Categorize employees by age and calculate average age by role"""
    return df.withColumn(
        "age_category",
        when(col("age") < 30, "Young")
        .when(col("age") < 40, "Middle")
        .otherwise("Senior")
    ).groupBy("role", "age_category") \
     .agg(avg("age").alias("avg_age"))

def filter_engineers(df: DataFrame) -> DataFrame:
    """Filter only engineers"""
    return df.filter(col("role") == "Engineer")
