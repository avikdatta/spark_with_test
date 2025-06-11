# conftest.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[1]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def sample_data():
    """Sample test data"""
    return [
        ("Alice", 25, "Engineer"),
        ("Bob", 30, "Manager"),
        ("Charlie", 35, "Analyst"),
        ("Diana", 28, "Engineer")
    ]

@pytest.fixture(scope="session")
def sample_schema():
    """Sample schema for test data"""
    return StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("role", StringType(), True)
    ])
