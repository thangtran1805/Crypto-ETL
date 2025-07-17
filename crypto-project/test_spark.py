from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquetExample") \
    .getOrCreate()

file_path = "/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/processed/dim/dim_time/dim_time_2025_07_16.parquet"

df = spark.read.parquet(file_path)
df.show()