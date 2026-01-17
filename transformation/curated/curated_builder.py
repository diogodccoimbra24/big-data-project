
from pyspark.sql import SparkSession
from pathlib import Path


spark = SparkSession \
    .builder \
    .appName("big-data-project") \
    .master("local[2]") \
    .getOrCreate()


PROJECT_ROOT = Path(__file__).resolve().parents[2]
crypto_path = PROJECT_ROOT / "data" / "formatted" / "crypto" / "crypto_daily.parquet"

df_crypto = spark.read.parquet(str(crypto_path))
df_crypto.printSchema()
df_crypto.show(5)