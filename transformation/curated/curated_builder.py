
from pyspark.sql import SparkSession
from pathlib import Path
import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

#Creating a spark session
spark = (
    SparkSession.builder
    .appName("big-data-project")
    .master("local[2]")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
    )
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
    .getOrCreate()
    )


#To access crypto parquet
PROJECT_ROOT = Path(__file__).resolve().parents[2]
crypto_path = PROJECT_ROOT / "data" / "formatted" / "crypto" / "crypto_daily.parquet"

#To access news parquet
news_path = PROJECT_ROOT / "data" / "formatted" / "news" / "news_daily.parquet"

#Reads the parquets
df_crypto = spark.read.parquet(str(crypto_path))
df_news = spark.read.parquet(str(news_path))


#Left joining the two data frames with spark
df_leftjoin_spark = df_crypto.join(
    df_news,
    (df_crypto.date == df_news.date),
    "left"
)

#Dropping date column duplicate
df_leftjoin_spark = df_leftjoin_spark.drop(df_news["date"])


#Saving in parquet format
#To save in the designated directory
output_path_curated = PROJECT_ROOT / "data" / "curated" / "crypto_news_kpi" / "crypto_news_parquet"
output_path_curated.parent.mkdir(parents=True, exist_ok=True)

#Writting the parquet
df_leftjoin_spark.write.mode("overwrite").parquet(str(output_path_curated))



