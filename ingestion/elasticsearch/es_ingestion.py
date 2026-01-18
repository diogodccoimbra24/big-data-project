
import requests
import json
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
crypto_news_path = PROJECT_ROOT / "data" / "curated" / "crypto_news_kpi" / "crypto_news_parquet"


#Reads the parquets
df_crypto_news = spark.read.parquet(str(crypto_news_path))

url = "http://localhost:9200"
index_name = "crypto-news-daily"

#Converting the dataframe into a list of dictionaries
rows = df_crypto_news.collect()

list_of_dicts = []

for r in rows:
    d = r.asDict()
    #Convert date to string for Elasticsearch
    if d.get("date") is not None:
        d["date"] = d["date"].isoformat()
    list_of_dicts.append(d)


#Building bulk payload
bulk_lines = []

for doc in list_of_dicts:
    doc_id = doc["date"]

    action = {
        "index" : {
            "_index" : index_name,
            "_id" : doc_id
        }
    }

    bulk_lines.append(json.dumps(action))
    bulk_lines.append(json.dumps(doc))

#Join with new line because it's required by bulk
bulk_payload = "\n".join(bulk_lines) + "\n"

headers = {"Content-Type": "application/x-ndjson"}

#Send it to es with one request
response = requests.post(
    f"{url}/_bulk",
    headers=headers,
    data=bulk_payload
)
