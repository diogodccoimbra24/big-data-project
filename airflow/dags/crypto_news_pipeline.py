

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="crypto_news_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["big-data", "etl"],
) as dag:

    ingest_crypto = EmptyOperator(task_id="ingest_crypto_coingecko")

    ingest_news = EmptyOperator(task_id="ingest_news_gdelt")

    format_crypto = EmptyOperator(task_id="format_crypto_parquet")

    format_news = EmptyOperator(task_id="format_news_parquet")

    curate_spark = EmptyOperator(task_id="spark_curate_join")

    index_es = EmptyOperator(task_id="index_elasticsearch")

    end = EmptyOperator(task_id="kibana_dashboard_ready")

    ingest_crypto >> format_crypto
    ingest_news >> format_news
    [format_crypto, format_news] >> curate_spark >> index_es >> end
