from datetime import timedelta
from random import randint

import backoff
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
from news_parser import Scrapper
from airflow.utils.dates import days_ago
from botocore.exceptions import (
    ConnectTimeoutError,
    EndpointConnectionError,
    ConnectionError,
)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

cid = "s3_connection"



DEFAULT_ARGS = {
    "owner": "Team 22",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "main_dag",
    tags=["mlops"],
    catchup=False,
    start_date=days_ago(2),
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
)

def init() -> Dict[str, Any]:
    """
    Step0: Pipeline initialisation.
    """
    info = {}
    info["start_tiemstamp"] = datetime.now().strftime("%Y%m%d %H:%M")
    info["dataset_end"] = datetime.now().strftime("%Y-%m-%d")
    # Импользуем данные с сегодня на 2 года назад.
    info["dataset_start"] = (datetime.now() -
                           timedelta(100)).strftime("%Y-%m-%d")
    return info


def scrape_news(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="init")
    info["data_download_start"] = datetime.now().strftime("%Y%m%d %H:%M")

    scr = Scrapper('https://www.finam.ru/publications/section/market/date/', start_date, end_date)
    scr.parse()
    news_df = scr.estimate()

    s3_hook = S3Hook("s3_connection")
    s3_hook.download_file(
        key = 'finam_news_scored.csv'
        bucket_name = 'studcamp-ml'
    )
    s3_news_df = pd.read_csv('finam_news_scored.csv')
    news_df = pd.concat([s3_news_df,news_df], ignore_index=True)
    s3_hook.load_file(news_df, 'finam_news_scored.csv', 
                          bucket_name='studcamp-ml', replace=True)
    
    _LOG.info("News loading finished.")

    info["data_download_end"] = datetime.now().strftime("%Y%m%d %H:%M")

    return info



