from datetime import timedelta
from datetime import datetime
import pandas as pd
from random import randint
from torch import load as tload

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
s3_hook = S3Hook(cid)


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
    info["start_timestamp"] = datetime.now().strftime("%Y%m%d %H:%M")
    # Импользуем данные с сегодня на 2 года назад.
    info["date_start"] = datetime.now().strftime("%Y-%m-%d")
    info["date_end"] = datetime.now().strftime("%Y-%m-%d")
    return info


def scrape_news(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="init")
    info["data_download_start"] = datetime.now().strftime("%Y%m%d %H:%M")
    start_date = info["date_start"]
    end_date = info["date_end"]

    scr = Scrapper("https://www.finam.ru/publications/section/market/date/", start_date, end_date)
    scr.parse()
    news_df = scr.estimate()

    s3_hook.download_file(
        key = "finam_news_scored.csv"
        bucket_name = "studcamp-ml"
    )
    s3_news_df = pd.read_csv("finam_news_scored.csv")
    news_df = pd.concat([s3_news_df,news_df], ignore_index=True)
    s3_hook.load_file(news_df, "finam_news_scored.csv", 
                          bucket_name="studcamp-ml", replace=True)
    
    _LOG.info("News loading finished.")

    info["data_download_end"] = datetime.now().strftime("%Y%m%d %H:%M")

    return info

def query_forming(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="scrape_news")
    info["query_forming_start"] = datetime.now().strftime("%Y%m%d %H:%M")

    max_news_count = 20
    lookback = 65

    s3_hook.download_file(
        key = "finam_news_scored.csv"
        bucket_name = "studcamp-ml"
    )
    news = pd.read_csv("finam_news_scored.csv")

    s3_hook.download_file(
        key = "IMOEX_filled.csv"
        bucket_name = "studcamp-ml"
    )
    df_filled = pd.read_csv("IMOEX_filled.csv")

    news["day"] = pd.to_datetime(news["day"])
    cur_date = news.iloc[-1].day

    query = pd.DataFrame()
    news_count = news[news.day == cur_date].shape[0]
    query["news_size"] = [news_count]
    query["year"] = [cur_date.year]
    query["month"] = [cur_date.month]
    query["day"] = [cur_date.day]

    res = news[news.day == cur_date].sample(min(max_news_count, news_count)).score

    for i in range(max_news_count - min(max_news_count, news_count)):
        res = res.append(pd.Series([0.0]), ignore_index=True)

    res = res.reset_index(drop=True)

    for i in range(max_news_count):
        query[f"news{i}"] = res[i]

    lags = df_filled[df_filled.date.between(str(cur_date-timedelta(days=lookback)),str(cur_date))].open.reset_index(drop=True)
    
    for i in range(1,lookback+1):
        query["shift"+str(i)] = lags[lags.size-i]
    
    info["query"] = query
    _LOG.info("Query forming finished.")
    info["query_forming_end"] = datetime.now().strftime("%Y%m%d %H:%M")

    return info

def model_predict(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="query_forming")
    info["prediction_start"] = datetime.now().strftime("%Y%m%d %H:%M")

    s3_hook.download_file(
        key = "catboost.pth"
        bucket_name = "studcamp-ml"
    )
    model = tload("catboost.pth")
    query = info["query"]
    preds = model.predict(query)

    s3_hook.load_file(preds, key = "prediction.csv", 
                        bucket_name="studcamp-ml", replace=True)
    
    info["predict"] = preds
    _LOG.info("Prediction finished.")
    info["prediction_end"] = datetime.now().strftime("%Y%m%d %H:%M")
    return info
    
def save_last_hundred(**kwargs):
    ti = kwargs["ti"]
    info = ti.xcom_pull(task_ids="prediction")
    info["save_last_start"] = datetime.now().strftime("%Y%m%d %H:%M")   

    s3_hook.download_file(
        key = "IMOEX_filled.csv"
        bucket_name = "studcamp-ml"
    )
    df_filled = pd.read_csv("IMOEX_filled.csv").tail(100)
    preds = info["predict"]
    last_hundred = pd.concat([df_filled, preds], ignore_index=True)

    s3_hook.load_file(last_hundred, key = "last_hundred.csv", 
                        bucket_name="studcamp-ml", replace=True)

    _LOG.info("Save finished.")
    info["save_last_end"] = datetime.now().strftime("%Y%m%d %H:%M")
    return info


    



