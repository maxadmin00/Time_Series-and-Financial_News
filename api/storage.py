import boto3
import io
import torch
import pandas as pd

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)

def get_model():
    response = s3.get_object(Bucket='studcamp-ml', Key='catboost.pth')
    file = response['Body'].read()
    buffer = io.BytesIO(file)
    model = torch.load(buffer)
    return model

def get_imoex():
    response = s3.get_object(Bucket='studcamp-ml', Key='IMOEX_filled.csv')
    csv_string = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(csv_string))
    return df

def get_news():
    response = s3.get_object(Bucket='studcamp-ml', Key='finam_news_scored.csv')
    csv_string = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(csv_string))
    return df
