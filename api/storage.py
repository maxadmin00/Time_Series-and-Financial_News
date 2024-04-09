import boto3
import io
import torch

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net'
)

def get_model():
    get_object_response = s3.get_object(Bucket='studcamp-ml', Key='model.pth')
    file = get_object_response['Body'].read()
    buffer = io.BytesIO(file)
    model = torch.load(buffer)
    return model
