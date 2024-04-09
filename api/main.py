from fastapi import FastAPI
import pandas as pd

import storage
import utils

app = FastAPI()


@app.get("/predict")
def predict():
    model = storage.get_model()
    imoex = storage.get_imoex()
    news = storage.get_news()
    query = utils.query_for_tomorrow(imoex, news)
    pred = model.predict(query)
    print(pred)