from fastapi import FastAPI
import pandas as pd

import storage

app = FastAPI()


@app.get("/predict")
def predict():
    model = storage.get_model()
    d = {'news': ['Будем дружить?']}
    df = pd.DataFrame(data=d)
    print(df.news)
    pred = model.predict(df.news)
    print(pred)