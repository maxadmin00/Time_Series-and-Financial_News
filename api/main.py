from fastapi import FastAPI

import storage

app = FastAPI()


@app.get("/predict")
def predict():
    model = storage.get_model()
    today_info = ''
    pred = model.predict(today_info)
    return pred