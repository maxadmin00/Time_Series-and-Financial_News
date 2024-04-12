import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import requests
import json

news_path = os.path.join('data','every_piece_of_news.csv')

st.title('Предсказание индексов российских бирж')

req_moscow = requests.get('http://84.252.139.210:8000/api/v1/predict?index_name=imoex')
json_response = json.loads(req_moscow.content)
df_moscow = pd.DataFrame(json_response)
pred_moscow = df_moscow.tail(1).values[0]
df_moscow = df_moscow.iloc[:-1]

req_spb = requests.get('http://84.252.139.210:8000/api/v1/predict?index_name=spbirus2')
json_response = json.loads(req_spb.content)
df_spb = pd.DataFrame(json_response)
pred_spb = df_spb.tail(1).values[0]
df_spb = df_spb.iloc[:-1]

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_moscow.date,
    y=df_moscow.open,
    name='IMOEX',
    line=dict(color = '#636efa'),
    visible=True))

fig.add_trace(go.Scatter(
    x= [df_moscow.date.tail(1).values[0], pred_moscow[0]],
    y= [df_moscow.open.tail(1).values[0], pred_moscow[1]],
    name='IMOEX_prediction',
    mode='lines+markers',
    line=dict(color = '#ef553b'),
    visible=True))
    
fig.add_trace(go.Scatter(
    x=df_spb.date,
    y=df_spb.open,
    name='SPBIRUS',
    line=dict(color = '#636efa'),
    visible=False))

fig.add_trace(go.Scatter(
    x= [df_spb.date.tail(1).values[0], pred_spb[0]],
    y= [df_spb.open.tail(1).values[0], pred_spb[1]],
    name='IMOEX_prediction',
    mode='lines+markers',
    line=dict(color = '#ef553b'),
    visible=False))

if pred_moscow[1] >= df_moscow.open.tail(1).values[0]:
    st.balloons()
else:
    st.snow()    

fig.update_layout(
    updatemenus=[
        dict(
            active=0,
            buttons=list([
                dict(label="IMOEX",
                     method="update",
                     args=[{"visible": [True, True, False, False]}]),
                dict(label="SPBIRUS",
                     method="update",
                     args=[{"visible": [False, False, True, True]}])
            ]),

            showactive=True,
            x=0.0,
            xanchor="left",
            y=1.2,
            yanchor="top"),]
        )

fig.update_layout(
    title_text = 'Значение индекса за последние 100 дней',
    title_x = 0.5,
    autosize=True,
    height=400,
    xaxis_title="Дата",
    yaxis_title="Значение",
)


st.plotly_chart(fig, use_container_width=True)
today = datetime.strftime(datetime.now() - timedelta(10), format = '%Y-%m-%d')
req_news = requests.get(f'http://84.252.139.210:8000/api/v1/news?day={today}')
json_response = json.loads(req_news.content)
df_news = pd.DataFrame(json_response)
news = df_news.news.values
display_news = '- ' + '\n- '.join(list(news))

st.subheader('Новости за сегодня')
st.markdown(display_news)



