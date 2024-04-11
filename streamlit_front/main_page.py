import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import requests

'''moscow_path = os.path.join('data','IMOEX_filled.csv')
spb_path = os.path.join('data','SPBIRUS2_df_clearn.csv')
news_path = os.path.join('data','every_piece_of_news.csv')'''

st.title('Предсказание индексов российских бирж')

req_moscow = requests.get(http://84.252.139.210:8000/api/v1/predict)

df_spb = pd.read_csv(spb_path).tail(30)
df_moscow = pd.read_csv(moscow_path).tail(30)
df_spb.rename(columns={'DATE':'date', 'OPEN' : 'open'}, inplace=True)

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_moscow.date,
    y=df_moscow.open,
    name='IMOEX',
    visible=True))

fig.add_trace(go.Scatter(
    x= [df_moscow.date.tail(1).values[0], datetime.strptime(df_moscow.date.tail(1).values[0], '%Y-%m-%d') + timedelta(days=1)],
    y= [df_moscow.open.tail(1).values[0], np.random.randint(3000, 4000)],
    name='IMOEX_prediction',
    mode='lines+markers',
    visible=True))
    
fig.add_trace(go.Scatter(
    x=df_spb.date,
    y=df_spb.open,
    name='SPBIRUS',
    visible=False))

fig.add_trace(go.Scatter(
    x= [df_spb.date.tail(1).values[0], datetime.strptime(df_spb.date.tail(1).values[0], '%Y-%m-%d') + timedelta(days=1)],
    y= [df_spb.open.tail(1).values[0], np.random.randint(100, 200)],
    name='IMOEX_prediction',
    mode='lines+markers',
    visible=False))

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
    title_text = 'Значение индекса за последние 30 дней',
    title_x = 0.5,
    autosize=True,
    height=400,
    xaxis_title="Дата",
    yaxis_title="Значение",
)


st.plotly_chart(fig, use_container_width=True)

df_news = pd.read_csv(news_path)
news_today = df_news.loc[df_news.day == df_news.tail(1).day.values[0]]
display_news = '- ' + '\n- '.join(list(news_today.news.values))

st.subheader('Новости за сегодня')
st.markdown(display_news)



