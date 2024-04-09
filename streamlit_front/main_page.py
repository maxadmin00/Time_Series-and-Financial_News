import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.title('Предсказание индексов российских бирж')

df_moscow = pd.read_csv('ml_data\data\IMOEX_2009-01-01_2024-04-08.csv').tail(30)

df_spb = pd.read_csv('ml_data\data\SPBIRUS2_df_clearn.csv').tail(30)

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_moscow.DATE,
    y=df_moscow.OPEN,
    name='IMOEX',
    visible=True))

fig.add_trace(go.Scatter(
    x= [df_moscow.DATE.tail(1).values[0], datetime.strptime(df_moscow.DATE.tail(1).values[0], '%Y-%m-%d') + timedelta(days=1)],
    y= [df_moscow.OPEN.tail(1).values[0], np.random.randint(3000, 4000)],
    name='IMOEX_prediction',
    mode='lines+markers',
    visible=True))
    
fig.add_trace(go.Scatter(
    x=df_spb.DATE,
    y=df_spb.OPEN,
    name='SPBIRUS',
    visible=False))

fig.add_trace(go.Scatter(
    x= [df_spb.DATE.tail(1).values[0], datetime.strptime(df_spb.DATE.tail(1).values[0], '%Y-%m-%d') + timedelta(days=1)],
    y= [df_spb.OPEN.tail(1).values[0], np.random.randint(100, 200)],
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

df_news = pd.read_csv('ml_data\data\every_piece_of_news.csv')
news_today = df_news.loc[df_news.day == df_news.tail(1).day.values[0]]
display_news = '- ' + '\n- '.join(list(news_today.news.values))

st.subheader('Новости за сегодня')
st.markdown(display_news)



