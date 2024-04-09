import streamlit as st
import datetime as dt
import pandas as pd
from dateutil.relativedelta import relativedelta # to add days or years
import time
import numpy as np
import plotly.graph_objects as go

col1, col2 = st.columns((1, 1))
day1 = col1.date_input('Начало периода', min_value = dt.date(year=2015, month=11, day=25) + relativedelta(day = 100), max_value= dt.date.today(), key = '1')
day2 = col2.date_input('Начало периода', min_value = day1 + relativedelta(day=1), max_value= dt.date.today(), key = '2')
option = st.selectbox(
     'Выберите индекс',
     ('IMOEX', 'SPBIRUS'))

if 'clicked' not in st.session_state:
    st.session_state.clicked = False

def click_button():
    st.session_state.clicked = True

st.button('Построить модель', on_click=click_button)

if st.session_state.clicked:
    bar_p = st.progress(0)

    with st.spinner('Please wait...'):
        time.sleep(2)

    if option == 'IMOEX':
        df = pd.read_csv('ml_data\data\IMOEX_2009-01-01_2024-04-08.csv')
    else:
        df = pd.read_csv('ml_data\data\SPBIRUS2_df_clearn.csv')

    mask = (pd.to_datetime(df.DATE, format = '%Y-%m-%d') >= pd.to_datetime(day1)) & (pd.to_datetime(df.DATE, format = '%Y-%m-%d') <= pd.to_datetime(day2))
    df = df.loc[mask]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df.DATE,
        y=df.OPEN,
        name=option,
        visible=True))

    fig.add_trace(go.Scatter(
        x= [df.DATE.tail(1).values[0], dt.datetime.strptime(df.DATE.tail(1).values[0], '%Y-%m-%d') + dt.timedelta(days=1)],
        y= [df.OPEN.tail(1).values[0], np.random.randint(3000, 4000)],
        name=f'{option}_prediction',
        mode='lines+markers',
        visible=True))
    
    st.plotly_chart(fig, use_container_width=True)
    st.session_state.clicked = False