import pandas as pd
from datetime import timedelta


def query_for_tomorrow(df_filled: pd.DataFrame, news: pd.DataFrame, max_news_count = 20, lookback = 65):
    news['day'] = pd.to_datetime(news['day'])
    cur_date = news.iloc[-1].day

    query = pd.DataFrame()
    news_count = news[news.day == cur_date].shape[0]
    query['news_size'] = [news_count]
    query['year'] = [cur_date.year]
    query['month'] = [cur_date.month]
    query['day'] = [cur_date.day]

    res = news[news.day == cur_date].sample(min(max_news_count, news_count)).score

    for i in range(max_news_count - min(max_news_count, news_count)):
        res = res.append(pd.Series([0.0]), ignore_index=True)

    res = res.reset_index(drop=True)

    for i in range(max_news_count):
        query[f'news{i}'] = res[i]

    lags = df_filled[df_filled.date.between(str(cur_date-timedelta(days=lookback)),str(cur_date))].open.reset_index(drop=True)
    
    for i in range(1,lookback+1):
        query['shift'+str(i)] = lags[lags.size-i]
    
    return query
