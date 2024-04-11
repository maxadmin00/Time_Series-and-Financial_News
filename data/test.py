import pandas as pd
df = pd.read_csv(r'C:\Users\denis\Time_Series-and-Financial_News\data\marked_stock_news_df.csv')
print(df.loc[df.day == '2024-01-01'].news.head())