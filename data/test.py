import pandas as pd
df = pd.read_csv('data\\news_stock.csv')
import joblib
'''model = joblib.load('data\model (1).joblib')
df['score'] = model.predict(df.header)
print(df.head())
df.to_csv('data\\news_stock.csv', index= False)'''
'''df.drop(columns=['description'], inplace = True)
df.to_csv('data\\news_stock.csv', index= False)'''
df_corrected = pd.read_csv('data\\finam_news_scored.csv')
df_corrected.drop(columns=['difference', 'color'], inplace=True)
#df_corrected.drop(columns = ['open'], inplace=True)
#df_corrected.reset_index(drop=True, inplace=True)
#df_corrected.drop(columns = ['Unnamed: 0'], inplace=True)
df_corrected.to_csv('data\\finam_news_scored.csv', index = False)