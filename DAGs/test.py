from news_parser import Scrapper
scr = Scrapper('https://www.finam.ru/publications/section/market/date/', '2024-04-08', '2024-04-08')
scr.parse()
scr.estimate()
print(scr.main_df)