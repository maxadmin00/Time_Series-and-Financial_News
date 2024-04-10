from selenium import webdriver
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.common.by import By
from torch import load as tload

class Scrapper:
    def __init__(self, url, start_date, end_date):
        self.main_df = pd.DataFrame()
        self.driver = webdriver.Chrome()
        self.data = []
        self.url = url
        self.cur_day = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_day = datetime.strptime(end_date, '%Y-%m-%d')
        self.k = 0
        
    def get_next_day(self):
        self.cur_day = self.cur_day + timedelta(days = 1)

    def cache(self):
        df = pd.DataFrame(self.data, columns = ['day', 'news'])
        self.data = []
        df.reset_index(drop=True, inplace=True)
        self.main_df.reset_index(drop=True, inplace=True)
        self.main_df = pd.concat([self.main_df, df], axis = 0)
        #print(df_cache.tail())
        print(self.cur_day, self.main_df.shape[0])
        
    def parse(self):
        self.k = 0
        
        while self.cur_day <= self.end_day:
            url_to_parse = self.url + self.cur_day.strftime('%Y-%m-%d') + '/'

            self.driver.get(url_to_parse)
            while True:
                button = self.driver.find_element(By.XPATH, '//*[@id="finfin-local-plugin-block-item-publication-list-filter-date-showMore-container"]/span')
                try:
                    button.click()
                except:
                    break

            bs = BeautifulSoup(self.driver.page_source)
            news = bs.find_all('a', {'class' : 'display-b pt2x pb05x publication-list-item'})
            for piece in news:
                to_insert = []
                to_insert.append(self.cur_day)
                to_insert.append(piece.find('div', {'class' : 'font-l bold'}).text.strip())   
                '''try:
                    to_insert.append(piece.find('p', {'class' : 'cl-black'}).text.strip())
                except:
                    to_insert.append(nan)'''
                self.data.append(to_insert)
                self.k += 1

            if self.k >= 100:
                self.k = 0
                self.cache()

            self.get_next_day() 

        self.cache()
        self.driver.close()

    
    def estimate(self):
        pipeline = tload('model.pth')
        self.main_df['score'] = pipeline.predict(self.main_df.news)
        #self.main_df['score'] = self.main_df.news.apply(lambda x: pipeline.predict(x))
        
    