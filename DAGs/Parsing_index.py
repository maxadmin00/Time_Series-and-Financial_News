import requests
from datetime import datetime, timedelta
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO


class IMOEXDataDownloader:
    def __init__(self, start_date, end_date=datetime.now().strftime('%Y-%m-%d')):
        self.main_df = pd.DataFrame()
        self.data = []
        self.today_df = []
        self.start_date = start_date
        self.end_date = end_date
        self.born_date = datetime.strptime("2004-01-01", '%Y-%m-%d')


    def get_history_data(self):
            """
            Парсит и сохраняет архивные данные в указанном промежутке.
            """
            self.download_data()
            self.get_open_price()
            self._clean_data()


    def download_data(self):
        start_date_dt = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(self.end_date, '%Y-%m-%d')

        if start_date_dt < self.born_date:
            print("Дата начала должна быть не ранее 2004-01-01.")
            return
        
        days_difference = (end_date_dt - start_date_dt).days
        iterations = days_difference // 100 + (1 if days_difference % 100 != 0 else 0)

        for _ in range(iterations):
            temporary_end = min(start_date_dt + timedelta(days=100), end_date_dt)
            start_date_str = start_date_dt.strftime('%Y-%m-%d')
            temporary_end_str = temporary_end.strftime('%Y-%m-%d')

            url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/securities/IMOEX.csv?from={start_date_str}&till={temporary_end_str}"

            response = requests.get(url)
            if response.status_code == 200:
                content_string = response.content.decode('windows-1251')
                content_lines = content_string.splitlines()
                clean_content = '\n'.join(content_lines[1:-6])  
                self.data = pd.read_csv(StringIO(clean_content), sep=';')
                self.main_df = pd.concat([self.main_df, self.data], ignore_index=True)
            else:
                print(f"Ошибка при скачивании файла: {response.status_code}")

            start_date_dt = temporary_end + timedelta(days=1)
        

    def _clean_data(self):
        self.main_df = self.main_df[['TRADEDATE', 'OPEN']].rename(columns={"TRADEDATE": "DATE"})
        self.main_df = pd.concat([self.main_df, self.today_df], ignore_index=True)
        self.main_df['DATE'] = pd.to_datetime(self.main_df['DATE'], format='%Y-%m-%d')

        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        self.main_df = self.main_df[(self.main_df['DATE'] >= start_date_dt) & (self.main_df['DATE'] <= end_date_dt)]

        date_range = pd.date_range(start=start_date_dt, end=end_date_dt)
        date_df = pd.DataFrame(date_range, columns=['DATE'])

        self.main_df = pd.merge(date_df, self.main_df, on='DATE', how='left')
        self.main_df['OPEN'] = self.main_df['OPEN'].fillna(method='ffill')

        self.main_df = self.main_df.dropna().drop_duplicates(subset='DATE', keep='last')


    def get_open_price(self):
        """
        Забирает цену открытия за сегодня с мосБиржи.
        """
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        try:
            driver.get('https://ru.tradingview.com/symbols/MOEX-IMOEX/')
            time.sleep(1)
            price_elements = driver.find_elements(By.CSS_SELECTOR, '.apply-overflow-tooltip.value-GgmpMpKr')
            open = price_elements[2].text  
            open = str(open).replace("RUB", "")
            open = [float(open)]
            
        finally:
            driver.quit()
        
        today = [datetime.now().strftime('%Y-%m-%d')]
        self.today_df = pd.DataFrame({
            "DATE":today,
            "OPEN":open
        })


class SPBEDataDownloader:
    def __init__(self, start_date, end_date=datetime.now().strftime('%Y-%m-%d'), download_folder = os.path.join(os.getcwd(), "data"), prefix = "SPBIRUS"):
        self.start_date = start_date
        self.end_date = end_date
        self.download_folder = download_folder
        self.prefix = prefix
        self.main_df = pd.DataFrame()
        self.today_df = pd.DataFrame()
        self.url = "https://spbexchange.ru/stocks/indices/SPBIRUS/"
        self.response = requests.get(self.url)
        self._setup_chrome_driver()


    def _setup_chrome_driver(self):
        """
        Настраивает драйвер Chrome с указанными опциями.
        """
        chrome_options = Options()
        prefs = {"download.default_directory": self.download_folder}
        chrome_options.add_experimental_option("prefs", prefs)
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)


    def get_history_data(self):
        """
        Парсит и сохраняет архивные данные с 2015-11-25 по вчерашний день.
        """
        self._download_data()
        self.get_open_price()
        file_path = self._find_download_data()

        if file_path:
            print(f"Найден файл: {file_path}")
            self._clean_data(file_path)

        else:
            print("Файл не найден.")


    def _find_download_data(self):
        """
        Ищет скаченный файл с архивными данными индекса.
        """
        files_with_dates = [(file, os.path.getmtime(os.path.join(self.download_folder, file))) 
                    for file in os.listdir(self.download_folder) if file.startswith('SPBIRUS')]
        
        files_with_dates.sort(key=lambda x: x[1], reverse=True)
        
        if files_with_dates:
            latest_file_path = os.path.join(self.download_folder, files_with_dates[0][0])
            return latest_file_path
        
        else:
            print("Файлы с заданным префиксом не найдены.")
            return False


    def _download_data(self):
        """
        Загружает данные, кликая по кнопке на странице.
        
        :param url: URL страницы для загрузки.
        """
        self.driver.get(self.url)
        try:
            button_selector = '.Button_root__ZhrsR.Button_spb__tqqde.Button_contained__7HNa2.Button_cancel__5lME4.Button_sm__ndTMz'
            self.driver.find_element(By.CSS_SELECTOR, button_selector).click()
            time.sleep(2)  # Ожидание загрузки файла
        finally:
            self.driver.quit()


    def _clean_data(self, file_path):
        """
        Обрабатывает скачанный файл: читает и сохраняет его с новой структурой.
        
        :param file_path: Полный путь к файлу для обработки.
        """
        self.main_df = pd.read_csv(file_path, sep=';', decimal=',')
        self.main_df.columns = ["DATE", "OPEN"]
        self.main_df = pd.concat([self.main_df, self.today_df], ignore_index=True)
        self.main_df['DATE'] = pd.to_datetime(self.main_df['DATE'], format='%Y-%m-%d')

        start_date_dt = pd.to_datetime(self.start_date)
        end_date_dt = pd.to_datetime(self.end_date)

        self.main_df = self.main_df[(self.main_df['DATE'] >= start_date_dt) & (self.main_df['DATE'] <= end_date_dt)]

        date_range = pd.date_range(start=start_date_dt, end=end_date_dt)
        date_df = pd.DataFrame(date_range, columns=['DATE'])

        self.main_df = pd.merge(date_df, self.main_df, on='DATE', how='left')
        self.main_df['OPEN'] = self.main_df['OPEN'].fillna(method='ffill')

        self.main_df = self.main_df.dropna().drop_duplicates(subset='DATE', keep='last')



    def get_open_price(self):
        """
        Забирает цену открытия с Питерской Биржи и обновляет данные в архивных значениях.
        """
        html_content = self.response.text
        
        soup = BeautifulSoup(html_content, 'html.parser')
        price_elements = soup.select('.IndexesCard_value__cjTBc')

        open = float(price_elements[0].text.replace(',', '.').replace(' ', ''))
        today = datetime.now().strftime('%Y-%m-%d')

        self.today_df = pd.DataFrame({
            "DATE":[today],
            "OPEN":[open]
        })

        
'''if __name__ == "__main__":
    imoex_parser = SPBEDataDownloader(start_date="2009-01-01")
    imoex_parser.get_history_data()
    imoex_parser.main_df.to_csv("test2.csv", index= False)'''