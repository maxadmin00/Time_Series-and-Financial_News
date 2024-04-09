from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from time import sleep
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime

class SPBEDataDownloader:
    def __init__(self, download_folder = os.path.join(os.getcwd(), "ml", "data"), prefix = "SPBIRUS"):
        self.download_folder = download_folder
        self.prefix = prefix
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
        file_path = self._find_downloaded_file(self.prefix)
        if file_path:
            print(f"Найден файл: {file_path}")
            self._process_downloaded_file(file_path)
        else:
            print("Файл не найден.")


    def _download_data(self):
        """
        Загружает данные, кликая по кнопке на странице.
        
        :param url: URL страницы для загрузки.
        """
        self.driver.get(self.url)
        try:
            button_selector = '.Button_root__ZhrsR.Button_spb__tqqde.Button_contained__7HNa2.Button_cancel__5lME4.Button_sm__ndTMz'
            self.driver.find_element(By.CSS_SELECTOR, button_selector).click()
            sleep(2)  # Ожидание загрузки файла
        finally:
            self.driver.quit()


    def _find_downloaded_file(self, prefix):
        """
        Ищет файл с заданным префиксом в папке загрузок.
        
        :param prefix: Префикс искомого файла.
        :return: Полный путь к файлу или None, если файл не найден.
        """
        for filename in os.listdir(self.download_folder):
            if filename.startswith(prefix):
                return os.path.join(self.download_folder, filename)
        return None


    def _process_downloaded_file(self, file_path):
        """
        Обрабатывает скачанный файл: читает и сохраняет его с новой структурой.
        
        :param file_path: Полный путь к файлу для обработки.
        """
        df = pd.read_csv(file_path, sep=';', decimal=',')
        df.columns = ["DATE", "OPEN"]
        df.to_csv(file_path, index=False)


    def get_open_price(self):
        """
        Забирает цену открытия с Питерской Биржи и обновляет данные в архивных значениях.
        """
        html_content = self.response.text
        
        soup = BeautifulSoup(html_content, 'html.parser')
        price_elements = soup.select('.IndexesCard_value__cjTBc')

        open = float(price_elements[0].text.replace(',', '.').replace(' ', ''))
        today = datetime.now().strftime('%Y-%m-%d')

        file_path = self._find_downloaded_file(self.prefix)
        if file_path:
            print(f"Найден файл: {file_path}")
            df = pd.read_csv(file_path)
            if today not in df["DATE"]:
                df.loc[len(df)] = [today, open]
                df.to_csv(file_path, index=False)

            print(f"Данные успешно обнавлены")
            
        else:
            self.get_history_data()
            self.get_open_price()
        


if __name__ == "__main__":
        download_folder = os.path.join(os.getcwd(), "ml", "data")
        parser = SPBEDataDownloader()
        parser.get_open_price()
    
