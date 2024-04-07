from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from time import sleep
import pandas as pd
import os

def setup_chrome_driver(download_folder):
    """
    Настраивает драйвер Chrome с указанными опциями.
    
    :param download_folder: Папка для загрузки файлов.
    :return: Инициализированный драйвер.
    """
    chrome_options = Options()
    prefs = {"download.default_directory": download_folder}
    chrome_options.add_experimental_option("prefs", prefs)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def download_data(driver, url):
    """
    Загружает данные, кликая по кнопке на странице.
    
    :param driver: Драйвер браузера.
    :param url: URL страницы для загрузки.
    """
    driver.get(url)
    try:
        button_selector = '.Button_root__ZhrsR.Button_spb__tqqde.Button_contained__7HNa2.Button_cancel__5lME4.Button_sm__ndTMz'
        driver.find_element(By.CSS_SELECTOR, button_selector).click()
        sleep(2)  # Ожидание загрузки файла
    finally:
        driver.quit()

def find_downloaded_file(download_folder, prefix):
    """
    Ищет файл с заданным префиксом в папке загрузок.
    
    :param download_folder: Папка для поиска.
    :param prefix: Префикс искомого файла.
    :return: Полный путь к файлу или None, если файл не найден.
    """
    for filename in os.listdir(download_folder):
        if filename.startswith(prefix):
            return os.path.join(download_folder, filename)
    return None

def process_downloaded_file(file_path):
    """
    Обрабатывает скачанный файл: читает и сохраняет его с новой структурой.
    
    :param file_path: Полный путь к файлу для обработки.
    """
    df = pd.read_csv(file_path, sep=';', decimal=',')
    df.columns = ["DATE", "OPEN"]
    df.to_csv(file_path, index=False)

if __name__ == "__main__":
    download_folder = os.path.join(os.getcwd(), "ml", "data")
    url = "https://spbexchange.ru/stocks/indices/SPBIRUS/"
    driver = setup_chrome_driver(download_folder)
    download_data(driver, url)
    prefix = "SPBIRUS"
    full_path = find_downloaded_file(download_folder, prefix)
    if full_path:
        print(f"Найден файл: {full_path}")
        process_downloaded_file(full_path)
    else:
        print("Файл не найден.")
