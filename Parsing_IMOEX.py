import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil

class IMOEXDataDownloader:
    def __init__(self, start_date, end_date=datetime.now().strftime('%Y-%m-%d')):
        self.start_date = start_date
        self.end_date = end_date
        self.born_date = datetime.strptime("2004-01-01", '%Y-%m-%d')
        self.main_path = "ml/data"
        self.unclearn_path = os.path.join(self.main_path, "UnClearn")
        self.clearn_path = os.path.join(self.main_path, "Clearn")

    def create_folders(self):
        """
        Создает папки 'Clearn' и 'UnClearn' в указанной базовой директории.
        
        :param base_path: Базовый путь, где будут созданы папки.
        """
        
        # Проверка существования и создание папки 'Clearn', если она не существует
        if not os.path.exists(self.clearn_path):
            os.makedirs(self.clearn_path)
            print(f"Папка '{self.clearn_path}' успешно создана.")
        
        # Проверка существования и создание папки 'UnClearn', если она не существует
        if not os.path.exists(self.unclearn_path):
            os.makedirs(self.unclearn_path)
            print(f"Папка '{self.unclearn_path}' успешно создана.")

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

            url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/securities/IMOEX.csv?iss.only=history&iss.dp=comma&iss.df=%25Y-%25m-%25d&iss.tf=%25H%3A%25M%3A%25S&iss.dtf=%25Y.%25m.%25d%20%25H%3A%25M%3A%25S&from={start_date_str}&till={temporary_end_str}&limit=1000000&start=0&sort_order=&sort_order_desc="
            self._fetch_and_save_data(url, start_date_str, temporary_end_str)

            start_date_dt = temporary_end + timedelta(days=1)

    def _fetch_and_save_data(self, url, start_date_str, temporary_end_str):
        response = requests.get(url)
        if response.status_code == 200:
            filename = f"{self.unclearn_path}\IMOEX_{start_date_str}_{temporary_end_str}.csv"
            with open(filename, "wb") as file:
                file.write(response.content)
            # print(f"Файл успешно скачан: {filename}")
        else:
            print(f"Ошибка при скачивании файла: {response.status_code}")

    def clean_and_combine_data(self):
        dataframes = []
        for filename in sorted(os.listdir(self.unclearn_path)):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.unclearn_path, filename)
                new_file_path = os.path.join(self.clearn_path, filename)

                self._clean_csv(file_path, new_file_path)
                dataframes.append(self._read_clean_csv(new_file_path))

        combined_df = pd.concat(dataframes, ignore_index=True).drop_duplicates()
        combined_df.to_csv(os.path.join(self.main_path, f'IMOEX_{self.start_date}_{self.end_date}.csv'), index=False)
        print("Данные объединены и очищены.")

    def _clean_csv(self, input_path, output_path):
        with open(input_path, 'r', encoding='windows-1251') as file:
            lines = file.readlines()[1:]  # Пропуск заголовков

        with open(output_path, 'w', encoding='windows-1251') as file:
            file.writelines(lines)

    def _read_clean_csv(self, file_path):
        df =pd.read_csv(file_path, sep=';', decimal=',', encoding='windows-1251')
        df = df[['TRADEDATE', 'OPEN']].rename(columns={"TRADEDATE": "DATE"})
        df['DATE'] = pd.to_datetime(df['DATE'], format='%Y-%m-%d')
        return df.dropna()

    def clean_directories(self):
        """
        Удаляет папки UnClearn и Clearn.
        """
        self._clean_directory(self.unclearn_path)
        self._clean_directory(self.clearn_path)

    def _clean_directory(self, path):
        """
        Удаляет все файлы в указанной директории.

        :param path: Путь к директории, которую необходимо очистить.
        """
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
                print(f"Папка '{path}' успешно удалена.")
            else:
                print(f"Папка '{path}' не найдена.")

        except Exception as e:
            print(f"Не удалось удалить папку '{path}'. Причина: {e}")

if __name__ == "__main__":
    downloader = IMOEXDataDownloader(start_date="2004-01-01", end_date="2024-04-05")
    downloader.create_folders()
    downloader.download_data()
    downloader.clean_and_combine_data()
    downloader.clean_directories()