import requests
import time
import sys
import os
import pytz  # Для работы с часовыми поясами
import pandas as pd
from datetime import datetime

'''
    API DOC: https://openapi.wildberries.ru/supplies/api/en/#tag/Information-for-forming-supplies
'''

# Логирование ошибок
log_file = 'script_log.txt'
sys.stderr = open(log_file, 'a')

# S3 options
PROJECT_NAME = 'warehouse_coef'
GRANULARITY = 'by_second'
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = 'test-sellerplan'
ENDPOINT = "https://storage.yandexcloud.net"
STORAGE_OPTIONS = {
    'endpoint_url': ENDPOINT,
    'key': AWS_ACCESS_KEY_ID,
    'secret': AWS_SECRET_ACCESS_KEY,
    'client_kwargs': {
        'region_name': 'ru-central1',
    }
}

# API Options
API_URL = os.environ.get('WB_API_KEY')
API_URL = 'https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients'
headers = {
        'Authorization': API_KEY
    }

# Variables
timezone_name = 'Europe/Moscow'
moscow_tz = pytz.timezone(timezone_name)
columns_to_filter = ['date', 'coefficient', 'warehouseName', 'boxTypeName', 'created_dttm']
load_interval = 10 # Согласно доке, максимум можно делать 6 вызовов в минуту


def s3_save_parquet(df, granularity, current_datetime):
    
    # Формируем время и дату для названия файла

    file_datetime = current_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    file_hour = current_datetime.replace(minute = 0, second = 0).strftime('%Y-%m-%dT%H:%M:%S')
    file_path_name = f's3://{S3_BUCKET_NAME}/project={PROJECT_NAME}/{granularity}/{file_hour}/{file_datetime}.parquet'
    df.to_parquet(file_path_name, engine = 'fastparquet', storage_options = STORAGE_OPTIONS)
    print(f'Saved file with S3 path: {file_path_name}')

# Функция для получения списка складов
def get_warehouses_coefs(current_datetime):
    current_datetime = current_datetime.strftime('%d.%m.%Y %H:%M:%S')
    try:
        response = requests.get(API_URL, headers=headers, verify=False)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%d.%m.%Y')

        # Добавляем столбец с текущей датой и временем по Московскому времени
        df['created_dttm'] = current_datetime
        return df
    except Exception as e:
        print(f"[{current_datetime}] Ошибка: {e}/n", file=sys.stderr)
        return None

def df_add_date_columns(df):
    # Конвертируем столбцы c датами в корректный формат
    df['created_dttm'] = pd.to_datetime(df['created_dttm'], format='%d.%m.%Y %H:%M:%S')
    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    # Добавляем столбец с показателем "Дней до отгрузки"
    df['days_before_shipment'] = (df['date'] - pd.to_datetime(df['created_dttm'].dt.date, format='%d.%m.%Y')).dt.days
    # Добавляем новый столбец с округленной до часа датой
    df['created_hour'] = df['created_dttm'].dt.floor('h')
    return df

while True:
    # Загружаем данные
    current_datetime = datetime.now(moscow_tz)
    df_coefs = get_warehouses_coefs(current_datetime)
    if df_coefs is not None:
        # Фильтруем данные
        df_coefs_filtered = df_coefs.query("coefficient!=-1")
        df_coefs_filtered = df_coefs_filtered[columns_to_filter]
        df_coefs_filtered = df_add_date_columns(df_coefs_filtered)

        # Создаем название файла с текущей датой и временем
        s3_save_parquet(df_coefs_filtered, GRANULARITY, current_datetime)
        
    else:
        print(f"[{current_datetime}] Данные не были загружены", file=sys.stderr)

    time.sleep(load_interval)