from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import requests
from minio import Minio
from io import BytesIO
import pandas as pd




# Конфигурация DAG
OWNER = "m.safo"
DAG_ID = "get_meteo_data"
# dwh
PG_CONN = "postgres_db"
# API openWeatherMap
API_KEY = Variable.get("api_key_openWeatherMap")

# s3 minio
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")


LONG_DESCRIPTION = """
# DAG для сбора данных о погоде из API OpenWeatherMap.
### Назначение
- сбор метео данных по api в формате json за каждый час 
- преоброзование данных в DataFrame
- сохранение в S3 в формате parquet за каждый день
### Источники данных
- OpenWeatherMap
### Расписание
- раз в час
"""

SHORT_DESCRIPTION = "Сбор данных о погоде"


client_minio = Minio(
    "10.130.0.11:9000",  # URL MinIO-сервера
    access_key=ACCESS_KEY,  # Логин MinIO
    secret_key=SECRET_KEY,  # Пароль MinIO
    secure=False,  # False = HTTP, True = HTTPS
    region = "us-east-1"
)


args = {'onwer': OWNER, 
        'start_date': pendulum.datetime(2025, 6, 19, tz='UTC'),
        "catchup": True,
        "retries": 3,
        "retry_delay": pendulum.duration(hours=1),}



def get_data_openweathermap(**context):
    URL = f'https://api.openweathermap.org/data/2.5/weather?q={context["city_name"]}&appid={API_KEY}&units=metric'
    responce = requests.get(url=URL)
    data = responce.json()
    df_temp = pd.DataFrame([data['main']])
    df_wind = pd.DataFrame([data['wind']])
    df_temp.insert(0, 'date', context["date_time"])
    df_temp[['deg', 'speed', 'gust']] = df_wind[['deg', 'speed', 'gust']]
    parquet_buffer_new = BytesIO()
    df = df_temp[['date', 'temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'deg', 'speed', 'gust',]]
    try:
        responce_minio = client_minio.get_object(bucket_name = "prod", object_name = f"moscow_{context['date']}_temp.parquet")
        parquet_buffer = BytesIO(responce_minio.read())
        df = pd.concat([pd.read_parquet(parquet_buffer), df])
    except:
        print('Нет файла')
    df.to_parquet(parquet_buffer_new, engine='pyarrow')
    parquet_buffer_new.seek(0)
    client_minio.put_object(
        bucket_name = "prod",
        object_name = f"moscow_{context['date']}_temp.parquet",
        data=parquet_buffer_new,
        length=parquet_buffer_new.getbuffer().nbytes,
        content_type='application/parquet',
        
    )


with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval='@hourly',  # Изменил на ежечасный запуск
    description="Сбор данных о погоде с сохранением в MinIO",
    max_active_runs=1,
    max_active_tasks=1,
    doc_md=LONG_DESCRIPTION  # Добавил длинное описание
) as dag:
    
    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=get_data_openweathermap,
        op_kwargs={
            "city_name": "Moscow",
            "date_time": '{{ ts }}',
            "date": '{{ ds }}'
        }
    )