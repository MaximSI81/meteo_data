from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import requests
from minio import Minio
from io import BytesIO
import pandas as pd
from minio.error import S3Error



# Конфигурация DAG
OWNER = "m.safo"
DAG_ID = "get_meteo_data"

# API openWeatherMap
API_KEY = Variable.get("api_key_openWeatherMap")

# s3 minio
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")
HOST_MINIO = Variable.get("HOST_minio")
BUCKET_NAME = Variable.get("bucket_name_minio")

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
    HOST_MINIO,  # URL MinIO-сервера
    access_key=ACCESS_KEY,  # Логин MinIO
    secret_key=SECRET_KEY,  # Пароль MinIO
    secure=False,  # False = HTTP, True = HTTPS
    region = "us-east-1"
)


args = {'owner': OWNER, 
        'start_date': pendulum.datetime(2025, 6, 22, tz="Europe/Moscow"),
        "catchup": False,
        "retries": 1,
        "retry_delay": pendulum.duration(hours=1),}



def get_data_openweathermap(**context):
    ''' получам и записываем данные в minio '''
    for city in context['city_name']:
        URL = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric'
        responce = requests.get(url=URL)  # запрос к openWeatherMap
        print(responce.status_code)
        data = responce.json()  # ответ в json
        
        df = pd.DataFrame([{**data['main'], **data['wind']}])
        df['clouds'] = data['clouds']['all']
        df['gust'] = pd.NA if 'gust' not in data['wind'] else df['gust']
        df['rain'] = pd.NA if 'rain' not in data else data['rain']['1h']
        df['snow'] = pd.NA if 'snow' not in data else data['snow']['1h']
        df.insert(0, 'date', context["date_time"])  # добавляем дату и время запроса
        try:
            responce_minio = client_minio.get_object(bucket_name = BUCKET_NAME, object_name = f"{city}/{city}_{context['date']}_temp.parquet") # получаем файл из minio если существует
            parquet_buffer = BytesIO(responce_minio.read()) # читаем и создаем буфер - файлоподобный объект
            responce_minio.close()  # закрываем соеденение
            responce_minio.release_conn() # Освобождает соединение в пуле
            df = pd.concat([pd.read_parquet(parquet_buffer), df])  # объеденяем прочитанный df с новым
        except S3Error as error:
            print(f"Файл {city}/{city}_{context['date']}_temp.parquet не найден - {error}")
        parquet_buffer_new = BytesIO() #  создаем новый буфер
        df.to_parquet(parquet_buffer_new, engine='pyarrow', index=False) # записываем parquet файл в буфер
        parquet_buffer_new.seek(0)  # Сбрасываем позицию чтения
            # загружаем в minio
        client_minio.put_object(
            bucket_name = BUCKET_NAME,
            object_name = f"{city}/{city}_{context['date']}_temp.parquet",
            data=parquet_buffer_new,
            length=parquet_buffer_new.getbuffer().nbytes,
            content_type='application/parquet',
        )  

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval='@hourly',  # запуск каждый час
    description=SHORT_DESCRIPTION,  # короткое описание
    max_active_runs=1,
    max_active_tasks=1,
    doc_md=LONG_DESCRIPTION  # Полное описание
) as dag:
    
    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=get_data_openweathermap,
        op_kwargs={
            "city_name": ["Moscow", "Minsk", "Saint Petersburg", "Kazan"],
            "date_time": '{{ ts }}',
            "date": '{{ ds }}'
        }
    )