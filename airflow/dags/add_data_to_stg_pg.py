
from airflow import DAG
import sys
sys.path.append('/opt/airflow/dags/plugins')
from plugins.castom_sensor import CustomSensorS3_minio
import duckdb as dk
from airflow.models import Variable
from minio import Minio
import pendulum
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


# s3 minio
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")
HOST_MINIO = Variable.get("HOST_minio")
BUCKET_NAME = Variable.get("bucket_name_minio")

# dwh
pg_conn = BaseHook.get_connection('postgres_db')
HOST = pg_conn.host
USER = pg_conn.login
SCHEMA = pg_conn.schema
PASSWORD = pg_conn.password
PORT = pg_conn.port


# Конфигурация DAG
OWNER = "m.safo"
DAG_ID = "add_data_to_stg_pg"
SHORT_DESCRIPTION = "Создание stg слоя в postgres"

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

def create_insert_stg_table(**context):
    #  "Moscow", "Minsk", "Saint Petersburg", "Kazan"
    prev_ds = context["prev_ds"]
    pg_conn = dk.connect()
    pg_conn.sql(f"""
    
        ATTACH 'dbname={SCHEMA} user={USER} password={PASSWORD} host={HOST} port={PORT}' AS pg_db (TYPE postgres);
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        
        CREATE TABLE IF NOT EXISTS pg_db.stg_meteo.Moscow_temp AS
        FROM 's3://prod/Moscow/Moscow_{prev_ds}_temp.parquet' LIMIT 0;
        
        CREATE TABLE IF NOT EXISTS pg_db.stg_meteo.Minsk_temp AS
        FROM 's3://prod/Minsk/Minsk_{prev_ds}_temp.parquet' LIMIT 0;
        
        CREATE TABLE IF NOT EXISTS pg_db.stg_meteo.SaintPetersburg_temp AS
        FROM 's3://prod/Saint*/Saint*_{prev_ds}_temp.parquet' LIMIT 0;
        
        CREATE TABLE IF NOT EXISTS pg_db.stg_meteo.Kazan_temp AS
        FROM 's3://prod/Kazan/Kazan_{prev_ds}_temp.parquet' LIMIT 0;
        
        INSERT INTO pg_db.stg_meteo.Moscow_temp
        SELECT * FROM 's3://prod/Moscow/Moscow_{prev_ds}_temp.parquet';
        
        INSERT INTO pg_db.stg_meteo.Minsk_temp
        SELECT * FROM 's3://prod/Minsk/Minsk_{prev_ds}_temp.parquet';
        
        INSERT INTO pg_db.stg_meteo.SaintPetersburg_temp
        SELECT * FROM 's3://prod/Saint*/Saint*_{prev_ds}_temp.parquet';
        
        INSERT INTO pg_db.stg_meteo.Kazan_temp
        SELECT * FROM 's3://prod/Kazan/Kazan_{prev_ds}_temp.parquet';
        
        """
    )
    
    
    
    
    

with DAG(dag_id=DAG_ID,
         default_args=args,
         schedule_interval="@daily",
         max_active_runs=1,
         max_active_tasks=1,
         concurrency=1,
         description=SHORT_DESCRIPTION) as dag:
    
    minio_sensor = CustomSensorS3_minio(
        task_id="minio_sensor",
        bucket=BUCKET_NAME,
        timeout=300,
        poke_interval=60,
        mode='reschedule',
        host=HOST_MINIO,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        objects = [f"{city}/{city}_{args['start_date'].subtract(days=1).strftime('%Y-%m-%d')}_temp.parquet" for city in ("Moscow", "Minsk", "Saint Petersburg", "Kazan")]
        )


    stg_table = PythonOperator(task_id="stg_table", 
                               python_callable=create_insert_stg_table, 
                               )

    minio_sensor>>stg_table