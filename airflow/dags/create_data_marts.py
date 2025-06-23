from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum



# Конфигурация DAG
OWNER = "m.safo"
DAG_ID = "create_data_marts"


SHORT_DESCRIPTION = "Создаем дашборд"


PG_CONN = "postgres_db"


args = {'owner': OWNER, 
        'start_date': pendulum.datetime(2025, 6, 22, tz="Europe/Moscow"),
        "catchup": False,
        "retries": 1,
        "retry_delay": pendulum.duration(hours=1),}


with DAG(dag_id=DAG_ID,
         default_args=args,
         schedule_interval="@daily",
         max_active_runs=1,
         max_active_tasks=1,
         concurrency=1,
         description=SHORT_DESCRIPTION) as dag:
    
    create_dm = SQLExecuteQueryOperator(
        task_id = "create_data_marts",
        conn_id=PG_CONN,
        sql="""DROP TABLE IF EXISTS dm.temp;
               CREATE TABLE dm.temp ( date TIMESTAMP,
							   temp_Moskow float,
							   temp_Minsk float,
							   temp_SaimtPeterburg float,
							   temp_Kazan float
								);
							
                INSERT INTO dm.temp 
                SELECT date::timestamp AS date, kaz.temp, mi.temp, mo.temp, sp.temp
                FROM stg_meteo."Kazan_temp" kaz JOIN stg_meteo."Minsk_temp" mi using(date) JOIN stg_meteo."Moscow_temp" mo using(date) JOIN stg_meteo."SaintPetersburg_temp" sp USING(date)
                WHERE date::date = '{{ prev_ds }}';
                
            DROP TABLE IF EXISTS dm.wind;
               CREATE TABLE dm.wind ( date TIMESTAMP,
							   speed_Moskow float,
							   speed_Minsk float,
							   speed_SaimtPeterburg float,
							   speed_Kazan float
								);
							
                INSERT INTO dm.wind
                SELECT date::timestamp AS date, kaz.speed, mi.speed, mo.speed, sp.speed
                FROM stg_meteo."Kazan_temp" kaz JOIN stg_meteo."Minsk_temp" mi using(date) JOIN stg_meteo."Moscow_temp" mo using(date) JOIN stg_meteo."SaintPetersburg_temp" sp USING(date)
                WHERE date::date = '{{ prev_ds }}';
                
            DROP TABLE IF EXISTS dm.clouds;
               CREATE TABLE dm.clouds ( date TIMESTAMP,
							   clouds_Moskow float,
							   clouds_Minsk float,
							   clouds_SaimtPeterburg float,
							   clouds_Kazan float
								);
							
                INSERT INTO dm.clouds
                SELECT date::timestamp AS date, kaz.clouds, mi.clouds, mo.clouds, sp.clouds
                FROM stg_meteo."Kazan_temp" kaz JOIN stg_meteo."Minsk_temp" mi using(date) JOIN stg_meteo."Moscow_temp" mo using(date) JOIN stg_meteo."SaintPetersburg_temp" sp USING(date)
                WHERE date::date = '{{ prev_ds }}';"""
    )
    
    