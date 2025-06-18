FROM apache/airflow:2.10.5-python3.8

COPY ./requirements.txt .  
RUN pip install -r ./requirements.txt

