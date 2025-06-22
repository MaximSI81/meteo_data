from airflow.sensors.base import BaseSensorOperator
from minio import Minio


class CustomSensorS3_minio(BaseSensorOperator):
  
    
    def __init__(self, host, bucket, objects, access_key, secret_key,  *args, **kwargs): 
        self.bucket = bucket
        self.objects = objects
        self.access_key = access_key
        self.secret_key = secret_key
        self.host = host
        super().__init__(*args, **kwargs)
    
    def poke(self, *args, **kwargs):
        print(self.objects)
        minio = Minio(
            self.host,  # URL MinIO-сервера
            access_key=self.access_key,  # Логин MinIO
            secret_key=self.secret_key,  # Пароль MinIO
            secure=False,  # False = HTTP, True = HTTPS
            region = "us-east-1"
        )
        try:
            for object in self.objects:
                minio.stat_object(bucket_name=self.bucket, object_name=object)
            return True
        except:
            return False
        

                