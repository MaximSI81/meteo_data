from airflow.sensors.base import BaseSensorOperator
from minio import Minio
class CustomSensorS3_minio(BaseSensorOperator):
    
    def __init__(self, bucket, object, access_key, secret_key,  *args, **kwargs): 
        self.bucket = bucket
        self.object = object
        self.access_key = access_key
        self.secret_key = secret_key
        self.file_s3 = None
        super().__init__(*args, **kwargs)
    
    def poke(self):
        minio = Minio(
            "10.130.0.11:9000",  # URL MinIO-сервера
            access_key=self.access_key,  # Логин MinIO
            secret_key=self.secret_key,  # Пароль MinIO
            secure=False,  # False = HTTP, True = HTTPS
            region = "us-east-1"
        )
        try:
            self.file_s3 = minio.stat_object(bucket_name=self.bucket, object_name=self.object)
            return True
        except:
            return False
        

                