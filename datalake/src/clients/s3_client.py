import logging
import os 
import json 
import boto3

from dotenv import load_dotenv

class S3Client:
    load_dotenv()
    def __init__(self):
        self.aws_access_key_id = str(os.getenv('AWS_ACCESS_KEY_ID'))
        self.aws_secret_access_key = str(os.getenv('AWS_SECRET_ACCESS_KEY'))
        self.region_name = str(os.getenv('REGION_NAME'))
        self.bucket_name = str(os.getenv('BUCKET_NAME'))
    
    def s3_connection(self):
        try:
            boto3.setup_default_session(
                aws_access_key_id = self.aws_access_key_id,
                aws_secret_access_key = self.aws_secret_access_key,
                region_name = self.region_name
            )
            
            s3 = boto3.client("s3")
            logging.info("Sucesso na conexão com s3!")
            
            return s3
        except Exception as e:
            logging.error(f"Erro ao fazer conexão com s3: {e}")
            
    def s3_upload_files(self, key: str, data: dict):
        try:
            s3 = self.s3_connection()
            
            body = json.dumps(data, ensure_ascii=False)
            
            s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json"
            )
            
            logging.info(f"Upload realizado: s3://{self.bucket_name}/key")
        except Exception as e:
            logging.error(f"Erro ao fazer upload no s3: {e}")
            raise
            