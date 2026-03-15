import sys 
import os 
from datetime import date

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

from clients.banco_central_api_client import BancoCentralApiClient
from clients.s3_client import S3Client

class MoedasExtractor:
    def __init__(self):
        self.client = BancoCentralApiClient(
            base_url="https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        )
        self.s3 = S3Client()
    
    def extract_moedas(self):
        data =  self.client.get_moedas()
        return data["value"]
    
    def save(self, data: dict):
        today = date.today().isoformat()
        key = f"landing/ptax/moedas/dt={today}/moedas.json"
        self.s3.s3_upload_files(key=key,data=data)
    
    def run(self):
        data = self.extract_moedas()
        self.save(data)

if __name__ == "__main__":
    extractor = MoedasExtractor()
    extractor.run()