import os 
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
from pathlib import Path

default_path = Path(__file__).parent.parent

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

AWS_BUCKET_NAME="olist-datalake-nean"
AWS_REGION="us-east-1"
LOCAL_DATA_DIR = default_path / 'data' / 'raw'
S3_PREFIX = "raw/olist"

def get_cliente():
    return boto3.client("s3")


def upload_file():

    if not os.path.exists(LOCAL_DATA_DIR):
        print(f"❌ Erro: A pasta '{LOCAL_DATA_DIR}' não foi encontrada.")
        return

    
    s3 = get_cliente()

    arquivos = list(LOCAL_DATA_DIR.glob("*.csv"))

    if not arquivos:
        print(f"⚠️ Nenhum arquivo CSV encontrado na pasta '{LOCAL_DATA_DIR}'.")
        return
    print(f"🚀 Iniciando upload de {len(arquivos)} arquivos para o S3 (Bucket: {AWS_BUCKET_NAME})...")

    for file_path in arquivos:
        s3_key = f"{S3_PREFIX}/{file_path.name}"
        try:
            print(f"⏳ Subindo: {file_path.name} -> s3://{AWS_BUCKET_NAME}/{s3_key}")
            s3.upload_file(str(file_path), AWS_BUCKET_NAME, s3_key)
        except NoCredentialsError:
            print("❌ Erro: Credenciais da AWS não encontradas. Verifique o seu arquivo .env!")
            break
        except ClientError as e:
            print(f"❌ Erro da AWS no arquivo {file_path.name}: {e}")


if __name__ == "__main__":
    upload_file()