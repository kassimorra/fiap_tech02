import requests
from datetime import datetime, timedelta
import boto3
from io import BytesIO
import zipfile
import pandas as pd
from botocore.exceptions import NoCredentialsError

access_key = ''
secret_key = ''
session_token = ''
file2SendPath = 'colectDataB3/dfB3.parquet'

def ajustUrl():
    url = "https://arquivos.b3.com.br/apinegocios/tickercsv/"
    last_day = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    url = url + last_day
    return url

def csv2Parquet(df):
    df.to_parquet(file2SendPath, engine='fastparquet')

def download():
    response = requests.get(ajustUrl())
    if(response.status_code == 200):
        zip_file = BytesIO(response.content)
        with zipfile.ZipFile(zip_file, 'r') as z:
            txt_filename = z.namelist()[0]
            with z.open(txt_filename) as f:
                dataframe_B3_csv = pd.read_csv(f, delimiter=';')
                csv2Parquet(dataframe_B3_csv)

def moveFile2B3(file_name,bucket, prefix=None):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )
    s3_client = session.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, file_name.split("/")[-1])
        
    except NoCredentialsError:
        print("As credenciais não estão disponíveis")
        return False
    except ValueError as ve:
        print(ve)
        return False
    except Exception as e:
        print(f"Erro ao salvar arquivo na B3: {e}")
        return False
    
download()
moveFile2B3(file2SendPath,'b3-files-grupo-mlet52')