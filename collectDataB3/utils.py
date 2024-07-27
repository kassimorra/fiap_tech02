import requests
from io import BytesIO
import zipfile
import pandas as pd
from botocore.exceptions import NoCredentialsError
import boto3
import awsCredentials.credentials as awsKey

from datetime import datetime, timedelta
last_day = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

def ajustUrl():
    url = "https://arquivos.b3.com.br/apinegocios/tickercsv/"
    url = url + last_day
    return url

def download() -> pd:
    response = requests.get(ajustUrl())
    if(response.status_code == 200):
        zip_file = BytesIO(response.content)
        with zipfile.ZipFile(zip_file, 'r') as z:
            txt_filename = z.namelist()[0]
            with z.open(txt_filename) as f:
                dataframe_B3_csv = pd.read_csv(f, delimiter=';')
                dataframe_B3_csv.to_csv('files/' + last_day + '-B3.csv', sep=';', index=False)

def s3_client():
    session = boto3.Session(
        aws_access_key_id=awsKey.aws_access_key_id,
        aws_secret_access_key=awsKey.aws_secret_access_key,
        aws_session_token=awsKey.aws_session_token
    )
    s3_client = session.client('s3')
    return s3_client

def csv2Parquet(path, filename, extension) -> None:
    df = pd.read_csv(path + filename + extension, sep=';')
    df.to_parquet(path + filename + '.parquet', engine='fastparquet')
    print(f"File saved as Parquet: {path + filename + '.parquet'}")

def moveFile2B3(file_name, bucket, s3_client ,prefix=None):
    try:
        s3_client.put_object(Bucket=bucket, Key=("B3/")) # + last_day))
        s3_client.upload_file('upload/' + file_name, bucket, "B3/" + last_day + '/' + file_name)
        
    except NoCredentialsError:
        print("As credenciais não estão disponíveis")
        return False
    
    except ValueError as ve:
        print(ve)
        return False
    
    except Exception as e:
        print(f"Erro ao salvar arquivo na B3: {e}")
        return False