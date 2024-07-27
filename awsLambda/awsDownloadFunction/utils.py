import requests, zipfile, io
import boto3

def downloadB3():
    year = '2024'
    month = '07'
    day = '11'
    zip_file_url = 'https://arquivos.b3.com.br/apinegocios/tickercsv/' + f'{year}-{month}-{day}'

    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    s3 = boto3.client('s3')
    s3_bucket = "fiap-mlet52-raw-bucket"

    for file_info in z.infolist():
        file_name = file_info.filename
        file_data = z.read(file_name)
        s3_key = f"Download/{file_name}"
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_data)