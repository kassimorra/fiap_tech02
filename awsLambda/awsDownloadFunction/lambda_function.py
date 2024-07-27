import json
from utils import downloadB3

def lambda_handler(event, context):  
    downloadB3()
    
    return {
        'statusCode': 200,
        'body': 'Files downloaded and uploaded to S3 successfully!'
    }