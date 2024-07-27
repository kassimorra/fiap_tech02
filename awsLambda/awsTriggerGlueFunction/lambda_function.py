import json
import boto3

def lambda_handler(event, context):
    # Log the received event
    print("Received event: " + json.dumps(event, indent=2))

    # Extract the bucket name and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Log the bucket name and object key
    print(f"Bucket: {bucket}")
    print(f"Key: {key}")

    # Initialize a Glue client
    glue_client = boto3.client('glue')

    # Start the Glue job
    try:
        response = glue_client.start_job_run(
            JobName='YOUR_GLUE_JOB_NAME',
            Arguments={
                '--bucket_name': bucket,
                '--object_key': key
            }
        )
        # Log the Glue job run ID
        print(f"Started Glue job with run ID: {response['JobRunId']}")
        
    except Exception as e:
        # Log any exceptions
        print(e)
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Glue job started successfully')
    }