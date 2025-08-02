import json
import boto3

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    object = event['Records'][0]['s3']['object']['key']
    print(bucket)
    print(object)
    input_path = 's3://'+bucket+'/'+object
    output_path = 's3://processed-data-bucket-5f593a/orders'
    arguments={'--s3-input-path':input_path,'--s3-output-path':output_path}
    glue_client = boto3.client('glue')
    repsonse = glue_client.start_job_run(JobName='glue_etl_process_orders',Arguments=arguments)
    return {
        'statusCode': 200,
        'body': json.dumps('Glue job run successfully!'),
        'additional_arguments': arguments,
        'job_response': repsonse
    }
