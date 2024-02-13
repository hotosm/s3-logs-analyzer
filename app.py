import os
import boto3
import pandas as pd
import logging
import io
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

REGION_NAME = os.getenv('AWS_REGION')
S3_ATHENA_OUTPUT = os.getenv('S3_ATHENA_OUTPUT')
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE')
QUERY_FILE_PATH = os.getenv('QUERY_FILE_PATH')
PARQUET_UPLOAD_LOCATION = os.getenv('PARQUET_UPLOAD_LOCATION')  
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
        region_name=REGION_NAME
    )
else:
    session = boto3.Session(region_name=REGION_NAME)

athena_client = session.client('athena')
s3_client = session.client('s3')

def run_athena_query():
    with open(QUERY_FILE_PATH, 'r') as file:
        query = file.read()

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_ATHENA_OUTPUT}
    )
    return response['QueryExecutionId']

def wait_for_query_to_complete(query_execution_id):
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return state, response['QueryExecution']['ResultConfiguration']['OutputLocation']
        time.sleep(5)

def fetch_query_results(query_execution_id):
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    results = []
    for row in response['ResultSet']['Rows']:
        results.append([val.get('VarCharValue') for val in row['Data']])
    return results

def results_to_dataframe(results):
    df = pd.DataFrame(results[1:], columns=results[0])
    return df

def dataframe_to_parquet_s3(df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        existing_df = pd.read_parquet(f"s3://{bucket}/{key}")
        new_df = pd.concat([existing_df, df], ignore_index=True)
        buffer = io.BytesIO()
        new_df.to_parquet(buffer, index=False)
        buffer.seek(0)
    except s3_client.exceptions.NoSuchKey:
        logger.info("Parquet file does not exist, creating a new one.")
    
    s3_client.upload_fileobj(buffer, bucket, key)
    logger.info(f"Data uploaded to s3://{bucket}/{key}")

def main():
    logger.info("Running Athena query...")
    query_execution_id = run_athena_query()
    
    logger.info("Waiting for the query to complete...")
    query_state, _ = wait_for_query_to_complete(query_execution_id)
    
    if query_state == 'SUCCEEDED':
        logger.info("Query succeeded, processing results...")
        results = fetch_query_results(query_execution_id)
        df = results_to_dataframe(results)
        
        bucket, key = PARQUET_UPLOAD_LOCATION.split('/', 2)[2].split('/', 1)
        dataframe_to_parquet_s3(df, bucket, key)
    else:
        logger.error(f"Query did not succeed, ended with state: {query_state}")

if __name__ == "__main__":
    main()
