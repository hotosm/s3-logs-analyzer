### S3 Logs analyzer 

## Configuration 

Make sure you have following env variables setup before you run script 

- `AWS_REGION`: The AWS region where your Athena database and S3 buckets are located.  
  **Example:** `us-west-2`

- `S3_ATHENA_OUTPUT`: The S3 location to store Athena query results.  
  **Example:** `s3://your-athena-query-results-bucket/path/`

- `ATHENA_DATABASE`: The name of the Athena database to run queries against.  
  **Example:** `your_athena_database`

- `S3_LOGS_LOCATION`: S3 logs location to analyze.  
  **Example:** `'s3://bucket-name/prefix-name/account-id/region/source-bucket-name/`

- `QUERY_FILE_PATH`: The local file path to the SQL query file you want to execute with Athena.  
  **Example:** `/path/to/your/query.sql`

- `PARQUET_UPLOAD_LOCATION`: The S3 location where the resulting Parquet file should be uploaded.  
  **Example:** `s3://your-result-bucket/path/to/results.parquet`

### Optional 

These are optional and should be used if you need to explicitly specify AWS credentials.

- `AWS_ACCESS_KEY_ID`: Your AWS access key ID for accessing AWS services.  
  **Example:** `fdsgfdsgfdsgfsdg`

- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key for accessing AWS services.  
  **Example:** `xyz/xz/fdsag`

- `AWS_SESSION_TOKEN` : Your AWS Session token 

## Usage 

After configuring env variables, You can simply run this script with following steps 

### Install 

```shell
pip install -r requirements.txt
```

### Run 

```shell
python app.py
```

This will pickup the server access logs from your bucket, analyze it using athena, download the result to pandas dataframe and upload result as parquet in your desired s3 location . If the parquet already present in the remote then it will read from it, combine the results and upload the updated data back to remote again. You can run this script frequently aligning with your logs cleanup lifecycle policy ! Run this script before the logs cleanup !   

## TODO 

- Use DuckDB instead of pandas 

## Resources and useful links : 

- https://www.learnaws.org/2022/01/16/aws-athena-boto3-guide/ 
- https://repost.aws/knowledge-center/analyze-logs-athena 
- https://duckdb.run/
- https://shell.duckdb.org/