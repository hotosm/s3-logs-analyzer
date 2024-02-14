### S3 Logs analyzer 

## Prerequisites
- You have s3 bucket created 
- You have enabled server access logging in your bucket (Note: For log object key format, choose date-based partitioning to speed up analytics and query applications.)
- You have enabled athena and make sure athena can access your logs (You can run athena on same bucket where you have logs, This can be configured directly from env variable on the script) 

## Configuration 

Make sure you have following env variables setup before you run script 

- `AWS_REGION`: The AWS region where your Athena database and S3 buckets are located.  
  **Example:** `us-west-2`

- `S3_ATHENA_OUTPUT`: The S3 location to store Athena query results. Athena stores its metadata in this location
  **Example:** `s3://your-athena-query-results-bucket/path/`

- `ATHENA_DATABASE`: The name of the Athena database to run queries against.  
  **Example:** `your_athena_database`

- `S3_LOGS_LOCATION`: S3 logs location to analyze.  
  **Example:** `'s3://bucket-name/prefix-name/account-id/region/source-bucket-name/`

- `QUERY_FILE_PATH`: The local file path to the SQL query file you want to execute with Athena. Find a sample query created here in this (repo)[./query.sql] . You can use the same if your log pattern is default log pattern from s3 server logs.
  **Example:** `/path/to/your/query.sql`

- `PARQUET_UPLOAD_LOCATION`: The S3 location where the resulting Parquet file should be uploaded. It will contain result of analysis that you can download and query later on.
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

After script is complete,  Download your parquet file and start shooting your queries following [here](./Query_result.md)

## TODO 

- Use DuckDB instead of pandas 

## Resources and useful links : 

- https://www.learnaws.org/2022/01/16/aws-athena-boto3-guide/ 
- https://repost.aws/knowledge-center/analyze-logs-athena 
- https://duckdb.run/
- https://shell.duckdb.org/
