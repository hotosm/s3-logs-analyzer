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


## TODO 

- Use DuckDB instead of pandas 

## Credits and resources : 

- https://www.learnaws.org/2022/01/16/aws-athena-boto3-guide/ 
- https://repost.aws/knowledge-center/analyze-logs-athena 