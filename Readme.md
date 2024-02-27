# S3 Logs analyzer 

This repo contains set of scripts designed to simplify the process of querying data from AWS Athena, processing the results, and optionally uploading these results to AWS S3 in both Parquet and CSV formats. It also supports sending an email notification with the results to specified recipients. This tool is particularly useful for application with AWS S3 services who need to automate their data usuage query and reporting workflows.

## Prerequisites
- You have s3 bucket created 
- You have enabled server access logging in your bucket (Note: For log object key format, choose date-based partitioning to speed up analytics and query applications.)
- You have enabled athena and made sure athena can access your logs (You can run athena on same bucket where you have logs, This can be configured directly from env variable on the script) 
- You have python and virtualenv installed on your machine

## Configuration 

Make sure you have following env variables setup before you run script 

- `ATHENA_DATABASE`: The name of the Athena database to run queries against.  
  **Example:** `your_athena_database`

- `ATHENA_TABLE`: The table in Athena to query. 
  **Example:** `logs`

- `S3_LOGS_LOCATION`: S3 logs location to analyze.  
  **Example:** `'s3://bucket-name/prefix-name/account-id/region/source-bucket-name/`

- `RESULT_PATH`: The S3 location where the resulting Parquet file should be uploaded. It will contain result of analysis that you can download and query later on.
  **Example:** `s3://your-result-bucket/path/to/`



### Email 
if you want to setup email service then you need following env variables 

- **`SMTP_HOST`**: Hostname or IP address of the SMTP server for sending email notifications.
  - **Example:** `smtp.example.com`

- **`SMTP_USERNAME`**: Username for SMTP server authentication, often an email address.
  - **Example:** `your-email@example.com`

- **`SMTP_PASSWORD`**: Password for SMTP_USERNAME for server authentication.
  - **Example:** `yourpassword`

- **`REPLY_TO_EMAIL`**: Email address for recipients to send responses if they reply.
  - **Example:** `reply-to@example.com`

- **`FROM_EMAIL`**: The "From" email address displayed as the sender in emails.
  - **Example:** `no-reply@example.com`

- **`TARGET_EMAIL_ADDRESS`**: Email addresses to send notifications to, separated by commas.
  - **Example:** `test@gmail.com,test2@hotmail.com`


### Optional 

These are optional and should be used if you need to explicitly specify AWS credentials. Other wise boto3 session manager will automatically instantiate if you have aws credentials setup

- `AWS_REGION`: The AWS region where your Athena database and S3 buckets are located.  
  **Example:** `us-west-2`

- `AWS_ACCESS_KEY_ID`: Your AWS access key ID for accessing AWS services.  
  **Example:** `fdsgfdsgfdsgfsdg`

- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key for accessing AWS services.  
  **Example:** `xyz/xz/fdsag`

- `AWS_SESSION_TOKEN` : Your AWS Session token 

- `AWS_PROFILE_NAME` : Your AWS Profile Name 

### Setting Up Environment Variables

Set the above variables in your operating system or deployment environment before running the application. Here's how to set them in Unix-like systems (Linux/macOS) and Windows.

#### Unix-like Systems (Linux/macOS):
Example :

```sh
export SMTP_HOST=smtp.example.com
export SMTP_USERNAME=your-email@example.com
export SMTP_PASSWORD=yourpassword
export REPLY_TO_EMAIL=reply-to@example.com
export FROM_EMAIL=no-reply@example.com
export TARGET_EMAIL_ADDRESS=test@gmail.com,test2@hotmail.com
```

## Setup 

- Open Athena editor on your aws home screen

- Create database for your application
```sql
CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE};
```

- Create logs table 

Replace S3_LOGS_LOCATION ,ATHENA_DATABASE ,ATHENA_TABLE env variable accordingly

```sql
CREATE EXTERNAL TABLE if not exists {ATHENA_DATABASE}.{ATHENA_TABLE}(
    `bucketowner` STRING, 
    `bucket_name` STRING, 
    `requestdatetime` STRING, 
    `remoteip` STRING, 
    `requester` STRING, 
    `requestid` STRING, 
    `operation` STRING, 
    `key` STRING, 
    `request_uri` STRING, 
    `httpstatus` STRING, 
    `errorcode` STRING, 
    `bytessent` BIGINT, 
    `objectsize` BIGINT, 
    `totaltime` STRING, 
    `turnaroundtime` STRING, 
    `referrer` STRING, 
    `useragent` STRING, 
    `versionid` STRING, 
    `hostid` STRING, 
    `sigv` STRING, 
    `ciphersuite` STRING, 
    `authtype` STRING, 
    `endpoint` STRING, 
    `tlsversion` STRING, 
    `accesspointarn` STRING, 
    `aclrequired` STRING
) PARTITIONED BY ( `timestamp` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ( 
    'input.regex'='([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ("[^"]*"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ("[^"]*"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$'
) 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
LOCATION '{S3_LOGS_LOCATION}' 
TBLPROPERTIES ( 
    'projection.enabled'='true', 
    'projection.timestamp.format'='yyyy/MM/dd', 
    'projection.timestamp.interval'='1', 
    'projection.timestamp.interval.unit'='DAYS', 
    'projection.timestamp.range'='2024/01/01,NOW', 
    'projection.timestamp.type'='date', 
    'storage.location.template'='{S3_LOGS_LOCATION}${{timestamp}}'
);
  ```


## Usage 

After configuring env variables and table creatoin, You can simply run this script with following steps 

### Install 

```shell
pip install -r requirements.txt
```

### Run 

```shell
python app.py
```

### Options 

#### Command Line Arguments

The application supports several command line arguments to control its behavior:

- `--remove_meta`: Removes the metadata folder created during query execution. This includes meta parquet and manifest files but not the query results.
- `--select_all`: Selects all attributes from the logs table in their raw format.
- `--remove_original_logs`: Deletes the original logs directory after the results have been uploaded. Use this option with caution, if output of results and logs are in your same mother dir then entire folder will be deleted. Make sure you are using different dir path before enabling this option. It is advised to use bucket lifecycle policy for deletion of logs 
- `--email`: Enables email notifications to be sent with the query results.
- `--verbose`: Displays additional information during execution, such as DataFrame shapes and content.
- `--frequency`: Specifies the frequency of data extraction. Users can choose between `weekly`, `monthly`, or `quarterly`. This option determines the time range of logs to be queried and processed. By default, the frequency is set to `monthly`.
  
  Example: `--frequency monthly`

- `--date_range`: Allows users to specify a custom date range for the data extraction. The date range should be provided in `YYYY-MM-DD` format, including both the start and end dates. This option is mutually exclusive with the `--frequency` option, meaning you can either specify a frequency or a custom date range, but not both.
  
  Example: `--date_range 2024-01-01 2024-01-31`

### Frequency to Date Range Conversion

This script allows users to specify the frequency of logs data extraction through the `--frequency` argument or define a custom date range using the `--date_range` argument. This section explains how the `frequency` option is interpreted and converted into specific date ranges.

#### Weekly

When the `--frequency` is set to `weekly`, the script calculates the date range to cover the complete week (Monday to Sunday) preceding the current date. This means if the script is run at any point during a week, it will select logs from the Monday to Sunday of the previous week, ensuring that the data for a complete week is analyzed without including partial data from the ongoing week.

#### Monthly

Setting the `--frequency` to `monthly` will adjust the date range to encompass the entire month immediately before the current month. This ensures that only complete months are considered for analysis, avoiding the inclusion of incomplete data for the current month. The script calculates the first and last day of the previous month to define this range.

#### Quarterly

The `quarterly` frequency option leads to the selection of logs from the complete quarter preceding the current date. Quarters are divided into three-month periods starting from January, April, July, and October. The script identifies the current quarter and then selects the entire previous quarter as the date range for analysis.

#### Custom Date Range (`--date_range`)

In addition to the predefined frequencies, users can specify a custom date range using the `--date_range` argument followed by the start and end dates in `YYYY-MM-DD` format. This option overrides the `--frequency` setting, allowing for more granular control over the period of logs to be analyzed.


#### Example 

To run the application with specific options, you can use the command line. Here is an example that runs a query, selects all attributes, uploads the results to S3, and sends an email notification with additional email report:

```sh
python app.py --select_all --email --verbose
```

## Consideration 

- After generating the stats you might want to setup the lifecycle policy for server logs as all the result will be saved in .parquet format in your result path. You can sync the frequency of this script with lifecycle policy.

## Workflow

1. **Setup AWS S3 Access Logs**: Configure your AWS S3 buckets to capture access logs. These logs are instrumental in monitoring and analyzing access patterns, potential security threats, and overall bucket usage.

2. **Analyze Logs with Athena**: Utilize AWS Athena for querying the S3 access logs. This serverless interactive query service makes it easy to analyze data directly in Amazon S3 using standard SQL. Prepare your SQL queries to extract meaningful information from the logs.

3. **Manage AWS Resources with Boto3 Session Manager**: Implement boto3 session manager for efficient AWS resource management. This allows for seamless interaction with AWS services within the script, enabling dynamic resource allocation and management based on the analysis needs.

4. **Path Handling with S3Pathlib**: Use s3pathlib for intuitive and effective S3 path handling. This library simplifies operations like reading from and writing to S3 buckets, making it easier to manage the data files generated during the analysis.

5. **Data Processing with PyArrow**: Leverage pyarrow for processing the query results obtained from Athena. PyArrow offers efficient data frame operations and IO capabilities, allowing for the transformation of raw query results into structured data formats suitable for analysis.

6. **Export Results to Parquet and Compressed CSV**: Process the data into a single parquet file for each execution, ensuring efficient storage and access. Additionally, generate a corresponding compressed CSV file (.csv.gz) for broader compatibility and ease of sharing.

7. **Email Reporting (Optional)**: If enabled, generate a comprehensive report from the pandas DataFrame containing the analysis results. The report is then emailed to specified recipients, including a downloadable link to the compressed CSV file. This feature facilitates easy sharing of insights and fosters informed decision-making.


## Resources and credits : 

- https://www.learnaws.org/2022/01/16/aws-athena-boto3-guide/ 
- https://repost.aws/knowledge-center/analyze-logs-athena 
- https://learn-aws.readthedocs.io/Analytics/Athena-Root/98-Athena-Best-Practice/Write-Athena-Query-Results-in-Parquet-Avro-ORC-JSON-Format/index.html
- https://duckdb.run/
- https://shell.duckdb.org/

