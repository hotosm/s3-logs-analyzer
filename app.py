import argparse
import io
import logging
import os
import sys
import tempfile
import time

import boto3
import pandas as pd

df_columns = [
    "requestid",
    "bucket_name",
    "requestdatetime",
    "operation",
    "key",
    "request_uri",
    "httpstatus",
    "errorcode",
    "objectsize",
    "totaltime",
]


def setup_logging():
    parser = argparse.ArgumentParser(
        description="Run script with optional logging level."
    )
    parser.add_argument(
        "--log",
        dest="loglevel",
        default="INFO",
        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    args, unknown = parser.parse_known_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {args.loglevel}")
    logging.basicConfig(level=numeric_level)
    global logger
    logger = logging.getLogger()
    logger.info("Logging level set to %s", args.loglevel.upper())


setup_logging()

REGION_NAME = os.getenv("AWS_REGION")
S3_ATHENA_OUTPUT = os.getenv("S3_ATHENA_OUTPUT")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE")
ATHENA_TABLE = os.getenv("ATHENA_TABLE")
S3_LOGS_LOCATION = os.getenv("S3_LOGS_LOCATION")
QUERY_FILE_PATH = os.getenv("QUERY_FILE_PATH")
PARQUET_UPLOAD_LOCATION = os.getenv("PARQUET_UPLOAD_LOCATION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

session_params = {"region_name": REGION_NAME}
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    session_params["aws_access_key_id"] = AWS_ACCESS_KEY_ID
    session_params["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if AWS_SESSION_TOKEN:
        session_params["aws_session_token"] = AWS_SESSION_TOKEN

session = boto3.Session(**session_params)
athena_client = session.client("athena")
s3_client = session.client("s3")
logger.info("AWS connection successful")


def check_env_variables():

    required_vars = [
        "S3_ATHENA_OUTPUT",
        "ATHENA_DATABASE",
        "ATHENA_TABLE",
        "S3_LOGS_LOCATION",
        "QUERY_FILE_PATH",
        "PARQUET_UPLOAD_LOCATION",
    ]
    env_vars = {}
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
        else:
            env_vars[var] = value

    if missing_vars:
        missing_vars_str = ", ".join(missing_vars)
        raise EnvironmentError(
            f"Missing required environment variables: {missing_vars_str}"
        )

    return env_vars


def read_parquet_s3(bucket, key):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        logger.info(f"Existing Parquet file read from s3://{bucket}/{key}")
        return df
    except Exception as e:
        logger.error(f"Failed to read existing Parquet file: {e}")
        return None


def check_s3_object_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        return False


def generate_create_query():
    with open(QUERY_FILE_PATH, "r", encoding="utf-8") as file:
        query = file.read()

    if ATHENA_DATABASE and S3_LOGS_LOCATION:
        query = (
            query.replace("$ATHENA_DATABASE", ATHENA_DATABASE)
            .replace("$S3_LOGS_LOCATION", S3_LOGS_LOCATION)
            .replace("$ATHENA_TABLE", ATHENA_TABLE)
        )
    return query


def generate_select_query():
    select = f"""select * from {ATHENA_DATABASE}.{ATHENA_TABLE};"""
    return select


def run_athena_query(query):
    logger.debug(query)
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": S3_ATHENA_OUTPUT},
    )
    return response["QueryExecutionId"]


def wait_for_query_to_complete(query_execution_id):
    while True:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = response["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return (
                state,
                response["QueryExecution"]["ResultConfiguration"]["OutputLocation"],
            )
        time.sleep(5)


def fetch_query_results(query_execution_id):
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    results = []
    for row in response["ResultSet"]["Rows"]:
        results.append([val.get("VarCharValue") for val in row["Data"]])
    return results


def results_to_dataframe(results):
    df = pd.DataFrame(results[1:], columns=results[0])
    logger.info("Total %s rows fetched", len(df.index))
    return df[df_columns]


def dataframe_to_parquet_s3(df, bucket, key):
    if check_s3_object_exists(bucket, key):
        logger.info(f"Object already exists in s3://{bucket}/{key}, Merging DataFrames")
        existing_df = read_parquet_s3(bucket, key)
        if existing_df is not None:
            combined_df = pd.concat([existing_df, df]).drop_duplicates(
                subset=["requestid"], keep="last"
            )
        else:
            combined_df = df
    else:
        combined_df = df
    logger.info("Preparing %s rows to upload", len(combined_df.index))

    with tempfile.NamedTemporaryFile() as tmp:
        combined_df.to_parquet(tmp.name, index=False)
        tmp.seek(0)
        try:
            s3_client.upload_file(tmp.name, bucket, key)
            logger.info(f"Combined data uploaded to s3://{bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to upload combined data: {e}")


def main():
    try:
        env_vars = check_env_variables()
        logger.info("All required environment variables are set.")
    except EnvironmentError as e:
        logger.error(e)
        sys.exit(1)
    logger.info("Athena : Creating database & tables...")

    query_execution_id = run_athena_query(generate_create_query())

    logger.info("Athena: Waiting for the create query to complete...")
    query_state, _ = wait_for_query_to_complete(query_execution_id)

    if query_state == "SUCCEEDED":
        logger.info("Athena: Create query succeeded, fetching results...")
        query_execution_id = run_athena_query(generate_select_query())
        logger.info("Athena: Waiting for the select query to complete...")
        query_state, _ = wait_for_query_to_complete(query_execution_id)
        logger.info("Athena : Select query succeeded, processing results...")

        results = fetch_query_results(query_execution_id)
        df = results_to_dataframe(results)
        s3_parts = PARQUET_UPLOAD_LOCATION.replace("s3://", "").split("/", 1)
        bucket_name = s3_parts[0]
        object_key = s3_parts[1]
        dataframe_to_parquet_s3(df, bucket_name, object_key)
    else:
        logger.error(f"Query did not succeed, ended with state: {query_state}")


if __name__ == "__main__":
    main()
