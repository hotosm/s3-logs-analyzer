# app.py
import argparse
import datetime
import gzip
import logging
import os
import textwrap

import pyarrow.parquet as pq
import s3fs
from boto_session_manager import BotoSesManager
from pyarrow import csv
from s3pathlib import S3Path

from aws_athena_query import _delete_s3_objects, run_athena_query
from email_results import send_email
from utils import check_env_vars, generate_full_report_email

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def athena_create_database_query(ATHENA_DATABASE):
    sql = textwrap.dedent(f"""CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE};""")
    print(sql)
    return sql


def athena_create_table_query(ATHENA_DATABASE, ATHENA_TABLE, S3_LOGS_LOCATION):
    sql = textwrap.dedent(
        f"""
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
        """
    )
    return sql


def athena_fetch_query(ATHENA_DATABASE, ATHENA_TABLE, SELECT_ALL=False):
    select = "*"
    if not SELECT_ALL:
        select = f"requestid, operation, SPLIT_PART(key, '/', 1) AS dir, SPLIT_PART(key, '/', 2) AS folder, SPLIT_PART(key, '/', 3) AS category, SPLIT_PART(key, '/', 4) AS geom_type, key, referrer, objectsize, httpstatus, requestdatetime, timestamp, remoteip"

    sql = textwrap.dedent(
        f"""
        SELECT {select} 
        FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
        """
    )
    return sql


def upload_df_to_s3_in_formats(df, s3_base_dir: S3Path, bsm: "BotoSesManager"):
    now = datetime.datetime.now()
    year = str(now.year)
    iso_date = now.strftime("%Y%m%dT%H%M%S")

    base_path = s3_base_dir.joinpath(str(year))
    file_name = f"{iso_date}"
    parquet_file_path = base_path.joinpath(f"{file_name}.parquet")
    csv_file_path = base_path.joinpath(f"{file_name}.csv.gz")

    if hasattr(bsm, "profile_name") and isinstance(bsm.profile_name, str):
        file_system = s3fs.S3FileSystem(profile_name=bsm.profile_name)
    else:
        credential = bsm.boto_ses.get_credentials().get_frozen_credentials()
        file_system = s3fs.S3FileSystem(
            key=credential.access_key,
            secret=credential.secret_key,
            token=credential.token,
        )
    # TODO : Merge the exisiting parquet dataset to maintain one parquet per year

    with file_system.open(parquet_file_path.uri, "wb") as f:
        pq.write_table(df, f)

    with file_system.open(csv_file_path.uri, "wb") as f:
        with gzip.GzipFile(fileobj=f, mode="wb") as gz:
            csv.write_csv(df, gz)

    s3_client = bsm.boto_ses.client("s3")

    presigned_url_csv = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": csv_file_path.bucket, "Key": csv_file_path.key},
        ExpiresIn=3600 * 24 * 7,
    )

    print(f"Uploaded files to {parquet_file_path.uri} and {csv_file_path.uri}")
    return presigned_url_csv


def main():
    parser = argparse.ArgumentParser(
        description="Process and upload Athena query results."
    )
    parser.add_argument(
        "--remove_meta",
        action="store_true",
        help="Remove metadata folder during generation, It will only remove meta parquet and manifest file not the result",
    )
    parser.add_argument(
        "--select_all",
        action="store_true",
        help="Selects all attribute from the logs table in raw format",
    )
    parser.add_argument(
        "--remove_original_logs",
        action="store_true",
        help="Removes original logs dir after result upload, Cautious with this",
    )
    parser.add_argument(
        "--email", action="store_true", help="Enable email notification"
    )
    args = parser.parse_args()

    check_env_vars(args.email)
    bsm = BotoSesManager()
    prefix = f"athena/results"
    meta_result_path = f"{os.getenv('RESULT_PATH')}/{prefix}/meta/"
    s3dir_result_meta = S3Path(meta_result_path).to_dir()
    database = os.getenv("ATHENA_DATABASE")
    table = os.getenv("ATHENA_TABLE")
    # TODO : Create tables and database first so that it can run on plain athena
    # lazy_df, exec_id = run_athena_query(
    #     bsm=bsm,
    #     s3dir_result=s3dir_result,
    #     sql=athena_create_database_query(database),
    #     database=database,
    # )
    # lazy_df, exec_id = run_athena_query(
    #     bsm=bsm,
    #     s3dir_result=s3dir_result,
    #     sql=athena_create_table_query(database, table, os.getenv("S3_LOGS_LOCATION")),
    #     database=database,
    # )
    lazy_df, exec_id = run_athena_query(
        bsm=bsm,
        s3dir_result=s3dir_result_meta,
        sql=athena_fetch_query(database, table, args.select_all),
        database=database,
    )
    df = lazy_df.collect()
    print(df.shape)
    print(df)
    result_path = f"{os.getenv('RESULT_PATH')}/{prefix}/"
    s3dir_result = S3Path(result_path).to_dir()
    presigned_url_csv = upload_df_to_s3_in_formats(
        df.to_arrow(), s3_base_dir=s3dir_result, bsm=bsm
    )
    if args.remove_meta:
        _delete_s3_objects(bsm, s3dir_result_meta)

    if args.email:
        email_body = generate_full_report_email(df, presigned_url_csv)
        target_emails = os.getenv("TARGET_EMAIL_ADDRESS").split(",")
        send_email(
            subject=f"Your {database.upper()} Usage Stats Report",
            content=email_body,
            to_emails=target_emails,
            content_type="html",
        )


if __name__ == "__main__":
    main()
