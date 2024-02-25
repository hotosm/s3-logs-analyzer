# app.py
import argparse
import datetime
import gzip
import logging
import os
import smtplib
import sys
import textwrap
from email.message import EmailMessage

import pandas as pd
import pyarrow.parquet as pq
import s3fs
from boto_session_manager import BotoSesManager
from pyarrow import csv
from s3pathlib import S3Path

from aws_athena_query import _delete_s3_objects, run_athena_query

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def check_env_vars(enable_email=False):

    required_vars = [
        "S3_LOGS_LOCATION",
        "ATHENA_DATABASE",
        "ATHENA_TABLE",
        "RESULT_PATH",
    ]
    if enable_email:
        required_vars.extend(
            [
                "SMTP_TARGET_EMAIL_ADDRESS",
                "SMTP_HOST",
                "SMTP_PORT",
                "EMAIL_USER",
                "EMAIL_PASSWORD",
            ]
        )
    missing_vars = [var for var in required_vars if var not in os.environ]
    if missing_vars:
        logger.error("Missing environment variables: " + ", ".join(missing_vars))
        sys.exit(1)


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


def generate_full_report_email(df):
    if not isinstance(df, pd.DataFrame):
        df = df.to_pandas()

    df["requestdatetime"] = pd.to_datetime(
        df["requestdatetime"], format="%d/%b/%Y:%H:%M:%S %z"
    )
    df["objectsize"] = pd.to_numeric(df["objectsize"], errors="coerce").fillna(0)
    df["method"] = df["operation"].apply(lambda x: x.split(".")[1] if "." in x else x)
    df["top_level_key"] = df["key"].apply(lambda x: x.split("/")[0])

    df["top_level_key"] = df["top_level_key"].replace("-", "default")

    # summary overall
    total_downloads = df[df["method"] == "GET"]["objectsize"].count()
    total_uploads = df[df["method"] == "PUT"]["objectsize"].count()
    total_download_size_bytes = df[df["method"] == "GET"]["objectsize"].sum()
    total_upload_size_bytes = df[df["method"] == "PUT"]["objectsize"].sum()
    timeframe_start = df["requestdatetime"].min().strftime("%B %d, %Y")
    timeframe_end = df["requestdatetime"].max().strftime("%B %d, %Y")

    def format_size(size_bytes):
        if size_bytes < 1024:
            return "less than 1 MB"
        elif size_bytes < 1024**2:
            return f"{size_bytes / 1024:.0f} MB"
        else:
            return f"{size_bytes / (1024**3):.0f} GB"

    email_body = f"""
Dear Stakeholder,

Please find below the comprehensive S3 Logs Summary Report covering the period from {timeframe_start} to {timeframe_end}.

Overall Summary for the Service:
- Total Downloads: {total_downloads}
- Total Uploads/Updates: {total_uploads}
- Total Download transferred: {format_size(total_download_size_bytes)}
- Total Upload/Update transferred: {format_size(total_upload_size_bytes)}

Detailed Folder Statistics:

Folder explanation : 
TM - Tasking Manager exports 
default - Default exports generated usually from export tool / FMTM and fAIr general call
ISO3 - Country exports currently pushed to HDX 

"""

    # folder specific
    for folder in df["top_level_key"].unique():
        if folder.startswith("log"):
            continue

        folder_df = df[df["top_level_key"] == folder]
        downloads = folder_df[folder_df["method"] == "GET"]["objectsize"].count()
        uploads = folder_df[folder_df["method"] == "PUT"]["objectsize"].count()
        download_size_bytes = folder_df[folder_df["method"] == "GET"][
            "objectsize"
        ].sum()
        upload_size_bytes = folder_df[folder_df["method"] == "PUT"]["objectsize"].sum()

        popular_files = folder_df["key"].value_counts().head(5)
        email_body += f"""
Folder: {folder}
- Total Downloads: {downloads}
- Total Uploads/Updates: {uploads}
- Total Download transferred: {format_size(download_size_bytes)}
- Total Upload/Update transferred: {format_size(upload_size_bytes)}
Most Popular Files:
"""
        for file, count in popular_files.items():
            email_body += f"   - {file}: {count} times\n"

    return email_body.strip()


def send_email(subject, body, recipient_list):
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = os.getenv("EMAIL_USER")
    msg["To"] = recipient_list

    server = smtplib.SMTP(os.getenv("SMTP_HOST"), os.getenv("SMTP_PORT"))
    server.starttls()
    server.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASSWORD"))
    server.send_message(msg)
    server.quit()


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
        email_body = generate_full_report_email(df)
        email_body += f"\n \nDownload full report meta csv for your custom analysis from attached link. Note : This link expires in 1 week so if you lost it, Please contact administrator ({presigned_url_csv})"
        print(email_body)
        target_emails = os.getenv("TARGET_EMAIL_ADDRESS").split(",")
        send_email(
            f"Your {database.upper()} Usage Stats Report",
            email_body,
            target_emails,
        )


if __name__ == "__main__":
    main()
