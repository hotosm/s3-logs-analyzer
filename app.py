# app.py
import argparse
import datetime
import gzip
import logging
import os

import pyarrow.parquet as pq
import s3fs
from boto_session_manager import BotoSesManager
from pyarrow import csv
from s3pathlib import S3Path

from aws_athena_query import _delete_s3_objects, run_athena_query
from email_results import send_email
from query import generate_athena_fetch_query
from utils import (
    analyze_metrics_by_day,
    calculate_date_ranges,
    check_env_vars,
    generate_full_report_email,
    insert_to_postgres,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def upload_df_to_s3_in_formats(
    df,
    start_date,
    end_date,
    s3_base_dir: S3Path,
    bsm: "BotoSesManager",
    verbose=True,
):
    now = datetime.datetime.now()
    year = str(now.year)

    base_path = s3_base_dir.joinpath(str(year))
    file_name = f"{start_date.replace('/','_')}-{end_date.replace('/','_')}"
    parquet_file_path = base_path.joinpath(f"{file_name}.parquet")
    csv_file_path = base_path.joinpath(f"{file_name}.csv.gz")

    if hasattr(bsm, "profile_name") and isinstance(bsm.profile_name, str):
        file_system = s3fs.S3FileSystem(profile=bsm.profile_name)
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
    if verbose:
        print(f"Uploaded files to {parquet_file_path.uri} and {csv_file_path.uri}")
    return presigned_url_csv, file_name


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
    parser.add_argument(
        "--verbose", action="store_true", help="Display additional information"
    )

    parser.add_argument(
        "--out",
        choices=["s3", "postgres"],
        default="s3",
        help="Where do you want to store processed data ?",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--frequency",
        choices=["daily", "weekly", "monthly", "quarterly"],
        default="monthly",
        help="Frequency of data extraction (weekly, monthly, quarterly). Default is monthly.",
    )
    group.add_argument(
        "--date_range",
        type=str,
        nargs=2,
        metavar=("START", "END"),
        help="Custom date range in YYYY-MM-DD format. Specify as: START END.",
    )

    args = parser.parse_args()

    check_env_vars(args.email)

    if args.date_range:
        start_date, end_date = calculate_date_ranges(None, *args.date_range)
    else:
        start_date, end_date = calculate_date_ranges(args.frequency)

    if os.getenv("AWS_PROFILE_NAME", None):
        bsm = BotoSesManager(profile_name=os.getenv("AWS_PROFILE_NAME"))
    else:
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
    logger.info("Running Athena Query")
    lazy_df, exec_id = run_athena_query(
        bsm=bsm,
        s3dir_result=s3dir_result_meta,
        sql=generate_athena_fetch_query(
            database, table, start_date, end_date, args.select_all, args.verbose
        ),
        database=database,
        verbose=args.verbose,
    )
    logger.info("Processing Query Results")

    df = lazy_df.collect()
    if args.verbose:
        print(df.shape)
        print(df)
    presigned_url_csv, s3dir_result, filename = None, None, "Report"
    if args.out == "s3":
        result_path = f"{os.getenv('RESULT_PATH')}/{prefix}/"
        s3dir_result = S3Path(result_path).to_dir()
        presigned_url_csv, filename = upload_df_to_s3_in_formats(
            df=df.to_arrow(),
            s3_base_dir=s3dir_result,
            bsm=bsm,
            verbose=args.verbose,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Uploaded results in S3")

    if args.out == "postgres":

        daily_metrics = analyze_metrics_by_day(df.to_pandas())

        REMOTE_DB = os.getenv(
            "REMOTE_DB", "postgresql://postgres:postgres@localhost:5432/postgres"
        )
        insert_to_postgres(daily_metrics, "metrics", REMOTE_DB)

    email_body = generate_full_report_email(
        df,
        presigned_url_csv,
        args.verbose,
        filename,
        bsm=bsm,
        start_date=start_date,
        end_date=end_date,
        s3dir_result=s3dir_result,
    )
    if args.remove_meta:
        logger.info("Cleaning up metadata from S3")

        _delete_s3_objects(bsm, s3dir_result_meta)

    if args.remove_original_logs:  # Use with caution
        _delete_s3_objects(bsm, S3Path(os.getenv("S3_LOGS_LOCATION")).to_dir())
    if args.verbose:
        with open("email_response.html", "w", encoding="utf-8") as file:
            file.write(email_body)
        print("Email content written to email_response.html")
    if args.email:
        target_emails = os.getenv("TARGET_EMAIL_ADDRESS").split(",")
        send_email(
            subject=f"[INTERNAL] Your {database.upper()} Usage Stats Report",
            content=email_body,
            to_emails=target_emails,
            content_type="html",
        )


if __name__ == "__main__":
    main()
