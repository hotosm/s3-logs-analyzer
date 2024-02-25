# -*- coding: utf-8 -*-

"""
This module allow user to use parquet file to store Athena query result.
It returns the athena query result as a
`polars.LazyFrame <https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html>`_.

Requirements:

    polars
    s3fs
    pyarrow

You can run ``pip install polars s3fs pyarrow`` to install them.

Usage::

    from aws_athena_query import (
        run_athena_query,
        read_athena_query_result,
        wait_athena_query_to_succeed,
    )
"""

import os
import textwrap
import time
import typing as T
import uuid

import polars as pl
import pyarrow.dataset
import s3fs

if T.TYPE_CHECKING:
    from boto_session_manager import BotoSesManager
    from s3pathlib import S3Path


def wait_athena_query_to_succeed(
    bsm: "BotoSesManager",
    exec_id: str,
    delta: int = 1,
    timeout: int = 30,
):
    """
    Wait a given athena query to reach ``SUCCEEDED`` status. If failed, raise
    ``RuntimeError`` immediately. If timeout, raise ``TimeoutError``.

    """
    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/get_query_execution.html
    elapsed = 0
    for _ in range(999999):
        res = bsm.athena_client.get_query_execution(
            QueryExecutionId=exec_id,
        )
        status = res["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            return
        elif status in ["FAILED", "CANCELLED"]:
            raise RuntimeError(f"execution {exec_id} reached status: {status}")
        else:
            time.sleep(delta)
        elapsed += delta
        if elapsed > timeout:
            raise TimeoutError(f"athena query timeout in {timeout} seconds!")


def _get_dataset_and_metadata_s3path(
    s3dir_result: "S3Path",
) -> T.Tuple["S3Path", "S3Path"]:
    # the dataset folder uri will be used in the UNLOAD command
    # the final parquet files will be stored in this folder
    # note that this folder has to be NOT EXISTING before the execution
    s3dir_dataset = s3dir_result.joinpath("dataset", uuid.uuid4().hex).to_dir()

    # the metadata folder will be used to store the query result metadata file
    # the metadata file will tell you the list of data file uris
    s3dir_metadata = s3dir_result.joinpath("metadata").to_dir()
    return s3dir_dataset, s3dir_metadata


def _delete_s3_objects(bsm: "BotoSesManager", s3dir: "S3Path", verbose: bool = True):
    """
    Delete all objects within a specified s3 directory.
    """
    bucket_name = s3dir.bucket
    prefix = s3dir.key
    if verbose:
        print("Deleting {s3dir}")
    s3 = bsm.boto_ses.client("s3")  # Using BotoSesManager to obtain boto3 client
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    delete_us = dict(Objects=[])
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                delete_us["Objects"].append(dict(Key=obj["Key"]))

            s3.delete_objects(Bucket=bucket_name, Delete=delete_us)
            delete_us = dict(Objects=[])


def read_athena_query_result(
    bsm: "BotoSesManager",
    s3dir_result: "S3Path",
    exec_id: str,
    verbose: bool = True,
) -> pl.LazyFrame:
    """
    Load the athena query result from s3. The query has to be succeeded already.

    :param bsm: boto_session_manager.BotoSesManager object.
    :param s3dir_result: an S3Path object that represent a s3 directory to store
        the athena query result.
    :param exec_id: athena query execution id, you can get it from the
        :func:`run_athena_query` function.
    :param verbose: do you want to print the log?

    :return: the lazy DataFrame of the result, If you just need to return the
        regular DataFrame, you can do ``df = read_athena_query_result(...).collect()``.

    """
    s3dir_dataset, s3dir_metadata = _get_dataset_and_metadata_s3path(s3dir_result)
    # read the manifest file to get list of parquet file uris
    # ref: https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.scan_pyarrow_dataset.html
    s3path_manifest = s3dir_metadata.joinpath(f"{exec_id}-manifest.csv")
    s3uri_list = s3path_manifest.read_text(bsm=bsm).splitlines()

    if verbose:
        query_editor_console_url = (
            f"https://{bsm.aws_region}.console.aws.amazon.com/athena"
            f"/home?region={bsm.aws_region}#/query-editor/history/{exec_id}"
        )
        print(f"preview query in athena editor: {query_editor_console_url}\n")
        print(f"query result manifest: {s3path_manifest.console_url}\n")
        print(f"query result data: {s3dir_dataset.console_url}\n")
        print(f"number of files in result: {len(s3uri_list)}")

    if isinstance(bsm.profile_name, str):
        file_system = s3fs.S3FileSystem(profile=bsm.profile_name)
    else:
        credential = bsm.boto_ses.get_credentials()
        file_system = s3fs.S3FileSystem(
            key=credential.access_key,
            secret=credential.secret_key,
            token=credential.token,
        )
    dataset = pyarrow.dataset.dataset(s3uri_list, filesystem=file_system)
    lazy_df = pl.scan_pyarrow_dataset(dataset)
    df = lazy_df.select(pl.col("*"))
    return df


def run_athena_query(
    bsm: "BotoSesManager",
    s3dir_result: "S3Path",
    sql: str,
    database: str,
    catalog: T.Optional[str] = None,
    compression: str = "gzip",
    encryption_configuration: T.Optional[dict] = None,
    expected_bucket_owner: T.Optional[str] = None,
    acl_configuration: T.Optional[dict] = None,
    workgroup: T.Optional[str] = None,
    execution_parameters: T.Optional[T.List[T.Any]] = None,
    result_cache_expire: T.Optional[int] = None,
    client_request_token: T.Optional[str] = None,
    delta: int = 1,
    timeout: int = 10,
    verbose: bool = True,
) -> T.Tuple[pl.LazyFrame, str]:
    """
    Run athena query and get the result as a polars.LazyFrame.
    With LazyFrame, you can do further select, filter actions before actually
    reading the data, and leverage the parquet predicate pushdown feature to
    reduce the amount of data to be read. If you just need to return the
    regular DataFrame, you can do ``df = run_athena_query(...)[0].collect()``.

    Example::

        >>> from boto_session_manager import BotoSesManager
        >>> from s3pathlib import S3Path
        >>>
        >>> bsm = BotoSesManager(profile_name="your_aws_profile")
        >>> bucket = f"{bsm.aws_account_id}-{bsm.aws_region}-data"
        >>> prefix = f"athena/results/"
        >>> s3dir_result = S3Path(f"s3://{bucket}/{prefix}").to_dir()

        >>> database = "your_database"
        >>> sql = f"SELECT * FROM {database}.your_table LIMIT 10;"
        >>> lazy_df, exec_id = run_athena_query(
        ...     bsm=bsm,
        ...     s3dir_result=s3dir_result,
        ...     sql=sql,
        ...     database=database,
        ... )
        >>> df = lazy_df.collect()
        >>> df
        ...

    start_query_execution doc: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/start_query_execution.html

    :param bsm: boto_session_manager.BotoSesManager object.
    :param s3dir_result: an S3Path object that represent a s3 directory to store
        the athena query result.
    :param sql: sql query string.
    :param database: database name.
    :param catalog: see start_query_execution doc
    :param compression: compression algorithm to use when writing parquet file,
        see all options here: https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html
    :param compression: see start_query_execution doc
    :param encryption_configuration: see start_query_execution doc
    :param expected_bucket_owner: see start_query_execution doc
    :param acl_configuration: see start_query_execution doc
    :param workgroup: see start_query_execution doc
    :param execution_parameters: see start_query_execution doc
    :param result_cache_expire: cache query result for X minutes, if None,
        it means no cache, see more information here: https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html
        this is very helpful to reduce the cost.
    :param client_request_token: see start_query_execution doc
    :param delta: sleep time in seconds between each query status check.
    :param timeout: timeout in seconds.
    :param verbose: do you want to print the log?

    :return: the tuple of two item, the first item is the lazy DataFrame of the result,
        the second item is the athena query execution id (str)>

    .. versionadded:: 0.11.1
    """
    # the sql query should not end with ;, it will be embedded in the final query
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1]

    s3dir_dataset, s3dir_metadata = _get_dataset_and_metadata_s3path(s3dir_result)

    # ref: https://docs.aws.amazon.com/athena/latest/ug/unload.html
    # use UNLOAD command to write result into data format other than csv
    final_sql = textwrap.dedent(
        f"""
        UNLOAD ({sql})
        TO '{s3dir_dataset.uri}'
        WITH ( format = 'parquet', compression= '{compression}')
        """
    )

    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena/client/start_query_execution.html
    # like cross database query, federated query
    query_execution_context = dict(Database=database)
    if catalog:
        query_execution_context["Catalog"] = catalog
    result_configuration = dict(OutputLocation=s3dir_metadata.uri)
    if encryption_configuration:
        result_configuration["EncryptionConfiguration"] = encryption_configuration
    if expected_bucket_owner:
        result_configuration["ExpectedBucketOwner"] = expected_bucket_owner
    if acl_configuration:
        result_configuration["AclConfiguration"] = acl_configuration
    kwargs = dict(
        QueryString=final_sql,
        QueryExecutionContext=query_execution_context,
        ResultConfiguration=dict(
            OutputLocation=s3dir_metadata.uri,
        ),
    )
    if workgroup:
        kwargs["WorkGroup"] = workgroup
    if execution_parameters:
        kwargs["Parameters"] = execution_parameters
    if result_cache_expire:
        kwargs["ResultReuseConfiguration"] = dict(
            ResultReuseByAgeConfiguration=dict(
                Enabled=True,
                MaxAgeInMinutes=result_cache_expire,
            )
        )
    if client_request_token:
        kwargs["ClientRequestToken"] = client_request_token
    res = bsm.athena_client.start_query_execution(**kwargs)

    # the start_query_execution API is async, it returns the execution id
    exec_id = res["QueryExecutionId"]

    # wait for the execution to finish
    wait_athena_query_to_succeed(bsm=bsm, exec_id=exec_id, delta=delta, timeout=timeout)
    lazy_df = read_athena_query_result(
        bsm=bsm,
        s3dir_result=s3dir_result,
        exec_id=exec_id,
        verbose=verbose,
    )

    return lazy_df, exec_id


if __name__ == "__main__":
    import random
    import textwrap

    from boto_session_manager import BotoSesManager
    from s3pathlib import S3Path

    bsm = BotoSesManager()

    bucket = os.getenv("S3_LOGS_LOCATION")
    prefix = f"athena/results/"
    result_path = f"{os.getenv('RESULT_PATH')}/{prefix}"
    s3dir_result = S3Path(result_path).to_dir()

    database = os.getenv("ATHENA_DATABASE")
    table = os.getenv("ATHENA_TABLE")

    n = random.randint(100, 500)
    sql = textwrap.dedent(
        f"""
        SELECT * 
        FROM "{database}"."{table}"
        LIMIT {n};
    """
    )

    lazy_df, exec_id = run_athena_query(
        bsm=bsm,
        s3dir_result=s3dir_result,
        sql=sql,
        database=database,
    )
    df = lazy_df.collect()
    print(df.shape)
    print(df)

    print(f"Deleting Query meta: {s3dir_result}")
    _delete_s3_objects(bsm, s3dir_result)

    assert df.shape[0] == n
