import textwrap


def generate_athena_create_database_query(ATHENA_DATABASE):
    sql = textwrap.dedent(f"""CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE};""")
    return sql


def generate_athena_create_table_query(ATHENA_DATABASE, ATHENA_TABLE, S3_LOGS_LOCATION):
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


def generate_athena_fetch_query(
    ATHENA_DATABASE, ATHENA_TABLE, START_DATE, END_DATE, SELECT_ALL=False, verbose=True
):
    select = "*"
    if not SELECT_ALL:
        select = f"requestid, operation, SPLIT_PART(key, '/', 1) AS dir, SPLIT_PART(key, '/', 2) AS folder, SPLIT_PART(key, '/', 3) AS category, SPLIT_PART(key, '/', 4) AS geom_type, key, referrer, objectsize, httpstatus, requestdatetime, timestamp, remoteip"

    sql = textwrap.dedent(
        f"""
        SELECT {select} 
        FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
        WHERE key != '-' and (timestamp BETWEEN '{START_DATE}' AND '{END_DATE}');
        """
    )
    print(sql)
    return sql
