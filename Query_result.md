# DuckDB Queries for S3 Access Logs Analysis

## Prerequisites 
1. Install duckdb in your system 
2. Download your remote result parquet 

## Create table 

```sql
CREATE TABLE logs AS SELECT * FROM 'results.parquet';
```

## Shoot your queries 

```sql
select * from logs;
```


### Example : 

- Strip timestamp 

```sql
SELECT 
    STRPTIME(requestdatetime, '%d/%b/%Y:%H:%M:%S %z') AS  requestdatetime
FROM 
    logs;
```
