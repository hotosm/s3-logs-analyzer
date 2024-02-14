# DuckDB Queries for S3 Access Logs Analysis

## Prerequisites 
1. Install duckdb in your system 
2. Download your remote result parquet 

## Create table 

```sql
Create table logs from ...result.parquet;
```

## Shoot your queries 

```sql
select * from logs;
```


