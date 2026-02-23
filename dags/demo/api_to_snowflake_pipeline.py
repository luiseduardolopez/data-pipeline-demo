"""
API to Snowflake ETL Pipeline DAG.

This DAG implements a production-ready ETL pipeline that:
1. Extracts data from API with retries and error handling
2. Saves raw data to S3 with date partitioning (Hive-style)
3. Transforms and saves curated data to S3 with partitioning
4. Loads to Snowflake with idempotency (UPSERT/MERGE)
5. Supports incremental processing via Airflow params
6. Includes comprehensive data quality checks
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import pendulum
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import pandas as pd
import numpy as np

LOG = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://45e984f2-4d86-4067-804a-e96dc24789ed.mock.pstmn.io"
DEFAULT_API_ENDPOINT = "api/v1/orders"
IDEMPOTENCY_KEY = "order_id"


@dag(
    dag_id="api_to_snowflake_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api", "etl", "snowflake", "s3", "production"],
    description="Production ETL: API → S3 (raw/curated) → Snowflake with idempotency",
    params={
        "api_base_url": Param(
            default=DEFAULT_API_BASE_URL,
            type="string",
            description="Base URL for the API"
        ),
        "api_endpoint": Param(
            default=DEFAULT_API_ENDPOINT,
            type="string",
            description="API endpoint to call"
        ),
        "since_timestamp": Param(
            default=None,
            type=["null", "string"],
            description="ISO timestamp for incremental processing (e.g., 2024-01-01T00:00:00)"
        ),
        "use_last_processed": Param(
            default=False,
            type="boolean",
            description="Use last successful run timestamp for incremental processing"
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
            description="If true, don't write to S3 or Snowflake"
        ),
        "snowflake_table": Param(
            default="orders",
            type="string",
            description="Target Snowflake table name"
        ),
        "reset_state": Param(
            default=False,
            type="boolean",
            description="Reset processing state to reprocess all records"
        ),
    },
)
def api_to_snowflake_pipeline():
    """
    Production ETL pipeline from API to Snowflake via S3.
    
    Features:
    - Idempotent: Running twice won't duplicate records in Snowflake
    - Incremental: Can process only new records since last run
    - Partitioned: Data stored in S3 with Hive-style partitioning (year=/month=/day=)
    - Resilient: Retries on API failures with exponential backoff
    - Observable: Comprehensive logging and data quality metrics
    """
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    @task(task_id="get_processing_state")
    def get_processing_state(**context) -> Dict[str, Any]:
        """
        Get processing state for incremental/idempotent processing.
        
        Reads from Airflow Variables to track:
        - last_processed_timestamp: For incremental processing
        - processed_ids: Set of already processed record IDs
        
        Returns:
            Dictionary with processing state
        """
        from airflow.models import Variable
        
        params = context["params"]
        
        state_var_name = "api_pipeline_state"
        try:
            state = Variable.get(state_var_name, deserialize_json=True)
        except Exception:
            state = {
                "last_processed_timestamp": None,
                "processed_ids": [],
                "run_count": 0
            }
        
        since_timestamp = params.get("since_timestamp")
        use_last_processed = params.get("use_last_processed", False)
        reset_state = params.get("reset_state", False)
        
        if reset_state:
            state = {
                "last_processed_timestamp": None,
                "processed_ids": [],
                "run_count": 0
            }
            LOG.info("Reset processing state - will reprocess all records")
        
        if use_last_processed and not since_timestamp:
            since_timestamp = state.get("last_processed_timestamp")
            if since_timestamp:
                LOG.info(f"Using last processed timestamp: {since_timestamp}")
        
        processing_state = {
            "state": state,
            "since_timestamp": since_timestamp,
            "dry_run": params.get("dry_run", False),
            "api_base_url": params.get("api_base_url", DEFAULT_API_BASE_URL),
            "api_endpoint": params.get("api_endpoint", DEFAULT_API_ENDPOINT),
            "snowflake_table": params.get("snowflake_table", "orders"),
            "reset_state": reset_state,
        }
        
        LOG.info(f"Processing state: {processing_state}")
        return processing_state
    
    @task(task_id="extract_from_api")
    def extract_from_api(processing_state: Dict[str, Any], **context) -> Dict[str, Any]:
        """
        Extract data from API with retry logic and error handling.
        
        Args:
            processing_state: State information including API URL and since timestamp
            
        Returns:
            Dictionary with extracted records and metadata
        """
        import requests
        
        api_base_url = processing_state["api_base_url"]
        api_endpoint = processing_state["api_endpoint"]
        since_timestamp = processing_state.get("since_timestamp")
        
        url = f"{api_base_url}/{api_endpoint}"
        
        LOG.info(f"Extracting from API: {url}")
        if since_timestamp:
            LOG.info(f"Incremental: records after {since_timestamp}")
        
        @retry(
            retry=retry_if_exception_type((requests.RequestException, Exception)),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=4, max=16),
            reraise=True
        )
        def _fetch_data():
            headers = {"Accept": "application/json"}
            params = {}
            if since_timestamp:
                params["since"] = since_timestamp
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        
        try:
            data = _fetch_data()
            
            if isinstance(data, dict):
                for key in ["data", "results", "items", "records", "orders"]:
                    if key in data:
                        records = data[key]
                        break
                else:
                    records = [data]
            elif isinstance(data, list):
                records = data
            else:
                records = []
            
            LOG.info(f"Extracted {len(records)} records from API")
            
            return {
                "records": records,
                "count": len(records),
                "api_url": url,
                "since_timestamp": since_timestamp,
            }
            
        except Exception as e:
            LOG.error(f"Failed to extract from API after retries: {e}")
            raise
    
    @task(task_id="filter_and_deduplicate")
    def filter_and_deduplicate(extraction_result: Dict[str, Any], processing_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter records for idempotency and incremental processing.
        
        Args:
            extraction_result: Result from API extraction
            processing_state: State with processed IDs and timestamp
            
        Returns:
            Filtered records for processing
        """
        records = extraction_result["records"]
        since_timestamp = extraction_result.get("since_timestamp")
        state = processing_state.get("state", {})
        processed_ids = set(state.get("processed_ids", []))
        
        new_records = []
        new_ids = []
        
        for record in records:
            record_id = str(record.get(IDEMPOTENCY_KEY, ""))
            
            if record_id in processed_ids:
                continue
            
            if since_timestamp and "created_at" in record:
                try:
                    record_time = datetime.fromisoformat(record["created_at"].replace("Z", "+00:00"))
                    since_time = datetime.fromisoformat(since_timestamp.replace("Z", "+00:00"))
                    if record_time <= since_time:
                        continue
                except Exception:
                    pass
            
            new_records.append(record)
            new_ids.append(record_id)
        
        LOG.info(f"Filtered {len(new_records)} new records from {len(records)} total")
        LOG.info(f"Skipped {len(records) - len(new_records)} already processed records")
        
        return {
            "records": new_records,
            "new_ids": new_ids,
            "total_count": len(records),
            "new_count": len(new_records),
            "skipped_count": len(records) - len(new_records),
        }
    
    @task(task_id="save_to_raw_s3")
    def save_to_raw_s3(filter_result: Dict[str, Any], processing_state: Dict[str, Any], **context) -> str:
        """
        Save raw data to S3 with Hive-style partitioning.
        
        Partition structure: s3://bucket/raw/year=YYYY/month=MM/day=DD/
        
        Strategy: Overwrite daily file with new data (no deduplication)
        
        Args:
            filter_result: Filtered records to save
            processing_state: Processing state
            
        Returns:
            S3 path where data was saved
        """
        if processing_state.get("dry_run"):
            LOG.info("[DRY RUN] Skipping S3 raw save")
            return "dry_run"
        
        records = filter_result["records"]
        if not records:
            LOG.info("No records to save to S3 raw")
            return "no_records"
        
        partition_date = datetime.now()
        
        year, month, day = partition_date.year, partition_date.month, partition_date.day
        
        s3_key = f"raw/year={year}/month={month:02d}/day={day:02d}/orders_daily.json"
        
        json_data = json.dumps(records, indent=2, default=str)
        
        s3_hook = S3Hook(aws_conn_id="aws_default")
        bucket_name = "data-pipeline-luis-demo-bucket"
        
        s3_hook.load_string(
            string_data=json_data,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True,
        )
        
        s3_path = f"s3://{bucket_name}/{s3_key}"
        LOG.info(f"Overwrote S3 raw file with {len(records)} records: {s3_path}")
        
        return s3_path
    
    @task(task_id="transform_to_curated")
    def transform_to_curated(filter_result: Dict[str, Any], s3_raw_path: str, **context) -> Dict[str, Any]:
        """
        Transform raw data to curated format.
        Transformations:
        - Standardize column names
        - Parse dates
        - Add partition columns (year, month, day)
        - Add ETL metadata
        - Remove duplicates
        
        Args:
            filter_result: Filtered records
            s3_raw_path: Path to raw data in S3
            
        Returns:
            Dictionary with transformed DataFrame and metadata
        """
        records = filter_result["records"]
        if not records:
            LOG.info("No records to transform")
            return {"records": [], "df": None, "count": 0}
        
        df = pd.DataFrame(records)
        
        LOG.info(f"Transforming {len(df)} records")
        
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        
        df["_etl_processed_at"] = datetime.now().isoformat()
        df["_etl_batch_id"] = datetime.now().strftime("%Y%m%d_%H%M%S")
        df["_etl_source"] = "api"
        
        date_cols = ["created_at", "updated_at", "order_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        if "created_at" in df.columns:
            df["year"] = df["created_at"].dt.year
            df["month"] = df["created_at"].dt.month
            df["day"] = df["created_at"].dt.day
        else:
            now = datetime.now()
            df["year"] = now.year
            df["month"] = now.month
            df["day"] = now.day
        
        if IDEMPOTENCY_KEY in df.columns:
            before_count = len(df)
            df = df.drop_duplicates(subset=[IDEMPOTENCY_KEY], keep="last")
            after_count = len(df)
            if before_count != after_count:
                LOG.info(f"Removed {before_count - after_count} duplicates")
        
        LOG.info(f"Transformation complete: {len(df)} records")
        
        records_json = df.to_json(orient="records", date_format="iso")
        records_clean = json.loads(records_json)
        
        return {
            "records": records_clean,
            "count": len(df),
            "columns": list(df.columns),
        }
    
    @task(task_id="save_to_curated_s3")
    def save_to_curated_s3(transform_result: Dict[str, Any], processing_state: Dict[str, Any], **context) -> str:
        """
        Save curated data to S3 with Hive-style partitioning.
        
        Partition structure: s3://bucket/curated/year=YYYY/month=MM/day=DD/
        
        Strategy: Overwrite daily file with new data (no deduplication)
        
        Args:
            transform_result: Transformed data
            processing_state: Processing state
            
        Returns:
            S3 path where data was saved
        """
        if processing_state.get("dry_run"):
            LOG.info("[DRY RUN] Skipping S3 curated save")
            return "dry_run"
        
        if not transform_result["records"]:
            LOG.info("No records to save to S3 curated")
            return "no_records"
        
        df = pd.DataFrame(transform_result["records"])
        
        now = datetime.now()
        year, month, day = now.year, now.month, now.day
        
        s3_key = f"curated/year={year}/month={month:02d}/day={day:02d}/orders_daily.parquet"
        
        parquet_buffer = df.to_parquet(index=False)
        
        s3_hook = S3Hook(aws_conn_id="aws_default")
        bucket_name = "data-pipeline-luis-demo-bucket"
        
        s3_hook.load_bytes(
            bytes_data=parquet_buffer,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True,
        )
        
        s3_path = f"s3://{bucket_name}/{s3_key}"
        LOG.info(f"Overwrote S3 curated file with {len(df)} records: {s3_path}")
        
        return s3_path
    
    @task(task_id="load_to_snowflake")
    def load_to_snowflake(transform_result: Dict[str, Any], s3_curated_path: str, processing_state: Dict[str, Any], **context) -> bool:
        """
        Load data to Snowflake with idempotency using MERGE/UPSERT.
        
        Technique for idempotency:
        - Create staging table with new data
        - Use MERGE statement to insert only new records or update existing
        - Based on idempotency key (order_id)
        
        Args:
            transform_result: Transformed data
            s3_curated_path: Path to curated data in S3
            processing_state: Processing state
            
        Returns:
            True if successful
        """
        if processing_state.get("dry_run"):
            LOG.info("[DRY RUN] Skipping Snowflake load")
            return True
        
        if not transform_result["records"]:
            LOG.info("No records to load to Snowflake")
            return True
        
        table_name = processing_state["snowflake_table"]
        df = pd.DataFrame(transform_result["records"])
        
        date_cols = ["created_at", "updated_at", "order_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        LOG.info(f"Loading {len(df)} records to Snowflake table: {table_name}")
        
        import snowflake.connector
        
        sf_user = os.getenv("SNOWFLAKE_USER")
        sf_password = os.getenv("SNOWFLAKE_PASSWORD")
        sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
        sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        database = os.getenv("SNOWFLAKE_DATABASE", "DATA_PIPELINE_DB")
        schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA")
        
        if not all([sf_user, sf_password, sf_account, sf_warehouse]):
            raise ValueError("Missing Snowflake credentials in environment variables")
        
        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=database,
            schema=schema
        )
        cursor = conn.cursor()
        
        try:
            columns_with_types = []
            for col in df.columns:
                if df[col].dtype == "object":
                    columns_with_types.append(f'"{col}" VARCHAR')
                elif df[col].dtype in ["int64", "int32"]:
                    columns_with_types.append(f'"{col}" INTEGER')
                elif df[col].dtype in ["float64", "float32"]:
                    columns_with_types.append(f'"{col}" FLOAT')
                elif "datetime" in str(df[col].dtype):
                    columns_with_types.append(f'"{col}" TIMESTAMP')
                else:
                    columns_with_types.append(f'"{col}" VARCHAR')
            
            LOG.info(f"DataFrame columns: {list(df.columns)}")
            LOG.info(f"Looking for idempotency key: '{IDEMPOTENCY_KEY}'")
            
            primary_key = IDEMPOTENCY_KEY
            if IDEMPOTENCY_KEY not in df.columns:
                available_cols = ", ".join(df.columns.tolist())
                LOG.warning(f"Idempotency key '{IDEMPOTENCY_KEY}' not found in DataFrame. Available columns: {available_cols}")
                primary_key = df.columns[0]
                LOG.info(f"Using '{primary_key}' as primary key instead")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns_with_types)},
                PRIMARY KEY ("{primary_key}")
            )
            """
            cursor.execute(create_table_sql)
            
            staging_table = f"{table_name}_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            create_staging_sql = f"""
            CREATE TEMPORARY TABLE {staging_table} AS
            SELECT * FROM {table_name} WHERE 1=0
            """
            cursor.execute(create_staging_sql)
            
            df = df.replace({np.nan: None, pd.NaT: None, 'NaN': None, 'nan': None})
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: None if x is None or (isinstance(x, str) and (x == 'NaN' or x == 'nan' or x == '')) else x)
            
            for col in df.columns:
                if "datetime" in str(df[col].dtype):
                    df[col] = df[col].fillna(pd.Timestamp.now()).astype(str)
                elif df[col].dtype == "object":
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x) if x is not None else None
                    )
                elif df[col].dtype in ["float64", "float32"]:
                    if col in ["year", "month", "day"]:
                        df[col] = df[col].fillna(0).astype(int)
                    else:
                        df[col] = df[col].where(pd.notna(df[col]), None).astype(int)
                else:
                    df[col] = df[col].fillna(None)
            
            df = df.replace({'NaN': None, 'nan': None, '': None})
            
            LOG.info(f"Sample data after cleaning: {df.head(1).to_dict() if len(df) > 0 else 'No data'}")
            
            values = [tuple(row) for row in df.values]
            columns = [f'"{col}"' for col in df.columns]
            
            insert_sql = f"""
            INSERT INTO {staging_table} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(df.columns))})
            """
            cursor.executemany(insert_sql, values)
            
            merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING {staging_table} AS source
            ON target."{primary_key}" = source."{primary_key}"
            WHEN MATCHED THEN UPDATE SET
                {', '.join([f'target."{col}" = source."{col}"' for col in df.columns if col != primary_key])}
            WHEN NOT MATCHED THEN INSERT
                ({', '.join([f'"{col}"' for col in df.columns])})
                VALUES ({', '.join([f'source."{col}"' for col in df.columns])})
            """
            cursor.execute(merge_sql)
            
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            
            conn.commit()
            
            LOG.info(f"Successfully loaded {len(df)} records to Snowflake (MERGE for idempotency)")
            return True
            
        except Exception as e:
            conn.rollback()
            LOG.error(f"Failed to load to Snowflake: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    @task(task_id="update_processing_state")
    def update_processing_state(
        filter_result: Dict[str, Any],
        snowflake_success: bool,
        processing_state: Dict[str, Any],
        **context
    ) -> bool:
        """
        Update processing state after successful run.
        
        Args:
            filter_result: Filter result with new IDs
            snowflake_success: Whether Snowflake load succeeded
            processing_state: Previous state
            
        Returns:
            True if state was updated
        """
        if not snowflake_success:
            LOG.warning("Snowflake load failed, not updating processing state")
            return False
        
        if processing_state.get("dry_run"):
            LOG.info("[DRY RUN] Skipping state update")
            return True
        
        from airflow.models import Variable
        
        state_var_name = "api_pipeline_state"
        
        try:
            current_state = Variable.get(state_var_name, deserialize_json=True)
        except Exception:
            current_state = {
                "last_processed_timestamp": None,
                "processed_ids": [],
                "run_count": 0
            }
        
        current_state["last_processed_timestamp"] = datetime.now().isoformat()
        current_state["processed_ids"] = list(set(
            current_state.get("processed_ids", []) + filter_result.get("new_ids", [])
        ))
        current_state["run_count"] = current_state.get("run_count", 0) + 1
        
        Variable.set(state_var_name, current_state, serialize_json=True)
        
        LOG.info(f"Updated processing state: {len(current_state['processed_ids'])} total IDs, {current_state['run_count']} runs")
        
        return True
    
    @task(task_id="data_quality_check")
    def data_quality_check(transform_result: Dict[str, Any], **context) -> Dict[str, Any]:
        """
        Perform data quality checks on transformed data.
        
        Args:
            transform_result: Transformed data
            
        Returns:
            Quality check results
        """
        if not transform_result["records"]:
            return {"status": "no_data", "checks": {}}
        
        df = pd.DataFrame(transform_result["records"])
        
        checks = {
            "total_records": len(df),
            "null_checks": {},
            "uniqueness_checks": {},
            "type_checks": {},
            "range_checks": {},
        }
        
        critical_cols = [IDEMPOTENCY_KEY, "created_at"]
        for col in critical_cols:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                checks["null_checks"][col] = {
                    "null_count": int(null_count),
                    "null_pct": float((null_count / len(df)) * 100),
                    "passed": null_count == 0
                }
        
        if IDEMPOTENCY_KEY in df.columns:
            dup_count = df[IDEMPOTENCY_KEY].duplicated().sum()
            checks["uniqueness_checks"][IDEMPOTENCY_KEY] = {
                "duplicate_count": int(dup_count),
                "passed": dup_count == 0
            }
        
        if "amount" in df.columns:
            checks["type_checks"]["amount"] = {
                "type": str(df["amount"].dtype),
                "is_numeric": pd.api.types.is_numeric_dtype(df["amount"])
            }
        
        if "amount" in df.columns and pd.api.types.is_numeric_dtype(df["amount"]):
            checks["range_checks"]["amount"] = {
                "min": float(df["amount"].min()),
                "max": float(df["amount"].max()),
                "mean": float(df["amount"].mean()),
            }
        
        all_passed = all(
            check.get("passed", True)
            for check_type in ["null_checks", "uniqueness_checks"]
            for check in checks.get(check_type, {}).values()
        )
        checks["overall_passed"] = all_passed
        
        LOG.info(f"Data quality checks: {checks}")
        
        return checks
    
    processing_state = get_processing_state()
    
    extraction_result = extract_from_api(processing_state)
    
    filter_result = filter_and_deduplicate(extraction_result, processing_state)
    
    s3_raw_path = save_to_raw_s3(filter_result, processing_state)
    
    transform_result = transform_to_curated(filter_result, s3_raw_path)
    
    s3_curated_path = save_to_curated_s3(transform_result, processing_state)
    
    quality_result = data_quality_check(transform_result)
    
    snowflake_success = load_to_snowflake(transform_result, s3_curated_path, processing_state)
    
    state_updated = update_processing_state(filter_result, snowflake_success, processing_state)
    
    start >> processing_state >> extraction_result >> filter_result >> s3_raw_path
    s3_raw_path >> transform_result >> s3_curated_path
    transform_result >> quality_result
    s3_curated_path >> snowflake_success >> state_updated >> end


api_to_snowflake_pipeline_dag = api_to_snowflake_pipeline()