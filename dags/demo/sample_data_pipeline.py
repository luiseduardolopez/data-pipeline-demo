"""
Sample Data Pipeline DAG.

This DAG demonstrates a complete ETL pipeline with:
1. Data extraction from a source
2. Data transformation with encryption
3. Loading to Snowflake
4. Backup to S3
"""

from __future__ import annotations

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator

from src.utils.encryption import encrypt_dataframe_columns
from src.utils.s3_utils import save_df_to_s3
from src.utils.snowflake_utils import load_df_to_snowflake

LOG = logging.getLogger(__name__)


@dag(
    dag_id="sample_data_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["demo", "etl", "sample"],
    description="Sample ETL pipeline demonstrating data extraction, transformation, and loading",
)
def sample_data_pipeline():
    """
    Sample data pipeline that processes customer data.
    
    This DAG demonstrates:
    - Data extraction from various sources
    - Data transformation with encryption
    - Loading to Snowflake data warehouse
    - Backup to S3 storage
    """
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    @task
    def extract_customer_data() -> dict:
        """
        Extract sample customer data.
        
        Returns:
            Dictionary with extracted data
        """
        import pandas as pd
        import numpy as np
        
        # Generate sample customer data
        np.random.seed(42)
        n_customers = 1000
        
        data = {
            "customer_id": range(1, n_customers + 1),
            "first_name": [f"Customer_{i}" for i in range(n_customers)],
            "last_name": [f"LastName_{i}" for i in range(n_customers)],
            "email": [f"customer{i}@example.com" for i in range(n_customers)],
            "phone": [f"555-{i:04d}" for i in range(n_customers)],
            "ssn": [f"{i:03d}-{i:02d}-{i:04d}" for i in range(100, 100 + n_customers)],
            "credit_card": [f"{i:016d}" for i in range(4000000000000000, 4000000000000000 + n_customers)],
            "address": [f"{i} Main St" for i in range(n_customers)],
            "city": [f"City_{i % 10}" for i in range(n_customers)],
            "state": ["CA", "NY", "TX", "FL", "IL"] * (n_customers // 5 + 1),
            "zip_code": [f"{10000 + i % 90000:05d}" for i in range(n_customers)],
            "registration_date": pd.date_range("2020-01-01", periods=n_customers, freq="D"),
            "last_purchase_date": pd.date_range("2023-01-01", periods=n_customers, freq="D"),
            "total_purchases": np.random.uniform(100, 10000, n_customers),
            "customer_tier": np.random.choice(["Bronze", "Silver", "Gold", "Platinum"], n_customers),
        }
        
        df = pd.DataFrame(data)
        LOG.info(f"Generated sample data with {len(df)} customers")
        
        return {
            "data": df.to_dict("records"),
            "count": len(df),
            "columns": list(df.columns),
        }
    
    @task
    def transform_and_encrypt_data(raw_data: dict) -> dict:
        """
        Transform and encrypt sensitive data.
        
        Args:
            raw_data: Raw data from extraction
            
        Returns:
            Dictionary with transformed and encrypted data
        """
        import pandas as pd
        
        # Convert back to DataFrame
        df = pd.DataFrame(raw_data["data"])
        
        # Data transformations
        df["full_name"] = df["first_name"] + " " + df["last_name"]
        df["email_domain"] = df["email"].str.split("@").str[1]
        df["age_group"] = pd.cut(
            pd.to_datetime("today").year - pd.to_datetime(df["registration_date"]).dt.year,
            bins=[0, 25, 35, 50, 65, 100],
            labels=["18-25", "26-35", "36-50", "51-65", "65+"]
        )
        
        # Encrypt sensitive columns
        sensitive_columns = ["ssn", "credit_card", "phone"]
        encrypt_dataframe_columns(df, sensitive_columns, inplace=True)
        
        LOG.info(f"Transformed and encrypted data for {len(df)} customers")
        
        return {
            "data": df.to_dict("records"),
            "count": len(df),
            "columns": list(df.columns),
            "encrypted_columns": sensitive_columns,
        }
    
    @task
    def load_to_snowflake(transformed_data: dict) -> bool:
        """
        Load transformed data to Snowflake.
        
        Args:
            transformed_data: Transformed data
            
        Returns:
            True if successful, False otherwise
        """
        import pandas as pd
        
        df = pd.DataFrame(transformed_data["data"])
        
        success = load_df_to_snowflake(
            df=df,
            table_name="customers",
            if_exists="replace",
        )
        
        if success:
            LOG.info(f"Successfully loaded {len(df)} records to Snowflake")
        else:
            LOG.error("Failed to load data to Snowflake")
        
        return success
    
    @task
    def backup_to_s3(transformed_data: dict) -> bool:
        """
        Backup transformed data to S3.
        
        Args:
            transformed_data: Transformed data
            
        Returns:
            True if successful, False otherwise
        """
        import pandas as pd
        from datetime import datetime
        
        df = pd.DataFrame(transformed_data["data"])
        
        # Create S3 key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"customers/backup/customers_{timestamp}.csv"
        
        success = save_df_to_s3(
            df=df,
            s3_key=s3_key,
            file_format="csv",
        )
        
        if success:
            LOG.info(f"Successfully backed up data to s3://{s3_key}")
        else:
            LOG.error("Failed to backup data to S3")
        
        return success
    
    @task
    def data_quality_checks(transformed_data: dict) -> dict:
        """
        Perform data quality checks.
        
        Args:
            transformed_data: Transformed data
            
        Returns:
            Dictionary with quality check results
        """
        import pandas as pd
        
        df = pd.DataFrame(transformed_data["data"])
        
        checks = {
            "total_records": len(df),
            "null_checks": {},
            "duplicate_checks": {},
            "data_ranges": {},
        }
        
        # Null checks
        for col in ["customer_id", "email", "registration_date"]:
            null_count = df[col].isnull().sum()
            checks["null_checks"][col] = {
                "null_count": null_count,
                "null_percentage": (null_count / len(df)) * 100,
            }
        
        # Duplicate checks
        duplicate_emails = df["email"].duplicated().sum()
        checks["duplicate_checks"]["email"] = {
            "duplicate_count": duplicate_emails,
            "duplicate_percentage": (duplicate_emails / len(df)) * 100,
        }
        
        # Data range checks
        checks["data_ranges"]["total_purchases"] = {
            "min": df["total_purchases"].min(),
            "max": df["total_purchases"].max(),
            "mean": df["total_purchases"].mean(),
        }
        
        LOG.info(f"Data quality checks completed for {len(df)} records")
        return checks
    
    # Define task dependencies
    extracted_data = extract_customer_data()
    transformed_data = transform_and_encrypt_data(extracted_data)
    
    load_task = load_to_snowflake(transformed_data)
    backup_task = backup_to_s3(transformed_data)
    quality_task = data_quality_checks(transformed_data)
    
    start >> extracted_data >> transformed_data
    transformed_data >> [load_task, backup_task, quality_task] >> end


# Instantiate the DAG
sample_data_pipeline_dag = sample_data_pipeline()
