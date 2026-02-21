"""
Settings configuration for the data pipeline.

This module handles environment-specific configuration settings,
including database connections, AWS credentials, and other
application settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    pass

# Load environment variables from .env file if it exists
load_dotenv()


class Environment(str, Enum):
    """Application environment enumeration."""

    DEV = "dev"
    UAT = "uat"
    PROD = "prod"


@dataclass
class Settings:
    """Application settings configuration."""
    
    # Environment
    env: str = os.getenv("PIPELINE_ENV", Environment.DEV.value)
    run_local: bool = os.getenv("PIPELINE_RUN_LOCAL", "true").lower() == "true"
    log_level: str = os.getenv("PIPELINE_LOG_LEVEL", "INFO")
    
    # Encryption
    secret_encryption_key: str = os.getenv("SECRET_ENCRYPTION_KEY", "")
    
    # AWS Configuration
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_default_region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    
    # S3 Configuration
    s3_bucket_name: str = os.getenv("S3_BUCKET_NAME", "")
    s3_raw_layer: str = os.getenv("S3_RAW_LAYER", "raw/")
    s3_curated_layer: str = os.getenv("S3_CURATED_LAYER", "curated/")
    
    # Snowflake Configuration
    snowflake_user: str = os.getenv("SNOWFLAKE_USER", "")
    snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE", "")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "")
    snowflake_role: str = os.getenv("SNOWFLAKE_ROLE", "")
    
    # PostgreSQL Configuration (Airflow metadata)
    postgres_user: str = os.getenv("POSTGRES_USER", "airflow")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "airflow")
    postgres_db: str = os.getenv("POSTGRES_DB", "airflow")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: str = os.getenv("POSTGRES_PORT", "5432")
    
    # Slack Configuration
    slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", "")
    
    def __post_init__(self) -> None:
        """Post-initialization validation."""
        if not self.secret_encryption_key and self.run_local:
            print("WARNING: SECRET_ENCRYPTION_KEY not set. Encryption features will not work.")
        
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            print("WARNING: AWS credentials not configured. S3 features will not work.")
            
        if not all([
            self.snowflake_user, self.snowflake_password, 
            self.snowflake_account, self.snowflake_warehouse,
            self.snowflake_database, self.snowflake_schema
        ]):
            print("WARNING: Snowflake credentials not fully configured. Snowflake features will not work.")


@lru_cache(maxsize=1)
def get_environment() -> str:
    """
    Get the current environment setting.
    
    Returns:
        Current environment (dev/uat/prod)
    """
    return os.getenv("PIPELINE_ENV", Environment.DEV.value).lower()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Returns:
        Settings object with current configuration
    """
    return Settings()
