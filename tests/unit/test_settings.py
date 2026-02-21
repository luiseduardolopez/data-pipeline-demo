"""
Unit tests for settings configuration.
"""

import os
import pytest
from unittest.mock import patch

from src.config.settings import Settings, get_environment, get_settings


class TestSettings:
    """Test cases for settings configuration."""
    
    def test_default_settings(self):
        """Test default settings values."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            
            assert settings.env == "dev"
            assert settings.run_local is True
            assert settings.log_level == "INFO"
            assert settings.aws_default_region == "us-east-1"
    
    def test_environment_override(self):
        """Test environment variable override."""
        with patch.dict(os.environ, {
            "PIPELINE_ENV": "prod",
            "PIPELINE_LOG_LEVEL": "DEBUG",
            "AWS_DEFAULT_REGION": "eu-west-1"
        }):
            settings = Settings()
            
            assert settings.env == "prod"
            assert settings.log_level == "DEBUG"
            assert settings.aws_default_region == "eu-west-1"
    
    def test_get_environment(self):
        """Test get_environment function."""
        with patch.dict(os.environ, {"PIPELINE_ENV": "uat"}):
            env = get_environment()
            assert env == "uat"
        
        with patch.dict(os.environ, {}, clear=True):
            env = get_environment()
            assert env == "dev"
    
    def test_get_settings_cached(self):
        """Test that get_settings returns cached instance."""
        with patch.dict(os.environ, {}, clear=True):
            settings1 = get_settings()
            settings2 = get_settings()
            
            assert settings1 is settings2
    
    def test_snowflake_configuration(self):
        """Test Snowflake configuration settings."""
        with patch.dict(os.environ, {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_password",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_SCHEMA": "test_schema",
            "SNOWFLAKE_ROLE": "test_role"
        }):
            settings = Settings()
            
            assert settings.snowflake_user == "test_user"
            assert settings.snowflake_password == "test_password"
            assert settings.snowflake_account == "test_account"
            assert settings.snowflake_warehouse == "test_warehouse"
            assert settings.snowflake_database == "test_database"
            assert settings.snowflake_schema == "test_schema"
            assert settings.snowflake_role == "test_role"
    
    def test_aws_configuration(self):
        """Test AWS configuration settings."""
        with patch.dict(os.environ, {
            "AWS_ACCESS_KEY_ID": "test_key_id",
            "AWS_SECRET_ACCESS_KEY": "test_secret_key",
            "AWS_DEFAULT_REGION": "us-west-2",
            "S3_BUCKET_NAME": "test-bucket"
        }):
            settings = Settings()
            
            assert settings.aws_access_key_id == "test_key_id"
            assert settings.aws_secret_access_key == "test_secret_key"
            assert settings.aws_default_region == "us-west-2"
            assert settings.s3_bucket_name == "test-bucket"
    
    def test_postgresql_configuration(self):
        """Test PostgreSQL configuration settings."""
        with patch.dict(os.environ, {
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "POSTGRES_HOST": "test_host",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "test_db"
        }):
            settings = Settings()
            
            assert settings.postgres_user == "test_user"
            assert settings.postgres_password == "test_password"
            assert settings.postgres_host == "test_host"
            assert settings.postgres_port == "5433"
            assert settings.postgres_db == "test_db"
