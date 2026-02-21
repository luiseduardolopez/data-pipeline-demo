"""
S3 utilities for data storage and retrieval.

This module provides functions to interact with AWS S3 for storing
and retrieving data files, with integrated encryption support.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

from src.config.settings import get_settings
from src.utils.encryption import decrypt_dataframe_columns, encrypt_dataframe_columns

LOG = logging.getLogger(__name__)


def get_s3_client():
    """
    Get configured S3 client.
    
    Returns:
        boto3 S3 client
        
    Raises:
        ValueError: If AWS credentials are not configured
    """
    settings = get_settings()
    
    if not settings.aws_access_key_id or not settings.aws_secret_access_key:
        raise ValueError(
            "AWS credentials not configured. "
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
        )
    
    return boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )


def get_s3_resource():
    """
    Get configured S3 resource.
    
    Returns:
        boto3 S3 resource
    """
    settings = get_settings()
    
    return boto3.resource(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )


def save_df_to_s3(
    df: pd.DataFrame,
    s3_key: str,
    bucket_name: Optional[str] = None,
    file_format: str = "csv",
    columns_to_encrypt: Optional[List[str]] = None,
    **kwargs,
) -> bool:
    """
    Save DataFrame to S3 with optional encryption.
    
    Args:
        df: DataFrame to save
        s3_key: S3 object key (path)
        bucket_name: S3 bucket name (from settings if not provided)
        file_format: File format ('csv', 'parquet', 'json')
        columns_to_encrypt: List of columns to encrypt before saving
        **kwargs: Additional arguments for pandas to_* functions
        
    Returns:
        True if successful, False otherwise
    """
    if df.empty:
        LOG.warning("DataFrame is empty, nothing to save")
        return False
    
    settings = get_settings()
    bucket = bucket_name or settings.s3_bucket_name
    
    if not bucket:
        LOG.error("S3 bucket name not configured")
        return False
    
    try:
        # Encrypt sensitive columns if specified
        if columns_to_encrypt:
            df = encrypt_dataframe_columns(df, columns_to_encrypt, inplace=False)
        
        # Prepare data based on format
        if file_format.lower() == "csv":
            buffer = df.to_csv(index=False, **kwargs)
            content_type = "text/csv"
        elif file_format.lower() == "parquet":
            buffer = df.to_parquet(index=False, **kwargs)
            content_type = "application/octet-stream"
        elif file_format.lower() == "json":
            buffer = df.to_json(orient="records", **kwargs)
            content_type = "application/json"
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Upload to S3
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buffer,
            ContentType=content_type,
        )
        
        LOG.info(f"Successfully saved DataFrame to s3://{bucket}/{s3_key}")
        return True
        
    except Exception as e:
        LOG.error(f"Error saving DataFrame to S3: {e}")
        return False


def read_csv_from_s3(
    s3_key: str,
    bucket_name: Optional[str] = None,
    columns_to_decrypt: Optional[List[str]] = None,
    **kwargs,
) -> Optional[pd.DataFrame]:
    """
    Read CSV file from S3 with optional decryption.
    
    Args:
        s3_key: S3 object key (path)
        bucket_name: S3 bucket name (from settings if not provided)
        columns_to_decrypt: List of columns to decrypt after reading
        **kwargs: Additional arguments for pandas read_csv
        
    Returns:
        DataFrame if successful, None otherwise
    """
    settings = get_settings()
    bucket = bucket_name or settings.s3_bucket_name
    
    if not bucket:
        LOG.error("S3 bucket name not configured")
        return None
    
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        
        # Read CSV data
        df = pd.read_csv(response["Body"], **kwargs)
        
        # Decrypt sensitive columns if specified
        if columns_to_decrypt:
            df = decrypt_dataframe_columns(df, columns_to_decrypt, inplace=False)
        
        LOG.info(f"Successfully read DataFrame from s3://{bucket}/{s3_key}")
        return df
        
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            LOG.error(f"S3 object not found: s3://{bucket}/{s3_key}")
        else:
            LOG.error(f"Error reading from S3: {e}")
        return None
    except Exception as e:
        LOG.error(f"Error reading CSV from S3: {e}")
        return None


def list_s3_objects(
    prefix: str = "",
    bucket_name: Optional[str] = None,
) -> List[str]:
    """
    List objects in S3 bucket with given prefix.
    
    Args:
        prefix: S3 prefix to filter objects
        bucket_name: S3 bucket name (from settings if not provided)
        
    Returns:
        List of S3 object keys
    """
    settings = get_settings()
    bucket = bucket_name or settings.s3_bucket_name
    
    if not bucket:
        LOG.error("S3 bucket name not configured")
        return []
    
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        objects = []
        if "Contents" in response:
            objects = [obj["Key"] for obj in response["Contents"]]
        
        LOG.info(f"Found {len(objects)} objects with prefix '{prefix}'")
        return objects
        
    except Exception as e:
        LOG.error(f"Error listing S3 objects: {e}")
        return []


def delete_s3_object(
    s3_key: str,
    bucket_name: Optional[str] = None,
) -> bool:
    """
    Delete object from S3.
    
    Args:
        s3_key: S3 object key to delete
        bucket_name: S3 bucket name (from settings if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    settings = get_settings()
    bucket = bucket_name or settings.s3_bucket_name
    
    if not bucket:
        LOG.error("S3 bucket name not configured")
        return False
    
    try:
        s3_client = get_s3_client()
        s3_client.delete_object(Bucket=bucket, Key=s3_key)
        
        LOG.info(f"Successfully deleted s3://{bucket}/{s3_key}")
        return True
        
    except Exception as e:
        LOG.error(f"Error deleting S3 object: {e}")
        return False


def check_s3_connection() -> bool:
    """
    Check if S3 connection is working.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        s3_client = get_s3_client()
        s3_client.list_buckets()
        LOG.info("S3 connection successful")
        return True
    except NoCredentialsError:
        LOG.error("AWS credentials not found")
        return False
    except Exception as e:
        LOG.error(f"S3 connection failed: {e}")
        return False
