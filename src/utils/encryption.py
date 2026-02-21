"""
Encryption utilities for sensitive data handling.

This module provides functions to encrypt and decrypt sensitive data
in pandas DataFrames using AES encryption.
"""

from __future__ import annotations

import base64
import logging
from typing import List, Optional, Union

import pandas as pd
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from src.config.settings import get_settings

LOG = logging.getLogger(__name__)


def generate_encryption_key(password: str, salt: Optional[bytes] = None) -> bytes:
    """
    Generate encryption key from password using PBKDF2.
    
    Args:
        password: Password to derive key from
        salt: Optional salt bytes (generated if not provided)
        
    Returns:
        Encryption key bytes
    """
    if salt is None:
        salt = b"data_pipeline_salt"  # In production, use a proper random salt
    
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    return key


def get_encryption_key() -> bytes:
    """
    Get encryption key from settings.
    
    Returns:
        Encryption key bytes
        
    Raises:
        ValueError: If encryption key is not configured
    """
    settings = get_settings()
    if not settings.secret_encryption_key:
        raise ValueError(
            "SECRET_ENCRYPTION_KEY not configured. "
            "Set this environment variable to enable encryption features."
        )
    
    return generate_encryption_key(settings.secret_encryption_key)


def encrypt_dataframe_columns(
    df: pd.DataFrame,
    columns: List[str],
    inplace: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Encrypt specified columns in a DataFrame.
    
    Args:
        df: DataFrame to encrypt columns in
        columns: List of column names to encrypt
        inplace: Whether to modify DataFrame in-place
        
    Returns:
        DataFrame with encrypted columns (if inplace=False)
        
    Raises:
        ValueError: If columns don't exist or encryption key not configured
    """
    if df.empty:
        LOG.warning("DataFrame is empty, nothing to encrypt")
        return df if inplace else None
    
    # Validate columns exist
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
    
    if not inplace:
        df = df.copy()
    
    try:
        key = get_encryption_key()
        fernet = Fernet(key)
        
        for col in columns:
            # Convert to string, encrypt, and store back
            encrypted_values = []
            for value in df[col]:
                if pd.isna(value):
                    encrypted_values.append(value)
                else:
                    str_value = str(value)
                    encrypted_value = fernet.encrypt(str_value.encode())
                    encrypted_values.append(encrypted_value.decode())
            
            df[col] = encrypted_values
        
        LOG.info(f"Successfully encrypted {len(columns)} columns")
        
    except Exception as e:
        LOG.error(f"Error encrypting DataFrame columns: {e}")
        raise
    
    return None if inplace else df


def decrypt_dataframe_columns(
    df: pd.DataFrame,
    columns: List[str],
    inplace: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Decrypt specified columns in a DataFrame.
    
    Args:
        df: DataFrame to decrypt columns in
        columns: List of column names to decrypt
        inplace: Whether to modify DataFrame in-place
        
    Returns:
        DataFrame with decrypted columns (if inplace=False)
        
    Raises:
        ValueError: If columns don't exist or encryption key not configured
    """
    if df.empty:
        LOG.warning("DataFrame is empty, nothing to decrypt")
        return df if inplace else None
    
    # Validate columns exist
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")
    
    if not inplace:
        df = df.copy()
    
    try:
        key = get_encryption_key()
        fernet = Fernet(key)
        
        for col in columns:
            decrypted_values = []
            for value in df[col]:
                if pd.isna(value):
                    decrypted_values.append(value)
                else:
                    try:
                        decrypted_value = fernet.decrypt(value.encode())
                        decrypted_values.append(decrypted_value.decode())
                    except Exception as e:
                        LOG.warning(f"Failed to decrypt value in column {col}: {e}")
                        decrypted_values.append(value)  # Keep original if decryption fails
            
            df[col] = decrypted_values
        
        LOG.info(f"Successfully decrypted {len(columns)} columns")
        
    except Exception as e:
        LOG.error(f"Error decrypting DataFrame columns: {e}")
        raise
    
    return None if inplace else df
