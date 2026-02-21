"""
Unit tests for encryption utilities.
"""

import pytest
import pandas as pd
from unittest.mock import patch

from src.utils.encryption import (
    encrypt_dataframe_columns,
    decrypt_dataframe_columns,
    generate_encryption_key,
    get_encryption_key,
)


class TestEncryption:
    """Test cases for encryption utilities."""
    
    def test_generate_encryption_key(self):
        """Test encryption key generation."""
        password = "test_password"
        key = generate_encryption_key(password)
        
        assert isinstance(key, bytes)
        assert len(key) > 0
        
        # Same password should generate same key
        key2 = generate_encryption_key(password)
        assert key == key2
    
    def test_encrypt_decrypt_dataframe_columns(self):
        """Test encryption and decryption of DataFrame columns."""
        # Create test data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "ssn": ["123-45-6789", "987-65-4321", "555-12-3456"],
            "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
        })
        
        # Mock the encryption key
        with patch("src.utils.encryption.get_encryption_key") as mock_get_key:
            mock_get_key.return_value = generate_encryption_key("test_key")
            
            # Encrypt sensitive columns
            encrypted_df = encrypt_dataframe_columns(
                df, 
                columns=["ssn"], 
                inplace=False
            )
            
            # Verify encryption
            assert encrypted_df["ssn"].iloc[0] != df["ssn"].iloc[0]
            assert encrypted_df["name"].iloc[0] == df["name"].iloc[0]  # Unchanged
            
            # Decrypt columns
            decrypted_df = decrypt_dataframe_columns(
                encrypted_df,
                columns=["ssn"],
                inplace=False
            )
            
            # Verify decryption
            assert decrypted_df["ssn"].iloc[0] == df["ssn"].iloc[0]
            assert decrypted_df.equals(df)
    
    def test_encrypt_nonexistent_columns(self):
        """Test encryption with non-existent columns."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        
        with patch("src.utils.encryption.get_encryption_key"):
            with pytest.raises(ValueError, match="Columns not found"):
                encrypt_dataframe_columns(df, columns=["nonexistent"])
    
    def test_encrypt_empty_dataframe(self):
        """Test encryption with empty DataFrame."""
        df = pd.DataFrame()
        
        with patch("src.utils.encryption.get_encryption_key"):
            result = encrypt_dataframe_columns(df, columns=["test"], inplace=False)
            assert result is None
    
    def test_encrypt_inplace(self):
        """Test inplace encryption."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "ssn": ["123-45-6789", "987-65-4321", "555-12-3456"]
        })
        
        with patch("src.utils.encryption.get_encryption_key"):
            original_id = df["id"].iloc[0]
            encrypt_dataframe_columns(df, columns=["ssn"], inplace=True)
            
            # Verify DataFrame was modified in place
            assert df["id"].iloc[0] == original_id
            assert df["ssn"].iloc[0] != "123-45-6789"
    
    def test_decrypt_with_null_values(self):
        """Test decryption with null values."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "ssn": ["123-45-6789", None, "555-12-3456"]
        })
        
        with patch("src.utils.encryption.get_encryption_key"):
            # Encrypt
            encrypted_df = encrypt_dataframe_columns(df, columns=["ssn"], inplace=False)
            
            # Decrypt
            decrypted_df = decrypt_dataframe_columns(encrypted_df, columns=["ssn"], inplace=False)
            
            # Verify null values are preserved
            assert pd.isna(decrypted_df["ssn"].iloc[1])
            assert decrypted_df["ssn"].iloc[0] == df["ssn"].iloc[0]
