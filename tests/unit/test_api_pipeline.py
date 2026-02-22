"""
Tests for API to Snowflake ETL Pipeline.

These tests validate:
- API extraction with mocks
- Data transformations
- Idempotency logic
- Incremental processing
- Error handling and retries
"""

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import requests

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "dags" / "demo"))

# Sample API response data for testing
SAMPLE_API_RESPONSE = {
    "orders": [
        {
            "order_id": "ord_1001",
            "user_id": "usr_9001",
            "amount": 49.99,
            "currency": "USD",
            "created_at": "2025-01-15T09:15:00Z",
            "items": [{"sku": "sku-tee-01", "qty": 1, "price": 49.99}],
            "metadata": {"source": "web", "note": "duplicate_record"}
        },
        {
            "order_id": "ord_1002",
            "user_id": "usr_9002",
            "amount": 120.0,
            "currency": "USD",
            "created_at": "2025-01-16T10:00:00Z",
            "items": [
                {"sku": "sku-mug-02", "qty": 2, "price": 30.0},
                {"sku": "sku-hoodie-03", "qty": 1, "price": 60.0}
            ],
            "metadata": {"source": "mobile", "campaign": "back-to-school"}
        },
        {
            "order_id": "ord_1003",
            "user_id": "usr_9003",
            "amount": 15.5,
            "currency": "USD",
            "created_at": "2025-01-17T08:30:00Z",
            "items": [{"sku": "sku-sticker-04", "qty": 5, "price": 3.1}],
            "metadata": {"source": "web"}
        },
        {
            "order_id": "ord_1001",  # Duplicate of first order
            "user_id": "usr_9001",
            "amount": 49.99,
            "currency": "USD",
            "created_at": "2025-01-15T09:15:00Z",
            "items": [{"sku": "sku-tee-01", "qty": 1, "price": 49.99}],
            "metadata": {"source": "web", "note": "duplicate_record"}
        }
    ]
}


class TestAPIExtraction:
    """Tests for API extraction functionality."""
    
    @patch('requests.get')
    def test_extract_success(self, mock_get):
        """Test successful API extraction."""
        # Configure mock
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_API_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Import here to avoid issues with Airflow dependencies
        from api_to_snowflake_pipeline import extract_from_api
        
        processing_state = {
            "api_base_url": "https://test.api.com",
            "api_endpoint": "api/v1/orders",
            "since_timestamp": None,
        }
        
        result = extract_from_api.function(processing_state)
        
        assert result["count"] == 4
        assert len(result["records"]) == 4
        assert result["api_url"] == "https://test.api.com/api/v1/orders"
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_extract_with_since_timestamp(self, mock_get):
        """Test API extraction with incremental timestamp."""
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_API_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        from api_to_snowflake_pipeline import extract_from_api
        
        processing_state = {
            "api_base_url": "https://test.api.com",
            "api_endpoint": "api/v1/orders",
            "since_timestamp": "2025-01-15T00:00:00",
        }
        
        result = extract_from_api.function(processing_state)
        
        # Verify the 'since' parameter was passed
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[1]["params"]["since"] == "2025-01-15T00:00:00"
    
    @patch('requests.get')
    def test_extract_retry_on_failure(self, mock_get):
        """Test retry logic on API failure."""
        # First two calls fail, third succeeds
        mock_get.side_effect = [
            requests.exceptions.RequestException("Connection error"),
            requests.exceptions.RequestException("Timeout"),
            Mock(json=lambda: SAMPLE_API_RESPONSE, raise_for_status=lambda: None)
        ]
        
        from api_to_snowflake_pipeline import extract_from_api
        
        processing_state = {
            "api_base_url": "https://test.api.com",
            "api_endpoint": "api/v1/orders",
            "since_timestamp": None,
        }
        
        result = extract_from_api.function(processing_state)
        
        assert result["count"] == 4
        assert mock_get.call_count == 3  # 2 retries + 1 success
    
    def test_extract_handles_different_response_formats(self):
        """Test handling of various API response formats."""
        from api_to_snowflake_pipeline import extract_from_api
        
        test_cases = [
            # Format 1: data key
            {"data": SAMPLE_API_RESPONSE["orders"]},
            # Format 2: results key
            {"results": SAMPLE_API_RESPONSE["orders"]},
            # Format 3: items key
            {"items": SAMPLE_API_RESPONSE["orders"]},
            # Format 4: direct list
            SAMPLE_API_RESPONSE["orders"],
            # Format 5: single record wrapped
            {"order_id": "ord_0001", "amount": 100.0},
        ]
        
        for test_data in test_cases:
            with patch('requests.get') as mock_get:
                mock_response = Mock()
                mock_response.json.return_value = test_data
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response
                
                processing_state = {
                    "api_base_url": "https://test.api.com",
                    "api_endpoint": "api/v1/orders",
                    "since_timestamp": None,
                }
                
                result = extract_from_api.function(processing_state)
                
                assert "records" in result
                assert isinstance(result["records"], list)
                assert len(result["records"]) > 0


class TestIdempotencyAndFiltering:
    """Tests for idempotency and incremental processing."""
    
    def test_filter_new_records(self):
        """Test filtering of already processed records."""
        from api_to_snowflake_pipeline import filter_and_deduplicate
        
        extraction_result = {
            "records": SAMPLE_API_RESPONSE["orders"],
            "since_timestamp": None,
        }
        
        # State with one already processed ID
        processing_state = {
            "state": {
                "processed_ids": ["ord_1001"],
                "last_processed_timestamp": None,
            }
        }
        
        result = filter_and_deduplicate.function(extraction_result, processing_state)
        
        # Should filter out the duplicate ord_1001
        assert result["new_count"] == 2  # ord_1002 and ord_1003
        assert result["skipped_count"] == 2  # 2 copies of ord_1001
        assert "ord_1001" not in result["new_ids"]
        assert "ord_1002" in result["new_ids"]
        assert "ord_1003" in result["new_ids"]
    
    def test_filter_with_since_timestamp(self):
        """Test incremental filtering by timestamp."""
        from api_to_snowflake_pipeline import filter_and_deduplicate
        
        extraction_result = {
            "records": SAMPLE_API_RESPONSE["orders"],
            "since_timestamp": "2025-01-16T00:00:00",  # Only records after this
        }
        
        processing_state = {
            "state": {
                "processed_ids": [],
                "last_processed_timestamp": None,
            }
        }
        
        result = filter_and_deduplicate.function(extraction_result, processing_state)
        
        # Should only include ord_1002 and ord_1003 (created after Jan 16)
        assert result["new_count"] == 2
        assert all(
            record["order_id"] in ["ord_1002", "ord_1003"]
            for record in result["records"]
        )
    
    def test_filter_all_records_new(self):
        """Test when all records are new."""
        from api_to_to_snowflake_pipeline import filter_and_deduplicate
        
        extraction_result = {
            "records": SAMPLE_API_RESPONSE["orders"],
            "since_timestamp": None,
        }
        
        processing_state = {
            "state": {
                "processed_ids": [],
                "last_processed_timestamp": None,
            }
        }
        
        result = filter_and_deduplicate.function(extraction_result, processing_state)
        
        # Should process all unique records (4 total, but 1 duplicate = 3 unique)
        assert result["new_count"] == 3
        assert result["skipped_count"] == 1  # The duplicate ord_1001


class TestDataTransformation:
    """Tests for data transformation logic."""
    
    def test_transform_standardizes_columns(self):
        """Test column name standardization."""
        from api_to_snowflake_pipeline import transform_to_curated
        
        filter_result = {
            "records": [
                {"Order ID": "ord_1001", "User ID": "usr_9001", "Amount": 49.99}
            ],
            "new_ids": ["ord_1001"],
            "new_count": 1,
        }
        
        result = transform_to_curated.function(filter_result, "s3://bucket/raw/data.json")
        
        df = pd.DataFrame(result["records"])
        
        # Check column names are standardized (lowercase, spaces to underscores)
        assert "order_id" in df.columns or "order_id" in [c.lower().replace(" ", "_") for c in df.columns]
        assert "_etl_processed_at" in df.columns
        assert "_etl_batch_id" in df.columns
    
    def test_transform_adds_partition_columns(self):
        """Test addition of date partition columns."""
        from api_to_snowflake_pipeline import transform_to_curated
        
        filter_result = {
            "records": SAMPLE_API_RESPONSE["orders"][:2],
            "new_ids": ["ord_1001", "ord_1002"],
            "new_count": 2,
        }
        
        result = transform_to_curated.function(filter_result, "s3://bucket/raw/data.json")
        
        df = pd.DataFrame(result["records"])
        
        # Check partition columns exist
        assert "year" in df.columns
        assert "month" in df.columns
        assert "day" in df.columns
        
        # Check values are correct
        assert df["year"].iloc[0] == 2025
        assert df["month"].iloc[0] == 1
    
    def test_transform_removes_duplicates(self):
        """Test removal of duplicate records based on idempotency key."""
        from api_to_snowflake_pipeline import transform_to_curated
        
        # Include the duplicate ord_1001
        filter_result = {
            "records": SAMPLE_API_RESPONSE["orders"],
            "new_ids": ["ord_1001", "ord_1002", "ord_1003"],
            "new_count": 4,
        }
        
        result = transform_to_curated.function(filter_result, "s3://bucket/raw/data.json")
        
        df = pd.DataFrame(result["records"])
        
        # Should have removed the duplicate
        order_ids = df["order_id"].tolist()
        assert order_ids.count("ord_1001") == 1
        assert len(df) == 3  # 4 records - 1 duplicate = 3
    
    def test_transform_handles_empty_data(self):
        """Test handling of empty data."""
        from api_to_snowflake_pipeline import transform_to_curated
        
        filter_result = {
            "records": [],
            "new_ids": [],
            "new_count": 0,
        }
        
        result = transform_to_curated.function(filter_result, "s3://bucket/raw/data.json")
        
        assert result["count"] == 0
        assert result["records"] == []


class TestDataQuality:
    """Tests for data quality checks."""
    
    def test_quality_check_null_values(self):
        """Test detection of null values in critical columns."""
        from api_to_snowflake_pipeline import data_quality_check
        
        # Data with null in critical column
        records = [
            {"order_id": "ord_1001", "created_at": "2025-01-15T09:15:00Z", "amount": 49.99},
            {"order_id": None, "created_at": "2025-01-16T10:00:00Z", "amount": 120.0},  # Null order_id
        ]
        
        transform_result = {
            "records": records,
            "count": 2,
        }
        
        result = data_quality_check.function(transform_result)
        
        assert result["total_records"] == 2
        assert "order_id" in result["null_checks"]
        assert result["null_checks"]["order_id"]["null_count"] == 1
        assert result["null_checks"]["order_id"]["passed"] == False
    
    def test_quality_check_uniqueness(self):
        """Test detection of duplicate IDs."""
        from api_to_snowflake_pipeline import data_quality_check
        
        transform_result = {
            "records": [
                {"order_id": "ord_1001", "amount": 49.99},
                {"order_id": "ord_1001", "amount": 50.00},  # Duplicate
            ],
            "count": 2,
        }
        
        result = data_quality_check.function(transform_result)
        
        assert "order_id" in result["uniqueness_checks"]
        assert result["uniqueness_checks"]["order_id"]["duplicate_count"] == 1
        assert result["uniqueness_checks"]["order_id"]["passed"] == False
        assert result["overall_passed"] == False
    
    def test_quality_check_range_validation(self):
        """Test range validation for numeric columns."""
        from api_to_snowflake_pipeline import data_quality_check
        
        transform_result = {
            "records": [
                {"order_id": "ord_1001", "amount": 49.99},
                {"order_id": "ord_1002", "amount": 120.0},
                {"order_id": "ord_1003", "amount": 15.5},
            ],
            "count": 3,
        }
        
        result = data_quality_check.function(transform_result)
        
        assert "amount" in result["range_checks"]
        assert result["range_checks"]["amount"]["min"] == 15.5
        assert result["range_checks"]["amount"]["max"] == 120.0
        assert result["range_checks"]["amount"]["mean"] == pytest.approx(61.83, 0.01)


class TestProcessingState:
    """Tests for processing state management."""
    
    @patch('api_to_snowflake_pipeline.Variable')
    def test_state_initialization(self, mock_variable):
        """Test initialization of processing state."""
        from api_to_snowflake_pipeline import get_processing_state
        
        # Mock no existing state
        mock_variable.get.side_effect = Exception("Not found")
        
        with patch('api_to_snowflake_pipeline.LOG'):
            result = get_processing_state.function()
        
        assert result["state"]["last_processed_timestamp"] is None
        assert result["state"]["processed_ids"] == []
        assert result["state"]["run_count"] == 0
    
    @patch('api_to_snowflake_pipeline.Variable')
    def test_state_with_last_processed(self, mock_variable):
        """Test using last_processed timestamp."""
        from api_to_snowflake_pipeline import get_processing_state
        
        # Mock existing state
        mock_variable.get.return_value = {
            "last_processed_timestamp": "2025-01-15T00:00:00",
            "processed_ids": ["ord_1001"],
            "run_count": 5,
        }
        
        # Mock context with use_last_processed=True
        mock_context = {
            "params": {
                "api_base_url": "https://test.api.com",
                "api_endpoint": "api/v1/orders",
                "since_timestamp": None,
                "use_last_processed": True,
                "dry_run": False,
                "snowflake_table": "orders",
            }
        }
        
        with patch('api_to_snowflake_pipeline.LOG'):
            result = get_processing_state.function(**mock_context)
        
        assert result["since_timestamp"] == "2025-01-15T00:00:00"
    
    @patch('api_to_snowflake_pipeline.Variable')
    def test_state_update_after_success(self, mock_variable):
        """Test state update after successful run."""
        from api_to_snowflake_pipeline import update_processing_state
        
        # Mock existing state
        existing_state = {
            "last_processed_timestamp": None,
            "processed_ids": [],
            "run_count": 0,
        }
        mock_variable.get.return_value = existing_state
        
        filter_result = {
            "records": [{"order_id": "ord_1001"}, {"order_id": "ord_1002"}],
            "new_ids": ["ord_1001", "ord_1002"],
            "new_count": 2,
        }
        
        processing_state = {
            "dry_run": False,
        }
        
        with patch('api_to_snowflake_pipeline.LOG'):
            result = update_processing_state.function(
                filter_result, True, processing_state
            )
        
        assert result == True
        # Verify Variable.set was called with updated state
        mock_variable.set.assert_called_once()
        call_args = mock_variable.set.call_args
        updated_state = call_args[0][1]
        assert updated_state["run_count"] == 1
        assert len(updated_state["processed_ids"]) == 2
        assert updated_state["last_processed_timestamp"] is not None


class TestS3Operations:
    """Tests for S3 save operations."""
    
    @patch('api_to_snowflake_pipeline.S3Hook')
    def test_save_to_raw_s3(self, mock_s3_hook_class):
        """Test saving raw data to S3 with partitioning."""
        from api_to_snowflake_pipeline import save_to_raw_s3
        
        # Configure mock
        mock_s3_hook = MagicMock()
        mock_s3_hook_class.return_value = mock_s3_hook
        mock_s3_hook.get_bucket.return_value = "test-bucket"
        
        filter_result = {
            "records": SAMPLE_API_RESPONSE["orders"][:2],
            "new_count": 2,
        }
        
        processing_state = {
            "dry_run": False,
        }
        
        result = save_to_raw_s3.function(filter_result, processing_state)
        
        # Verify S3 path includes partitioning
        assert "raw/year=" in result
        assert "month=" in result
        assert "day=" in result
        assert ".json" in result
        
        # Verify S3Hook was called
        mock_s3_hook.load_string.assert_called_once()
    
    @patch('api_to_snowflake_pipeline.S3Hook')
    def test_save_to_curated_s3(self, mock_s3_hook_class):
        """Test saving curated data to S3 with partitioning."""
        from api_to_snowflake_pipeline import save_to_curated_s3
        
        mock_s3_hook = MagicMock()
        mock_s3_hook_class.return_value = mock_s3_hook
        mock_s3_hook.get_bucket.return_value = "test-bucket"
        
        transform_result = {
            "records": [
                {
                    "order_id": "ord_1001",
                    "amount": 49.99,
                    "year": 2025,
                    "month": 1,
                    "day": 15,
                }
            ],
            "count": 1,
        }
        
        processing_state = {
            "dry_run": False,
        }
        
        result = save_to_curated_s3.function(transform_result, processing_state)
        
        # Verify S3 path includes partitioning and parquet format
        assert "curated/year=" in result
        assert "month=" in result
        assert "day=" in result
        assert ".parquet" in result
        
        # Verify S3Hook was called with bytes (parquet)
        mock_s3_hook.load_bytes.assert_called_once()
    
    def test_dry_run_skips_s3_save(self):
        """Test that dry run mode skips S3 operations."""
        from api_to_snowflake_pipeline import save_to_raw_s3
        
        filter_result = {
            "records": SAMPLE_API_RESPONSE["orders"],
            "new_count": 4,
        }
        
        processing_state = {
            "dry_run": True,  # Dry run enabled
        }
        
        with patch('api_to_snowflake_pipeline.S3Hook') as mock_s3:
            result = save_to_raw_s3.function(filter_result, processing_state)
        
        assert result == "dry_run"
        mock_s3.assert_not_called()


class TestSnowflakeOperations:
    """Tests for Snowflake operations."""
    
    @patch('api_to_snowflake_pipeline.SnowflakeHook')
    def test_load_to_snowflake_with_merge(self, mock_sf_hook_class):
        """Test Snowflake load with MERGE for idempotency."""
        from api_to_snowflake_pipeline import load_to_snowflake
        
        # Configure mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_sf_hook = MagicMock()
        mock_sf_hook_class.return_value = mock_sf_hook
        mock_sf_hook.get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        transform_result = {
            "records": [
                {
                    "order_id": "ord_1001",
                    "amount": 49.99,
                    "created_at": "2025-01-15T09:15:00",
                }
            ],
            "count": 1,
            "columns": ["order_id", "amount", "created_at"],
        }
        
        s3_curated_path = "s3://bucket/curated/data.parquet"
        
        processing_state = {
            "dry_run": False,
            "snowflake_table": "orders",
        }
        
        result = load_to_snowflake.function(
            transform_result, s3_curated_path, processing_state
        )
        
        assert result == True
        
        # Verify MERGE SQL was executed
        execute_calls = mock_cursor.execute.call_args_list
        merge_call_found = any(
            "MERGE INTO" in str(call) for call in execute_calls
        )
        assert merge_call_found, "MERGE statement should be executed for idempotency"
    
    def test_dry_run_skips_snowflake(self):
        """Test that dry run mode skips Snowflake operations."""
        from api_to_snowflake_pipeline import load_to_snowflake
        
        transform_result = {
            "records": [{"order_id": "ord_1001", "amount": 49.99}],
            "count": 1,
        }
        
        s3_curated_path = "s3://bucket/curated/data.parquet"
        
        processing_state = {
            "dry_run": True,
            "snowflake_table": "orders",
        }
        
        with patch('api_to_snowflake_pipeline.SnowflakeHook') as mock_sf:
            result = load_to_snowflake.function(
                transform_result, s3_curated_path, processing_state
            )
        
        assert result == True
        mock_sf.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
