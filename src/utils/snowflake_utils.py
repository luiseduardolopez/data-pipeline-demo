"""
Snowflake utilities for data warehouse operations.

This module provides functions to interact with Snowflake data warehouse
for executing queries and loading data.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from snowflake.connector import Error as SnowflakeError
from snowflake.connector import connect
from sqlalchemy import text

from src.config.settings import get_settings

LOG = logging.getLogger(__name__)


def get_snowflake_connection():
    """
    Get Snowflake connection using settings.
    
    Returns:
        Snowflake connection object
        
    Raises:
        ValueError: If Snowflake credentials are not configured
    """
    settings = get_settings()
    
    required_fields = [
        "snowflake_user", "snowflake_password", "snowflake_account",
        "snowflake_warehouse", "snowflake_database", "snowflake_schema"
    ]
    
    missing_fields = [
        field for field in required_fields 
        if not getattr(settings, field)
    ]
    
    if missing_fields:
        raise ValueError(
            f"Missing Snowflake configuration: {', '.join(missing_fields)}"
        )
    
    try:
        conn = connect(
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            account=settings.snowflake_account,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            role=settings.snowflake_role,
        )
        LOG.info("Successfully connected to Snowflake")
        return conn
    except SnowflakeError as e:
        LOG.error(f"Error connecting to Snowflake: {e}")
        raise


def execute_snowflake_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    fetch: bool = True,
) -> Optional[Union[List[tuple], int]]:
    """
    Execute SQL query on Snowflake.
    
    Args:
        query: SQL query to execute
        params: Query parameters
        fetch: Whether to fetch results (for SELECT queries)
        
    Returns:
        Query results (list of tuples) or row count for INSERT/UPDATE/DELETE
    """
    conn = None
    cursor = None
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        if fetch:
            results = cursor.fetchall()
            LOG.info(f"Query executed successfully, returned {len(results)} rows")
            return results
        else:
            row_count = cursor.rowcount
            LOG.info(f"Query executed successfully, affected {row_count} rows")
            return row_count
            
    except SnowflakeError as e:
        LOG.error(f"Error executing Snowflake query: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def read_snowflake_to_df(
    query: str,
    params: Optional[Dict[str, Any]] = None,
) -> Optional[pd.DataFrame]:
    """
    Read data from Snowflake into DataFrame.
    
    Args:
        query: SQL query to execute
        params: Query parameters
        
    Returns:
        DataFrame with query results or None if error
    """
    try:
        # Use SQLAlchemy engine for better DataFrame integration
        from src.config.snowflake import get_snowflake_engine
        
        engine = get_snowflake_engine()
        
        if params:
            df = pd.read_sql(text(query), engine, params=params)
        else:
            df = pd.read_sql(text(query), engine)
        
        LOG.info(f"Successfully read {len(df)} rows from Snowflake")
        return df
        
    except Exception as e:
        LOG.error(f"Error reading from Snowflake to DataFrame: {e}")
        return None


def load_df_to_snowflake(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
    method: str = "multi",
    chunksize: int = 10000,
) -> bool:
    """
    Load DataFrame to Snowflake table.
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        if_exists: Action if table exists ('fail', 'replace', 'append')
        method: Method for insertion ('multi', None)
        chunksize: Number of rows per insert
        
    Returns:
        True if successful, False otherwise
    """
    if df.empty:
        LOG.warning("DataFrame is empty, nothing to load")
        return False
    
    try:
        from src.config.snowflake import get_snowflake_engine
        
        engine = get_snowflake_engine()
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method=method,
            chunksize=chunksize,
        )
        
        LOG.info(f"Successfully loaded {len(df)} rows to {table_name}")
        return True
        
    except Exception as e:
        LOG.error(f"Error loading DataFrame to Snowflake: {e}")
        return False


def create_snowflake_table_from_df(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "fail",
) -> bool:
    """
    Create Snowflake table from DataFrame schema.
    
    Args:
        df: DataFrame to create table from
        table_name: Target table name
        if_exists: Action if table exists ('fail', 'replace')
        
    Returns:
        True if successful, False otherwise
    """
    if df.empty:
        LOG.warning("DataFrame is empty, cannot create table")
        return False
    
    try:
        from src.config.snowflake import get_snowflake_engine
        
        engine = get_snowflake_engine()
        
        # Create table with schema but no data
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
        )
        
        LOG.info(f"Successfully created table {table_name}")
        return True
        
    except Exception as e:
        LOG.error(f"Error creating Snowflake table: {e}")
        return False


def check_snowflake_connection() -> bool:
    """
    Check if Snowflake connection is working.
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        LOG.info(f"Snowflake connection successful. Version: {result[0]}")
        return True
    except Exception as e:
        LOG.error(f"Snowflake connection failed: {e}")
        return False


def get_snowflake_table_info(table_name: str) -> Optional[Dict[str, Any]]:
    """
    Get information about a Snowflake table.
    
    Args:
        table_name: Table name to get info for
        
    Returns:
        Dictionary with table information or None if error
    """
    try:
        settings = get_settings()
        
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        
        results = execute_snowflake_query(
            query, 
            params=(settings.snowflake_schema, table_name)
        )
        
        if results:
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3],
                    "max_length": row[4],
                }
                for row in results
            ]
            
            return {
                "table_name": table_name,
                "schema": settings.snowflake_schema,
                "database": settings.snowflake_database,
                "columns": columns,
            }
        
        return None
        
    except Exception as e:
        LOG.error(f"Error getting table info for {table_name}: {e}")
        return None
