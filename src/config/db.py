"""
Database utilities and logging configuration.

This module provides common database utilities and logging
configuration used across the application.
"""

import logging
import sys
from typing import Any, Dict

from .settings import get_settings


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    settings = get_settings()
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Set log level
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Avoid duplicate handlers
    if not logger.handlers:
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
    
    return logger


def get_connection_params(conn_type: str) -> Dict[str, Any]:
    """
    Get connection parameters for different database types.
    
    Args:
        conn_type: Type of connection (snowflake, postgres, etc.)
        
    Returns:
        Dictionary of connection parameters
    """
    settings = get_settings()
    
    if conn_type.lower() == "snowflake":
        return {
            "user": settings.snowflake_user,
            "password": settings.snowflake_password,
            "account": settings.snowflake_account,
            "warehouse": settings.snowflake_warehouse,
            "database": settings.snowflake_database,
            "schema": settings.snowflake_schema,
            "role": settings.snowflake_role,
        }
    elif conn_type.lower() == "postgres":
        return {
            "user": settings.postgres_user,
            "password": settings.postgres_password,
            "host": settings.postgres_host,
            "port": settings.postgres_port,
            "database": settings.postgres_db,
        }
    else:
        raise ValueError(f"Unsupported connection type: {conn_type}")
