"""
Configuration module for the data pipeline.

This module provides centralized configuration management for the entire
data pipeline application, including database connections, AWS settings,
and environment-specific configurations.
"""

from .settings import get_environment, get_settings
from .snowflake import SnowflakeConfig
from .postgres import PostgresConfig

__all__ = [
    "get_environment",
    "get_settings", 
    "SnowflakeConfig",
    "PostgresConfig",
]
