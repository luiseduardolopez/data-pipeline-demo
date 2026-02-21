"""
Snowflake database configuration and connection utilities.

This module provides configuration and connection management
for Snowflake data warehouse operations.
"""

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .settings import get_settings


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    
    @classmethod
    def from_settings(cls) -> "SnowflakeConfig":
        """Create SnowflakeConfig from application settings."""
        settings = get_settings()
        return cls(
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            account=settings.snowflake_account,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            role=settings.snowflake_role,
        )
    
    def get_connection_string(self) -> str:
        """
        Generate Snowflake connection string.
        
        Returns:
            Connection string for SQLAlchemy
        """
        connection_params = [
            f"snowflake://{self.user}:{self.password}@{self.account}/{self.database}/{self.schema}",
            f"warehouse={self.warehouse}",
        ]
        
        if self.role:
            connection_params.append(f"role={self.role}")
            
        return "?".join(connection_params)
    
    def create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine for Snowflake.
        
        Returns:
            SQLAlchemy Engine instance
        """
        connection_string = self.get_connection_string()
        return create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600,
        )


def get_snowflake_engine() -> Engine:
    """
    Get Snowflake SQLAlchemy engine.
    
    Returns:
        SQLAlchemy Engine instance for Snowflake
    """
    config = SnowflakeConfig.from_settings()
    return config.create_engine()
