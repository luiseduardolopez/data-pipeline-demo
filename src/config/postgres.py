"""
PostgreSQL database configuration and connection utilities.

This module provides configuration and connection management
for PostgreSQL database operations, primarily for Airflow metadata.
"""

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .settings import get_settings


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""
    
    user: str
    password: str
    host: str
    port: str
    database: str
    
    @classmethod
    def from_settings(cls) -> "PostgresConfig":
        """Create PostgresConfig from application settings."""
        settings = get_settings()
        return cls(
            user=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
        )
    
    def get_connection_string(self) -> str:
        """
        Generate PostgreSQL connection string.
        
        Returns:
            Connection string for SQLAlchemy
        """
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
    
    def create_engine(self) -> Engine:
        """
        Create SQLAlchemy engine for PostgreSQL.
        
        Returns:
            SQLAlchemy Engine instance
        """
        connection_string = self.get_connection_string()
        return create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=3600,
        )


def get_postgres_engine() -> Engine:
    """
    Get PostgreSQL SQLAlchemy engine.
    
    Returns:
        SQLAlchemy Engine instance for PostgreSQL
    """
    config = PostgresConfig.from_settings()
    return config.create_engine()
