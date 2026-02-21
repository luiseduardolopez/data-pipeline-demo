"""
Command Line Interface for local development.

This module provides a CLI for testing and running pipeline components
locally without requiring Airflow.
"""

import os
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

# Add src to Python path for local development
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from src.config.settings import get_settings
from src.utils.encryption import (
    encrypt_dataframe_columns,
    decrypt_dataframe_columns,
    generate_encryption_key,
)
from src.utils.s3_utils import (
    check_s3_connection,
    list_s3_objects,
    save_df_to_s3,
    read_csv_from_s3,
)
from src.utils.snowflake_utils import (
    check_snowflake_connection,
    read_snowflake_to_df,
    load_df_to_snowflake,
)

app = typer.Typer(help="Data Pipeline Local Development CLI")
console = Console()


@app.command()
def config():
    """Display current configuration settings."""
    settings = get_settings()
    
    table = Table(title="Pipeline Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    config_items = [
        ("Environment", settings.env),
        ("Run Local", str(settings.run_local)),
        ("Log Level", settings.log_level),
        ("AWS Region", settings.aws_default_region),
        ("S3 Bucket", settings.s3_bucket_name or "Not configured"),
        ("Snowflake Account", settings.snowflake_account or "Not configured"),
        ("Snowflake Database", settings.snowflake_database or "Not configured"),
        ("Encryption Key", "✓ Set" if settings.secret_encryption_key else "✗ Not set"),
    ]
    
    for setting, value in config_items:
        table.add_row(setting, value)
    
    console.print(table)


@app.command()
def test_connections():
    """Test all configured connections."""
    console.print("[bold blue]Testing Connections...[/bold blue]")
    
    # Test S3
    console.print("\n[yellow]Testing S3 Connection...[/yellow]")
    try:
        s3_ok = check_s3_connection()
        if s3_ok:
            console.print("[green]✓ S3 connection successful[/green]")
        else:
            console.print("[red]✗ S3 connection failed[/red]")
    except Exception as e:
        console.print(f"[red]✗ S3 connection error: {e}[/red]")
    
    # Test Snowflake
    console.print("\n[yellow]Testing Snowflake Connection...[/yellow]")
    try:
        snowflake_ok = check_snowflake_connection()
        if snowflake_ok:
            console.print("[green]✓ Snowflake connection successful[/green]")
        else:
            console.print("[red]✗ Snowflake connection failed[/red]")
    except Exception as e:
        console.print(f"[red]✗ Snowflake connection error: {e}[/red]")


@app.command()
def list_s3_files(prefix: str = ""):
    """List files in S3 storage."""
    console.print(f"[bold blue]Listing S3 files with prefix: '{prefix}'[/bold blue]")
    
    try:
        files = list_s3_objects(prefix=prefix)
        
        if not files:
            console.print("[yellow]No files found[/yellow]")
            return
        
        table = Table(title="S3 Files")
        table.add_column("File Path", style="cyan")
        table.add_column("Size", style="green")
        
        for file_path in files:
            table.add_row(file_path, "N/A")  # Size would require additional API calls
        
        console.print(table)
        console.print(f"\n[green]Total files: {len(files)}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error listing S3 files: {e}[/red]")


@app.command()
def test_encryption():
    """Test encryption/decryption functionality."""
    console.print("[bold blue]Testing Encryption...[/bold blue]")
    
    try:
        import pandas as pd
        import numpy as np
        
        # Create test data
        test_data = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "ssn": ["123-45-6789", "987-65-4321", "555-12-3456"],
            "credit_card": ["4111111111111111", "5555555555554444", "378282246310005"],
        })
        
        console.print("\n[yellow]Original Data:[/yellow]")
        console.print(test_data.to_string(index=False))
        
        # Encrypt sensitive columns
        sensitive_cols = ["ssn", "credit_card"]
        encrypted_data = encrypt_dataframe_columns(
            test_data, 
            columns=sensitive_cols, 
            inplace=False
        )
        
        console.print("\n[yellow]Encrypted Data:[/yellow]")
        console.print(encrypted_data.to_string(index=False))
        
        # Decrypt columns
        decrypted_data = decrypt_dataframe_columns(
            encrypted_data,
            columns=sensitive_cols,
            inplace=False
        )
        
        console.print("\n[yellow]Decrypted Data:[/yellow]")
        console.print(decrypted_data.to_string(index=False))
        
        # Verify data integrity
        if test_data.equals(decrypted_data):
            console.print("\n[green]✓ Encryption/Decryption test passed[/green]")
        else:
            console.print("\n[red]✗ Encryption/Decryption test failed[/red]")
            
    except Exception as e:
        console.print(f"[red]Error during encryption test: {e}[/red]")


@app.command()
def generate_sample_data(
    output_file: str = "sample_data.csv",
    records: int = 1000,
):
    """Generate sample data for testing."""
    console.print(f"[bold blue]Generating {records} sample records...[/bold blue]")
    
    try:
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        
        # Generate sample data
        np.random.seed(42)
        
        data = {
            "customer_id": range(1, records + 1),
            "first_name": [f"Customer_{i}" for i in range(records)],
            "last_name": [f"LastName_{i}" for i in range(records)],
            "email": [f"customer{i}@example.com" for i in range(records)],
            "phone": [f"555-{i:04d}" for i in range(records)],
            "ssn": [f"{100+i:03d}-{50+i:02d}-{1000+i:04d}" for i in range(records)],
            "address": [f"{i} Main St" for i in range(records)],
            "city": np.random.choice(["New York", "Los Angeles", "Chicago", "Houston"], records),
            "state": np.random.choice(["NY", "CA", "IL", "TX"], records),
            "zip_code": [f"{10000 + i % 90000:05d}" for i in range(records)],
            "registration_date": [
                (datetime.now() - timedelta(days=np.random.randint(1, 1000))).date()
                for _ in range(records)
            ],
            "total_purchases": np.random.uniform(100, 10000, records),
        }
        
        df = pd.DataFrame(data)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        console.print(f"[green]✓ Sample data saved to {output_file}[/green]")
        console.print(f"[green]Generated {len(df)} records[/green]")
        
    except Exception as e:
        console.print(f"[red]Error generating sample data: {e}[/red]")


@app.command()
def upload_to_s3(
    file_path: str,
    s3_key: Optional[str] = None,
    encrypt_cols: Optional[str] = None,
):
    """Upload a file to S3 with optional encryption."""
    if not os.path.exists(file_path):
        console.print(f"[red]File not found: {file_path}[/red]")
        return
    
    if s3_key is None:
        s3_key = f"uploads/{os.path.basename(file_path)}"
    
    console.print(f"[bold blue]Uploading {file_path} to s3://{s3_key}[/bold blue]")
    
    try:
        import pandas as pd
        
        # Read the file
        df = pd.read_csv(file_path)
        
        # Parse encryption columns
        columns_to_encrypt = None
        if encrypt_cols:
            columns_to_encrypt = [col.strip() for col in encrypt_cols.split(",")]
        
        # Upload to S3
        success = save_df_to_s3(
            df=df,
            s3_key=s3_key,
            file_format="csv",
            columns_to_encrypt=columns_to_encrypt,
        )
        
        if success:
            console.print(f"[green]✓ File uploaded successfully to s3://{s3_key}[/green]")
        else:
            console.print("[red]✗ Upload failed[/red]")
            
    except Exception as e:
        console.print(f"[red]Error uploading file: {e}[/red]")


@app.command()
def setup_env():
    """Set up environment file from template."""
    env_example = Path(".env.example")
    env_file = Path(".env")
    
    if env_file.exists():
        console.print("[yellow]⚠ .env file already exists[/yellow]")
        if not typer.confirm("Do you want to overwrite it?"):
            return
    
    if env_example.exists():
        import shutil
        shutil.copy(env_example, env_file)
        console.print("[green]✓ .env file created from .env.example[/green]")
        console.print("[yellow]Please edit .env file with your credentials[/yellow]")
    else:
        console.print("[red]✗ .env.example file not found[/red]")


if __name__ == "__main__":
    # Set environment for local development
    os.environ["PIPELINE_RUN_LOCAL"] = "true"
    app()
