# Data Pipeline Demo

A comprehensive data pipeline demonstration project built with Apache Airflow, showcasing modern data engineering best practices including ETL workflows, data encryption, cloud storage integration, and CI/CD automation.

## 🏗️ Architecture Overview

This project demonstrates a production-ready data pipeline architecture with:

- **Orchestration:** Apache Airflow 2.10.5 with Docker
- **Data Storage:** AWS S3 for raw and curated data layers
- **Data Warehouse:** Snowflake for analytics and reporting
- **Security:** Column-level encryption for sensitive data
- **CI/CD:** GitHub Actions for automated testing and deployment
- **Quality:** Code formatting, linting, and comprehensive testing

## 📁 Project Structure

```
data-pipeline-demo/
├── dags/                    # Airflow DAG definitions
│   ├── demo/               # Example DAGs demonstrating patterns
│   └── extractions/        # Data extraction workflows
├── src/                    # Python source code
│   ├── config/             # Configuration management
│   │   ├── settings.py     # Environment settings
│   │   ├── snowflake.py    # Snowflake configuration
│   │   ├── postgres.py     # PostgreSQL configuration
│   │   └── db.py          # Database utilities
│   ├── utils/              # Shared utilities
│   │   ├── encryption.py   # Data encryption/decryption
│   │   ├── s3_utils.py     # S3 operations
│   │   └── snowflake_utils.py # Snowflake operations
│   ├── models/             # Business logic modules
│   └── extractions/        # Data extraction modules
├── local/                  # Local development CLI
│   └── cli.py             # Command-line interface
├── tests/                  # Test suite
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── .github/workflows/      # CI/CD pipelines
├── docker-compose.yaml     # Docker configuration
├── Dockerfile             # Container definition
├── pyproject.toml         # Poetry dependencies
├── requirements.txt       # Pip dependencies
└── .env.example          # Environment variables template
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+
- AWS credentials (for S3 access)
- Snowflake credentials (for data warehouse)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd data-pipeline-demo
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# Configure AWS, Snowflake, and encryption settings
```

### 3. Install Dependencies

```bash
# Using Poetry (recommended)
poetry install

# Or using pip
pip install -r requirements.txt
```

### 4. Start Airflow with Docker

```bash
# Start all services
docker-compose up -d

# Check services status
docker-compose ps

# View logs
docker-compose logs -f airflow-webserver
```

### 5. Access Airflow UI

Open your browser and navigate to:
- **Airflow UI:** http://localhost:8080
- **Default credentials:** airflow / airflow

## 🛠️ Local Development

### CLI Tools

The project includes a powerful CLI for local development and testing:

```bash
# Display current configuration
python local/cli.py config

# Test all connections
python local/cli.py test-connections

# List S3 files
python local/cli.py list-s3-files --prefix=raw/

# Test encryption functionality
python local/cli.py test-encryption

# Generate sample data
python local/cli.py generate-sample-data --records=1000

# Upload file to S3
python local/cli.py upload-to-s3 file.csv --encrypt-cols=ssn,credit_card

# Setup environment file
python local/cli.py setup-env
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_encryption.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Format code with Black
black src/ dags/ local/ tests/

# Sort imports with isort
isort src/ dags/ local/ tests/

# Lint with ruff
ruff check src/ dags/ local/ tests/

# Run all quality checks
black --check src/ && isort --check-only src/ && ruff check src/
```

## 🔧 Configuration

### Environment Variables

Key environment variables to configure in `.env`:

```bash
# Pipeline Settings
PIPELINE_ENV=dev                    # dev/uat/prod
PIPELINE_RUN_LOCAL=true             # Local development flag
PIPELINE_LOG_LEVEL=INFO             # Logging level

# AWS Configuration
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket

# Snowflake Configuration
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Security
SECRET_ENCRYPTION_KEY=your-32-char-secret-key
```

### Data Encryption

The project includes built-in encryption for sensitive data:

```python
from src.utils.encryption import encrypt_dataframe_columns

# Encrypt sensitive columns
encrypt_dataframe_columns(df, columns=["ssn", "credit_card"])

# Decrypt when needed
from src.utils.encryption import decrypt_dataframe_columns
decrypt_dataframe_columns(df, columns=["ssn", "credit_card"])
```

## 📊 Example DAGs

### Sample Data Pipeline

The `sample_data_pipeline` DAG demonstrates a complete ETL workflow:

1. **Extract:** Generate sample customer data
2. **Transform:** Clean, enrich, and encrypt sensitive fields
3. **Load:** Store in Snowflake and backup to S3
4. **Validate:** Perform data quality checks

### S3 Monitoring

The `s3_monitoring` DAG provides automated storage monitoring:

- Connection health checks
- File analysis and pattern detection
- Data freshness validation
- Automated alerting

## 🔒 Security Features

### Column-Level Encryption

- AES-256 encryption for sensitive data
- Configurable encryption keys per environment
- Seamless integration with pandas DataFrames
- Automatic key management via environment variables

### Access Control

- Environment-specific configurations
- Credential isolation (dev/staging/prod)
- Secure secret management
- Audit logging for data operations

## 🚀 Deployment

### Development

```bash
# Start development environment
docker-compose up -d

# Run migrations (if needed)
docker-compose exec airflow-webserver airflow db migrate
```

### Production

Production deployment is triggered by Git tags:

```bash
# Create and push release tag
git tag rel/v1.0.0
git push origin rel/v1.0.0
```

This automatically:
- Runs full test suite
- Builds Docker images
- Deploys to production
- Sends deployment notifications

## 🧪 Testing Strategy

### Unit Tests
- Individual function and method testing
- Mock external dependencies
- Fast execution and isolation

### Integration Tests
- End-to-end workflow testing
- Real cloud service connections
- Data pipeline validation

### Quality Gates
- Code formatting (Black)
- Import sorting (isort)
- Linting (ruff)
- Security scanning (bandit)
- Dependency vulnerability checks (safety)

## 📈 Monitoring and Observability

### Airflow Monitoring
- DAG execution status
- Task performance metrics
- Error tracking and alerting

### Data Quality
- Automated validation checks
- Schema verification
- Freshness monitoring

### Infrastructure Health
- S3 connectivity monitoring
- Snowflake connection validation
- Resource utilization tracking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add tests for new functionality
- Update documentation
- Ensure all quality checks pass

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

### Troubleshooting

**Common Issues:**

1. **Airflow UI not accessible:**
   ```bash
   # Check if containers are running
   docker-compose ps
   
   # Restart services
   docker-compose restart
   ```

2. **S3 connection errors:**
   ```bash
   # Test AWS credentials
   python local/cli.py test-connections
   
   # Verify environment variables
   python local/cli.py config
   ```

3. **Snowflake connection issues:**
   ```bash
   # Check Snowflake configuration
   python local/cli.py config
   
   # Test connection
   python local/cli.py test-connections
   ```

### Getting Help

- Check the [Issues](../../issues) page for known problems
- Create a new issue for bugs or feature requests
- Review the [Wiki](../../wiki) for detailed documentation

## 🗺️ Roadmap

- [ ] Add dbt integration for data transformations
- [ ] Implement data catalog and lineage tracking
- [ ] Add real-time streaming capabilities
- [ ] Enhanced monitoring and alerting
- [ ] Multi-cloud deployment support
- [ ] Advanced data quality frameworks

---

**Built with ❤️ for modern data engineering**
#   d a t a - p i p e l i n e - d e m o  
 