FROM apache/airflow:2.10.5

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
ENV AIRFLOW_HOME="/opt/airflow"

# Install system dependencies
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        && apt-get autoremove -yqq --purge \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# Create necessary directories
RUN mkdir -p /opt/airflow/logs /opt/airflow/plugins

# Set proper permissions
USER root
RUN chown -R airflow:airflow /opt/airflow
USER airflow

# Expose Airflow webserver port
EXPOSE 8080

# Default command
CMD ["airflow", "webserver"]
