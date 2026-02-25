FROM apache/airflow:3.1.7

COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory for Airflow
WORKDIR /opt/airflow

# Create necessary directories for DAGs, logs, and data
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/data
