FROM python:3.12-slim

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Install Java (OpenJDK 17 headless), procps (for 'ps') and bash
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps bash && \
    rm -rf /var/lib/apt/lists/* && \
    # Ensure Spark's scripts run with bash instead of dash
    ln -sf /bin/bash /bin/sh && \
    # Create expected JAVA_HOME directory and symlink the java binary there (only if missing)
    mkdir -p /usr/lib/jvm/java-17-openjdk-amd64/bin && \
    [ -f /usr/lib/jvm/java-17-openjdk-amd64/bin/java ] || ln -s "$(which java)" /usr/lib/jvm/java-17-openjdk-amd64/bin/java

# Set JAVA_HOME to the directory expected by Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements file into the container
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set Airflow environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins \
    /opt/airflow/data /opt/airflow/datamart /opt/airflow/utils \
    /opt/airflow/models /opt/airflow/monitoring

# Expose Airflow webserver port
EXPOSE 8080

# Create a volume mount point
VOLUME /opt/airflow

# Default command (will be overridden by docker-compose)
CMD ["airflow", "webserver"]