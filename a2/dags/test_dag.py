from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def test_python_function():
    """Test function to verify Python environment"""
    import pandas as pd
    import numpy as np
    import pyspark
    
    print("=== Environment Test ===")
    print(f"Pandas version: {pd.__version__}")
    print(f"NumPy version: {np.__version__}")
    print(f"PySpark version: {pyspark.__version__}")
    
    # Test basic functionality
    df = pd.DataFrame({'test': [1, 2, 3]})
    print(f"Test DataFrame created: {df.shape}")
    
    return "Environment test completed successfully!"

def test_directory_structure():
    """Test if all directories are accessible"""
    import os
    
    directories = [
        '/opt/airflow/data',
        '/opt/airflow/datamart',
        '/opt/airflow/utils',
        '/opt/airflow/models',
        '/opt/airflow/monitoring'
    ]
    
    for directory in directories:
        if os.path.exists(directory):
            print(f"✓ Directory exists: {directory}")
        else:
            print(f"✗ Directory missing: {directory}")
            
    return "Directory structure test completed!"

# Default arguments
default_args = {
    'owner': 'sam',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'test_environment',
    default_args=default_args,
    description='Test DAG to verify Airflow environment',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'environment'],
)

# Create directories task
create_dirs_task = BashOperator(
    task_id='create_directories',
    bash_command='''
    mkdir -p /opt/airflow/models/experiments
    mkdir -p /opt/airflow/monitoring/reports
    mkdir -p /opt/airflow/datamart/bronze
    mkdir -p /opt/airflow/datamart/silver
    mkdir -p /opt/airflow/datamart/gold
    echo "Directories created successfully!"
    ''',
    dag=dag,
)

# Test Python environment
test_python_task = PythonOperator(
    task_id='test_python_environment',
    python_callable=test_python_function,
    dag=dag,
)

# Test directory structure
test_dirs_task = PythonOperator(
    task_id='test_directory_structure',
    python_callable=test_directory_structure,
    dag=dag,
)

# Define task dependencies
create_dirs_task >> test_python_task >> test_dirs_task