from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'test_pipeline_dag',
    default_args=default_args,
    description='Test ML Pipeline DAG',
    schedule_interval=None,  # Manual trigger for now
    catchup=False,
    tags=['test', 'ml', 'pipeline'],
)

def test_environment():
    """Test function to verify environment setup"""
    print("Testing environment...")
    
    # Check PySpark
    try:
        import pyspark
        print(f"✅ PySpark version: {pyspark.__version__}")
    except ImportError as e:
        print(f"❌ PySpark import failed: {e}")
    
    # Check other dependencies
    try:
        import pandas as pd
        import numpy as np
        import sklearn
        print(f"✅ Pandas version: {pd.__version__}")
        print(f"✅ NumPy version: {np.__version__}")
        print(f"✅ Scikit-learn version: {sklearn.__version__}")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
    
    # Check file paths
    base_path = "/opt/airflow"
    scripts_path = os.path.join(base_path, "scripts")
    data_path = os.path.join(base_path, "scripts", "data")
    
    print(f"📁 Base path exists: {os.path.exists(base_path)}")
    print(f"📁 Scripts path exists: {os.path.exists(scripts_path)}")
    print(f"📁 Data path exists: {os.path.exists(data_path)}")
    
    if os.path.exists(data_path):
        files = os.listdir(data_path)
        print(f"📄 Files in data directory: {files}")
    
    return "Environment test completed"

def test_spark_session():
    """Test PySpark session creation"""
    print("Testing Spark session...")
    
    try:
        import pyspark
        from pyspark.sql import SparkSession
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("AirflowTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Test basic functionality
        test_data = [(1, "test"), (2, "data")]
        df = spark.createDataFrame(test_data, ["id", "value"])
        
        print(f"✅ Spark session created successfully")
        print(f"✅ Test DataFrame count: {df.count()}")
        
        df.show()
        
        spark.stop()
        return "Spark test completed successfully"
        
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        raise

# Define tasks
test_env_task = PythonOperator(
    task_id='test_environment',
    python_callable=test_environment,
    dag=dag,
)

test_spark_task = PythonOperator(
    task_id='test_spark_session',
    python_callable=test_spark_session,
    dag=dag,
)

check_directories_task = BashOperator(
    task_id='check_directories',
    bash_command='''
    echo "=== Directory Structure ==="
    ls -la /opt/airflow/
    echo "=== Scripts Directory ==="
    ls -la /opt/airflow/scripts/ 2>/dev/null || echo "Scripts directory not found"
    echo "=== Data Directory ==="
    ls -la /opt/airflow/scripts/data/ 2>/dev/null || echo "Data directory not found"
    ''',
    dag=dag,
)

# Set task dependencies
test_env_task >> check_directories_task >> test_spark_task