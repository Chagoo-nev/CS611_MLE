from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'simple_spark_test_dag',
    default_args=default_args,
    description='Simple Spark Test DAG',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'spark'],
)

def simple_spark_test():
    """Simplified Spark test with better error handling"""
    print("Starting simple Spark test...")
    
    try:
        # Check Java installation
        java_home = os.environ.get('JAVA_HOME')
        print(f"JAVA_HOME: {java_home}")
        
        import subprocess
        java_version = subprocess.run(['java', '-version'], capture_output=True, text=True)
        print(f"Java version check: {java_version.stderr}")
        
        # Import PySpark
        print("Importing PySpark...")
        import pyspark
        print(f"PySpark version: {pyspark.__version__}")
        
        # Try creating SparkSession with minimal config
        print("Creating SparkSession...")
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("SimpleTest") \
            .master("local[1]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        print("✅ SparkSession created successfully!")
        
        # Simple test
        data = [1, 2, 3, 4, 5]
        rdd = spark.sparkContext.parallelize(data)
        result = rdd.sum()
        print(f"✅ Simple RDD test result: {result}")
        
        # Clean up
        spark.stop()
        print("✅ Spark test completed successfully")
        
        return "Success"
        
    except Exception as e:
        print(f"❌ Error in Spark test: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        raise

def check_spark_dependencies():
    """Check all Spark-related dependencies"""
    print("Checking Spark dependencies...")
    
    # Check environment variables
    env_vars = ['JAVA_HOME', 'SPARK_HOME', 'PYTHONPATH']
    for var in env_vars:
        value = os.environ.get(var, 'Not set')
        print(f"{var}: {value}")
    
    # Check available memory
    try:
        import psutil
        memory = psutil.virtual_memory()
        print(f"Available memory: {memory.available / (1024**3):.1f} GB")
    except ImportError:
        print("psutil not available, cannot check memory")
    
    # Check Python packages
    try:
        import pyspark
        import py4j
        print(f"✅ PySpark: {pyspark.__version__}")
        print(f"✅ Py4J: {py4j.__version__}")
    except ImportError as e:
        print(f"❌ Import error: {e}")
    
    return "Dependencies checked"

# Define tasks
check_deps_task = PythonOperator(
    task_id='check_spark_dependencies',
    python_callable=check_spark_dependencies,
    dag=dag,
)

simple_spark_task = PythonOperator(
    task_id='simple_spark_test',
    python_callable=simple_spark_test,
    dag=dag,
)

# Set task dependencies
check_deps_task >> simple_spark_task