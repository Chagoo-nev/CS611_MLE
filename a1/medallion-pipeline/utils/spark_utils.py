"""
Spark session management and utility functions
"""
from pyspark.sql import SparkSession

def create_spark_session(app_name="Medallion Pipeline"):
   """
   Create and configure Spark session
   
   Args:
       app_name (str): Spark application name
       
   Returns:
       SparkSession: Configured Spark session
   """
   return (SparkSession.builder
           .appName(app_name)
           .config("spark.sql.warehouse.dir", "./spark-warehouse")
           .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
           .config("spark.driver.memory", "2g")
           .config("spark.executor.memory", "2g")
           .config("spark.sql.execution.arrow.pyspark.enabled", "true")
           .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
           .enableHiveSupport()
           .getOrCreate())

def stop_spark_session(spark):
   """
   Safely stop Spark session
   
   Args:
       spark (SparkSession): Spark session to stop
   """
   if spark is not None:
       spark.stop()