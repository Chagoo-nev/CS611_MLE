"""
Spark 会话管理和工具函数
"""
from pyspark.sql import SparkSession

def create_spark_session(app_name="Medallion Pipeline"):
    """
    创建并配置 Spark 会话
    
    Args:
        app_name (str): Spark 应用名称
        
    Returns:
        SparkSession: 已配置的 Spark 会话
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
    安全地停止 Spark 会话
    
    Args:
        spark (SparkSession): 要停止的 Spark 会话
    """
    if spark is not None:
        spark.stop()