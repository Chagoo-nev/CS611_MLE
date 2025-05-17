"""
Bronze 层数据处理工具函数
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, col, to_date

def create_bronze_directories():
    """创建 Bronze 层数据目录"""
    os.makedirs("datamart/bronze", exist_ok=True)
    print("Created bronze directory")

def process_bronze_dataframe(spark, file_path, source_name, date_columns=None):
    """
    将原始 CSV 处理为 Bronze 层 DataFrame
    
    Args:
        spark (SparkSession): Spark 会话
        file_path (str): CSV 文件路径
        source_name (str): 数据源名称
        date_columns (list): 需要转换为日期类型的列名列表
        
    Returns:
        DataFrame: 处理后的 Bronze 层 DataFrame
    """
    print(f"Processing {source_name} from {file_path}")
    
    # 读取原始 CSV 文件
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # 添加 Bronze 层元数据
    df = df.withColumn("bronze_ingest_timestamp", current_timestamp()) \
           .withColumn("bronze_source_file", lit(os.path.basename(file_path)))
    
    # 日期列转换 (如果指定)
    if date_columns:
        for date_col in date_columns:
            if date_col in df.columns:
                df = df.withColumn(date_col, to_date(col(date_col), "d/MM/yyyy"))
    
    return df

def save_bronze_table(df, table_name):
    """
    将 DataFrame 保存为 Bronze 层表
    
    Args:
        df (DataFrame): 要保存的 DataFrame
        table_name (str): 表名 (不含 bronze_ 前缀)
        
    Returns:
        str: 保存的表路径
    """
    output_path = f"datamart/bronze/bronze_{table_name}"
    
    # 保存为 Parquet 格式
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"Saved bronze table to {output_path}")
    return output_path