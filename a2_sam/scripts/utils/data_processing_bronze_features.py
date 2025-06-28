import os
from datetime import datetime
from pyspark.sql.functions import col

def process_bronze_clickstream(snapshot_date_str, bronze_clickstream_directory, spark):
    """处理clickstream特征数据"""
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # 连接到源系统 - 实际应用中连接到后端源系统
    csv_file_path = "data/features_clickstream.csv"

    # 加载数据 - 实际应用中从后端源系统摄取
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(col('snapshot_date') == snapshot_date)
    print(f"Clickstream {snapshot_date_str} row count: {df.count()}")

    # 保存bronze表到数据集市 - 实际应用中连接到数据库写入
    partition_name = f"bronze_features_clickstream_{snapshot_date_str.replace('-','_')}.csv"
    filepath = bronze_clickstream_directory + partition_name
    df.toPandas().to_csv(filepath, index=False)
    print(f"Clickstream saved to: {filepath}")

    return df

def process_bronze_attributes(snapshot_date_str, bronze_attributes_directory, spark):
    """处理attributes特征数据"""
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    csv_file_path = "data/features_attributes.csv"
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(col('snapshot_date') == snapshot_date)
    print(f"Attributes {snapshot_date_str} row count: {df.count()}")

    partition_name = f"bronze_features_attributes_{snapshot_date_str.replace('-','_')}.csv"
    filepath = bronze_attributes_directory + partition_name
    df.toPandas().to_csv(filepath, index=False)
    print(f"Attributes saved to: {filepath}")

    return df

def process_bronze_financials(snapshot_date_str, bronze_financials_directory, spark):
    """处理financials特征数据"""
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    csv_file_path = "data/features_financials.csv"
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True).filter(col('snapshot_date') == snapshot_date)
    print(f"Financials {snapshot_date_str} row count: {df.count()}")

    partition_name = f"bronze_features_financials_{snapshot_date_str.replace('-','_')}.csv"
    filepath = bronze_financials_directory + partition_name
    df.toPandas().to_csv(filepath, index=False)
    print(f"Financials saved to: {filepath}")

    return df