"""
Silver层数据处理脚本 - 独立于Bronze层处理
"""
import os
from utils.spark_utils import create_spark_session, stop_spark_session
from utils.silver_utils import (
    create_silver_directories, 
    process_silver_customer_profile,
    process_silver_financials,
    process_silver_loan_daily,
    process_silver_clickstream,
    save_silver_table
)

def evaluate_silver_data_quality(spark):
    """
    评估Silver层数据质量
    
    Args:
        spark (SparkSession): Spark会话
        
    Returns:
        dict: 数据质量指标
    """
    from pyspark.sql.functions import col
    
    quality_metrics = {}
    
    # 检查客户profile表
    try:
        profile_df = spark.read.parquet("datamart/silver/silver_customer_profile")
        profile_metrics = {
            "total_rows": profile_df.count(),
            "null_age_percent": profile_df.filter(col("Age").isNull()).count() / profile_df.count() * 100 if "Age" in profile_df.columns else "N/A",
            "valid_ssn_percent": profile_df.filter(col("SSN_Valid") == True).count() / profile_df.count() * 100 if "SSN_Valid" in profile_df.columns else "N/A"
        }
        quality_metrics["customer_profile"] = profile_metrics
    except Exception as e:
        quality_metrics["customer_profile"] = {"error": str(e)}
    
    # 检查金融表
    try:
        financials_df = spark.read.parquet("datamart/silver/silver_financials")
        financials_metrics = {
            "total_rows": financials_df.count(),
            "null_income_percent": financials_df.filter(col("Annual_Income").isNull()).count() / financials_df.count() * 100 if "Annual_Income" in financials_df.columns else "N/A",
        }
        
        # 检查Credit_Mix_Standard如果存在
        if "Credit_Mix_Standard" in financials_df.columns:
            credit_mix_counts = financials_df.groupBy("Credit_Mix_Standard").count().collect()
            financials_metrics["credit_mix_distribution"] = {
                row["Credit_Mix_Standard"]: row["count"] / financials_df.count() * 100
                for row in credit_mix_counts
            }
            
        quality_metrics["financials"] = financials_metrics
    except Exception as e:
        quality_metrics["financials"] = {"error": str(e)}
    
    # 检查贷款表
    try:
        loan_df = spark.read.parquet("datamart/silver/silver_loan_daily")
        loan_metrics = {
            "total_rows": loan_df.count(),
            "amount_check_valid_percent": loan_df.filter(col("amount_check_flag") == True).count() / loan_df.count() * 100 if "amount_check_flag" in loan_df.columns else "N/A"
        }
        quality_metrics["loan_daily"] = loan_metrics
    except Exception as e:
        quality_metrics["loan_daily"] = {"error": str(e)}
    
    # 检查点击流表
    try:
        clickstream_df = spark.read.parquet("datamart/silver/silver_clickstream")
        clickstream_metrics = {
            "total_rows": clickstream_df.count(),
            "rows_per_customer": clickstream_df.count() / clickstream_df.select("Customer_ID").distinct().count() if "Customer_ID" in clickstream_df.columns else "N/A"
        }
        quality_metrics["clickstream"] = clickstream_metrics
    except Exception as e:
        quality_metrics["clickstream"] = {"error": str(e)}
    
    # 打印数据质量报告
    print("\n=== Silver Layer Data Quality Report ===")
    for table, metrics in quality_metrics.items():
        print(f"\nTable: {table}")
        for metric, value in metrics.items():
            if metric != "credit_mix_distribution":
                print(f"  {metric}: {value}")
            else:
                print(f"  {metric}:")
                for category, percent in value.items():
                    print(f"    {category}: {percent:.2f}%")
    
    return quality_metrics

def process_silver_layer():
    """单独处理Silver层数据"""
    # 创建Spark会话
    spark = create_spark_session(app_name="Silver Layer Processing")
    print("Spark session created for Silver Layer processing")
    
    try:
        # 1. 创建Silver目录
        create_silver_directories()
        
        # 2. 处理客户属性数据
        print("\nProcessing customer profile data...")
        customer_profile_df = process_silver_customer_profile(spark)
        save_silver_table(customer_profile_df, "customer_profile")
        
        # 3. 处理金融数据
        print("\nProcessing financial data...")
        financials_df = process_silver_financials(spark)
        save_silver_table(financials_df, "financials")
        
        # 4. 处理贷款数据
        print("\nProcessing loan daily data...")
        loan_daily_df = process_silver_loan_daily(spark)
        save_silver_table(loan_daily_df, "loan_daily")
        
        # 5. 处理点击流数据
        print("\nProcessing clickstream data...")
        clickstream_df = process_silver_clickstream(spark)
        save_silver_table(clickstream_df, "clickstream")
        
        print("\nSilver layer processing completed successfully")
        
        # 6. 评估数据质量
        evaluate_silver_data_quality(spark)
        
    except Exception as e:
        print(f"Error in Silver layer processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保Spark会话被停止
        stop_spark_session(spark)

if __name__ == "__main__":
    process_silver_layer()