"""
Medallion Architecture Data Pipeline for Loan Default Prediction
"""
import os
from utils.spark_utils import create_spark_session, stop_spark_session
from utils.bronze_utils import create_bronze_directories, process_bronze_dataframe, save_bronze_table

def create_datamart_directories():
    """创建数据集市目录结构"""
    os.makedirs("datamart/bronze", exist_ok=True)
    os.makedirs("datamart/silver", exist_ok=True)
    os.makedirs("datamart/gold", exist_ok=True)
    print("Created datamart directories")

def process_bronze_layer(spark):
    """处理 Bronze 层数据"""
    # 1. 处理贷款数据
    loan_df = process_bronze_dataframe(
        spark=spark,
        file_path="data/lms_loan_daily.csv",
        source_name="loan_daily",
        date_columns=["loan_start_date", "snapshot_date"]
    )
    save_bronze_table(loan_df, "loan_daily")
    
    # 2. 处理金融数据
    financials_df = process_bronze_dataframe(
        spark=spark,
        file_path="data/features_financials.csv",
        source_name="financials",
        date_columns=["snapshot_date"]
    )
    save_bronze_table(financials_df, "financials")
    
    # 3. 处理属性数据
    attributes_df = process_bronze_dataframe(
        spark=spark,
        file_path="data/features_attributes.csv",
        source_name="attributes",
        date_columns=["snapshot_date"]
    )
    save_bronze_table(attributes_df, "attributes")
    
    # 4. 处理点击流数据
    clickstream_df = process_bronze_dataframe(
        spark=spark,
        file_path="data/feature_clickstream.csv",
        source_name="clickstream",
        date_columns=["snapshot_date"]
    )
    save_bronze_table(clickstream_df, "clickstream")
    
    print("Bronze layer processing completed")

def main():
    """主函数，运行整个管道"""
    # 创建 Spark 会话
    spark = create_spark_session()
    print("Spark session created")
    
    try:
        # 创建数据集市目录
        create_datamart_directories()
        
        # 处理 Bronze 层
        process_bronze_layer(spark)
        
        print("Data pipeline completed successfully")
    except Exception as e:
        print(f"Error in data pipeline: {e}")
        raise
    finally:
        # 确保 Spark 会话被停止
        stop_spark_session(spark)

if __name__ == "__main__":
    main()