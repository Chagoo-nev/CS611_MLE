import argparse
import os
import pyspark
import utils.data_processing_bronze_features

# to call this script: python bronze_features_financials.py --snapshotdate "2023-01-01"

def main(snapshotdate):
    print('\n\n---starting financials bronze job---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("bronze_financials") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # 创建bronze目录
    bronze_financials_directory = "datamart/bronze/features_financials/"
    if not os.path.exists(bronze_financials_directory):
        os.makedirs(bronze_financials_directory)

    # 运行数据处理
    utils.data_processing_bronze_features.process_bronze_financials(
        snapshotdate, bronze_financials_directory, spark
    )
    
    # end spark session
    spark.stop()
    print('\n\n---completed financials bronze job---\n\n')

if __name__ == "__main__":
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="Process financials bronze data")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    # Call main with arguments explicitly passed
    main(args.snapshotdate)