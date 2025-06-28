import argparse
import os
import pyspark
import utils.data_processing_bronze_features

# to call this script: python bronze_features_clickstream.py --snapshotdate "2023-01-01"

def main(snapshotdate):
    print('\n\n---starting clickstream bronze job---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("bronze_clickstream") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # 创建bronze目录
    bronze_clickstream_directory = "datamart/bronze/features_clickstream/"
    if not os.path.exists(bronze_clickstream_directory):
        os.makedirs(bronze_clickstream_directory)

    # 运行数据处理
    utils.data_processing_bronze_features.process_bronze_clickstream(
        snapshotdate, bronze_clickstream_directory, spark
    )
    
    # end spark session
    spark.stop()
    print('\n\n---completed clickstream bronze job---\n\n')

if __name__ == "__main__":
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="Process clickstream bronze data")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    # Call main with arguments explicitly passed
    main(args.snapshotdate)