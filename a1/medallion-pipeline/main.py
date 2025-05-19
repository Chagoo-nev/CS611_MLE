"""
Medallion Architecture Data Pipeline for Loan Default Prediction
"""
import os
import argparse
from utils.spark_utils import create_spark_session, stop_spark_session
from utils.bronze_utils import create_bronze_directories, process_bronze_dataframe, save_bronze_table
from utils.silver_utils import (
   create_silver_directories, save_silver_loan, save_silver_attributes, 
   save_silver_financials, save_silver_clickstream
)
from utils.gold_utils import create_gold_directories, save_gold_features

def create_datamart_directories():
   """Create datamart directory structure"""
   os.makedirs("datamart/bronze", exist_ok=True)
   os.makedirs("datamart/silver", exist_ok=True)
   os.makedirs("datamart/gold", exist_ok=True)
   print("Created datamart directories")

def process_bronze_layer(spark):
   """Process Bronze layer data"""
   print("Starting Bronze layer processing...")
   
   # 1. Process loan data
   loan_df = process_bronze_dataframe(
       spark=spark,
       file_path="data/lms_loan_daily.csv",
       source_name="loan_daily",
       date_columns=["loan_start_date", "snapshot_date"]
   )
   save_bronze_table(loan_df, "loan_daily")
   
   # 2. Process financial data
   financials_df = process_bronze_dataframe(
       spark=spark,
       file_path="data/features_financials.csv",
       source_name="financials",
       date_columns=["snapshot_date"]
   )
   save_bronze_table(financials_df, "financials")
   
   # 3. Process attribute data
   attributes_df = process_bronze_dataframe(
       spark=spark,
       file_path="data/features_attributes.csv",
       source_name="attributes",
       date_columns=["snapshot_date"]
   )
   save_bronze_table(attributes_df, "attributes")
   
   # 4. Process clickstream data
   clickstream_df = process_bronze_dataframe(
       spark=spark,
       file_path="data/feature_clickstream.csv",
       source_name="clickstream",
       date_columns=["snapshot_date"]
   )
   save_bronze_table(clickstream_df, "clickstream")
   
   print("Bronze layer processing completed")

def process_silver_layer(spark):
   """Process Silver layer data"""
   print("Starting Silver layer processing...")
   
   # 1. Process loan data
   save_silver_loan(spark)
   
   # 2. Process customer attribute data
   save_silver_attributes(spark)
   
   # 3. Process financial data
   save_silver_financials(spark)
   
   # 4. Process clickstream data
   save_silver_clickstream(spark)
   
   print("Silver layer processing completed")

def process_gold_layer(spark):
   """Process Gold layer data"""
   print("Starting Gold layer processing...")
   
   # Create and save Gold layer feature table
   save_gold_features(spark)
   
   print("Gold layer processing completed")

def main():
   """Main function to run the data pipeline"""
   # Set up command line argument parsing
   parser = argparse.ArgumentParser(description='Medallion Architecture Data Pipeline')
   parser.add_argument('--layer', type=str, default='all', 
                       choices=['all', 'bronze', 'silver', 'gold'],
                       help='Specify layer to process: bronze, silver, gold, or all (default)')
   parser.add_argument('--table', type=str, default='all',
                       choices=['all', 'loan', 'attributes', 'financials', 'clickstream', 'features'],
                       help='Specify table to process: loan, attributes, financials, clickstream, features, or all (default)')
   parser.add_argument('--create-dirs', action='store_true',
                       help='Only create directory structure')
   
   args = parser.parse_args()
   
   # Create Spark session
   spark = create_spark_session()
   print("Spark session created")
   
   try:
       # Create datamart directories
       if args.create_dirs or args.layer != 'all':
           create_datamart_directories()
           if args.create_dirs:
               print("Directories created, exiting program")
               return
       
       # Execute processing based on specified layer
       if args.layer == 'all' or args.layer == 'bronze':
           process_bronze_layer(spark)
       
       if args.layer == 'all' or args.layer == 'silver':
           if args.table == 'all':
               process_silver_layer(spark)
           elif args.table == 'loan':
               save_silver_loan(spark)
           elif args.table == 'attributes':
               save_silver_attributes(spark)
           elif args.table == 'financials':
               save_silver_financials(spark)
           elif args.table == 'clickstream':
               save_silver_clickstream(spark)
       
       if args.layer == 'all' or args.layer == 'gold':
           if args.table == 'all' or args.table == 'features':
               process_gold_layer(spark)
       
       print("Data pipeline completed successfully")
   except Exception as e:
       print(f"Error in data pipeline: {e}")
       raise
   finally:
       # Ensure Spark session is stopped
       stop_spark_session(spark)

if __name__ == "__main__":
   main() 