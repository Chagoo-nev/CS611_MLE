"""
Bronze layer data processing utility functions
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, col, to_date, when

def create_bronze_directories():
    """Create Bronze layer data directory"""
    os.makedirs("datamart/bronze", exist_ok=True)
    print("Created bronze directory")

def process_bronze_dataframe(spark, file_path, source_name, date_columns=None):
    """
    Process raw CSV to Bronze layer DataFrame
    
    Args:
        spark: Spark session
        file_path: CSV file path
        source_name: Data source name
        date_columns: List of column names to convert to date type
        
    Returns:
        DataFrame: Processed Bronze layer DataFrame
    """
    try:
        print(f"Processing {source_name} from {file_path}")
        
        # Check if file exists
        import os
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return None
        
        # Read raw CSV file
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        # Check if read successful
        if df is None or df.count() == 0:
            print(f"Error: No data read from {file_path} or DataFrame is empty")
            return None
        
        # Add Bronze layer metadata
        df = df.withColumn("bronze_ingest_timestamp", current_timestamp()) \
               .withColumn("bronze_source_file", lit(os.path.basename(file_path)))
        
        # Convert date columns (if specified)
        if date_columns:
            for date_col in date_columns:
                if date_col in df.columns:
                    # Try multiple date formats
                    df = df.withColumn(date_col, 
                        when(
                            to_date(col(date_col), "yyyy-MM-dd").isNotNull(), 
                            to_date(col(date_col), "yyyy-MM-dd")
                        ).otherwise(
                            when(
                                to_date(col(date_col), "d/MM/yyyy").isNotNull(),
                                to_date(col(date_col), "d/MM/yyyy")
                            ).otherwise(to_date(col(date_col)))
                        )
                    )
        
        return df
    except Exception as e:
        print(f"Error processing {source_name}: {e}")
        # Raise exception in debug mode, otherwise return None
        # raise
        return None

def save_bronze_table(df, table_name):
    """
    Save DataFrame as Bronze layer table
    
    Args:
        df: DataFrame to save
        table_name: Table name (without bronze_ prefix)
        
    Returns:
        str: Saved table path, None if save fails
    """
    try:
        # Check if DataFrame is None
        if df is None:
            print(f"Error: Cannot save {table_name} - DataFrame is None")
            return None
        
        output_path = f"datamart/bronze/bronze_{table_name}"
        
        # Save as Parquet format
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"Saved bronze table to {output_path}")
        return output_path
    except Exception as e:
        print(f"Error saving {table_name}: {e}")
        return None