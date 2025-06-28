import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, ceil, add_months, datediff, lit, max as spark_max, coalesce
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def clean_lms_data(df):
    """
    Clean and validate LMS bronze data
    """
    # Define column type mapping
    column_type_map = {
        "loan_id": StringType(),
        "Customer_ID": StringType(),
        "loan_start_date": DateType(),
        "tenure": IntegerType(),
        "installment_num": IntegerType(),
        "loan_amt": FloatType(),
        "due_amt": FloatType(),
        "paid_amt": FloatType(),
        "overdue_amt": FloatType(),
        "balance": FloatType(),
        "snapshot_date": DateType(),
    }

    # Cast columns to proper data types
    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))
    
    # Data validation: ensure non-negative financial amounts
    financial_cols = ["loan_amt", "due_amt", "paid_amt", "overdue_amt", "balance"]
    for col_name in financial_cols:
        df = df.withColumn(
            col_name,
            when(col(col_name) < 0, lit(0)).otherwise(col(col_name))
        )
    
    # Validate installment_num and tenure are non-negative
    df = df.withColumn(
        "installment_num",
        when(col("installment_num") < 0, lit(0)).otherwise(col("installment_num"))
    )
    df = df.withColumn(
        "tenure", 
        when(col("tenure") <= 0, lit(1)).otherwise(col("tenure"))
    )
    
    return df


def add_derived_features(df):
    """
    Add derived features from LMS data
    """
    # Month on Book (MOB) - same as installment_num
    df = df.withColumn("mob", col("installment_num").cast(IntegerType()))
    
    # Calculate installments missed (avoid division by zero)
    df = df.withColumn(
        "installments_missed", 
        when(col("due_amt") > 0, 
             ceil(col("overdue_amt") / col("due_amt")))
        .otherwise(lit(0))
        .cast(IntegerType())
    ).fillna(0)
    
    # Calculate first missed date (when customer first became delinquent)
    df = df.withColumn(
        "first_missed_date", 
        when(col("installments_missed") > 0, 
             add_months(col("snapshot_date"), -1 * col("installments_missed")))
        .cast(DateType())
    )
    
    # Days Past Due (DPD)
    df = df.withColumn(
        "dpd", 
        when(col("overdue_amt") > 0.0, 
             datediff(col("snapshot_date"), col("first_missed_date")))
        .otherwise(lit(0))
        .cast(IntegerType())
    )
    
    # Payment ratio (what percentage of due amount was paid)
    # 修正版本：
    df = df.withColumn(
        "payment_ratio",
        when(col("due_amt") > 0,
            col("paid_amt") / col("due_amt"))
        .when(col("due_amt") == 0,
            lit(None))  # 没有应还金额时，payment_ratio为NULL
        .cast(FloatType())
    )
    
    # Loan utilization (how much of original loan is still outstanding)
    df = df.withColumn(
        "loan_utilization",
        when(col("loan_amt") > 0,
             col("balance") / col("loan_amt"))
        .otherwise(lit(0.0))
        .cast(FloatType())
    )
    
    # Loan progression (percentage of tenure completed)
    df = df.withColumn(
        "loan_progression",
        when(col("tenure") > 0,
             col("installment_num") / col("tenure"))
        .otherwise(lit(0.0))
        .cast(FloatType())
    )
    
    return df


def generate_risk_labels(df):
    """
    Generate risk-based labels for ML modeling
    IMPORTANT: Only use current and historical data to avoid data leakage
    """
    # Current delinquency status
    df = df.withColumn(
        "is_current_delinquent",
        when(col("overdue_amt") > 0, lit(1)).otherwise(lit(0)).cast(IntegerType())
    )
    
    # DPD-based risk categories (standard industry practice)
    df = df.withColumn(
        "dpd_bucket",
        when(col("dpd") == 0, lit("Current"))
        .when((col("dpd") >= 1) & (col("dpd") <= 30), lit("DPD_1_30"))
        .when((col("dpd") >= 31) & (col("dpd") <= 60), lit("DPD_31_60"))
        .when((col("dpd") >= 61) & (col("dpd") <= 90), lit("DPD_61_90"))
        .when(col("dpd") > 90, lit("DPD_90_Plus"))
        .otherwise(lit("Unknown"))
        .cast(StringType())
    )
    
    # Binary risk label (commonly used for ML)
    df = df.withColumn(
        "is_high_risk",
        when(col("dpd") >= 30, lit(1)).otherwise(lit(0)).cast(IntegerType())
    )
    
    # Severe delinquency (90+ days)
    df = df.withColumn(
        "is_severely_delinquent", 
        when(col("dpd") >= 90, lit(1)).otherwise(lit(0)).cast(IntegerType())
    )
    

    df = df.withColumn(
    "payment_performance",
    when(col("payment_ratio").isNull(), lit("Not_Due"))  #  新增：还未到期
    .when(col("payment_ratio") >= 1.0, lit("Excellent"))
    .when((col("payment_ratio") >= 0.8) & (col("payment_ratio") < 1.0), lit("Good"))
    .when((col("payment_ratio") >= 0.5) & (col("payment_ratio") < 0.8), lit("Fair"))
    .when((col("payment_ratio") > 0) & (col("payment_ratio") < 0.5), lit("Poor"))
    .when(col("payment_ratio") == 0, lit("No_Payment"))
    .otherwise(lit("Unknown"))
    .cast(StringType())
)
    
    # Early warning indicator (multiple missed payments but not yet severe)
    df = df.withColumn(
        "early_warning_flag",
        when((col("installments_missed") >= 2) & (col("dpd") < 90), lit(1))
        .otherwise(lit(0))
        .cast(IntegerType())
    )
    
    return df


def process_silver_label_store(snapshot_date_str, bronze_lms_dir, silver_label_dir, spark):
    """
    Process bronze LMS data into silver label store
    
    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
        bronze_lms_dir: Directory containing bronze LMS data
        silver_label_dir: Output directory for silver label data
        spark: Spark session
    
    Returns:
        Processed DataFrame with labels
    """
    # Parse snapshot date
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    part_suffix = snapshot_date_str.replace('-', '_')
    
    # Load bronze LMS data
    lms_file = f"bronze_loan_daily_{part_suffix}.csv"
    lms_path = os.path.join(bronze_lms_dir, lms_file)
    
    # Check if file exists
    if not os.path.exists(lms_path):
        raise FileNotFoundError(f"Bronze LMS file not found: {lms_path}")
        
    df_lms = spark.read.csv(lms_path, header=True, inferSchema=True)
    print(f'Loaded LMS data from: {lms_path}, row count: {df_lms.count()}')
    
    # Clean the data
    df_clean = clean_lms_data(df_lms)
    
    # Add derived features
    df_enhanced = add_derived_features(df_clean)
    
    # Generate risk labels
    df_labeled = generate_risk_labels(df_enhanced)
    
    # Save silver label store
    output_file = f"silver_label_store_{part_suffix}.parquet"
    output_path = os.path.join(silver_label_dir, output_file)
    df_labeled.write.mode("overwrite").parquet(output_path)
    print(f'Saved to: {output_path}')
    
    # Print label distribution for validation
    print("\n📊 Label Distribution:")
    df_labeled.groupBy("dpd_bucket").count().orderBy("count", ascending=False).show()
    df_labeled.groupBy("is_high_risk").count().show()
    
    return df_labeled


def main():
    """
    Main function to handle command line arguments and execute processing
    """
    parser = argparse.ArgumentParser(description='Process silver label store')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--bronze_lms_dir',
                       default='datamart/bronze/lms/', 
                       help='Bronze LMS data directory')
    parser.add_argument('--silver_label_dir',
                       default='datamart/silver/label_store/',
                       help='Silver label store output directory')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = pyspark.sql.SparkSession.builder \
        .appName("SilverLabelStore") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Process the data
        df_result = process_silver_label_store(
            snapshot_date_str=args.snapshotdate,
            bronze_lms_dir=args.bronze_lms_dir,
            silver_label_dir=args.silver_label_dir,
            spark=spark
        )
        
        print("Silver label store processing completed successfully!")
        print(f"Final schema:")
        df_result.printSchema()
        
    except Exception as e:
        print(f"Error processing silver label store: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()