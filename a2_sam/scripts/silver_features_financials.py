import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col, regexp_replace, when, trim, regexp_extract, lit, split, size, isnan, isnull
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def pre_clean_data(df):
    """
    Pre-clean data to remove known problematic values before any processing
    """
    print("🧹 Pre-cleaning problematic values...")
    
    # Get all string columns
    string_columns = [col_name for col_name, dtype in df.dtypes if dtype in ['string']]
    
    for col_name in string_columns:
        # Replace problematic values with None
        df = df.withColumn(
            col_name,
            when(
                (col(col_name).rlike("__.*__")) |  # Any __text__ pattern
                (col(col_name) == "__-333333333333333333333333333__") |  # Specific bad value
                (col(col_name).rlike(".*-333333333333.*")) |  # Any variant with -333333...
                (col(col_name) == "") |  # Empty strings
                (col(col_name).isNull()),  # Already null
                None
            ).otherwise(col(col_name))
        )
    
    print("✅ Pre-cleaning completed")
    return df


def clean_financial_data(df):
    """
    Clean and process financial bronze data
    """
    # Clean Annual_Income: remove underscores
    df = df.withColumn(
        "Annual_Income",
        regexp_replace(col("Annual_Income").cast(StringType()), r"_+", "").cast(FloatType())
    )
    
    # Clean Num_Bank_Accounts: int <100 (reasonable limit)
    df = df.withColumn(
        "Num_Bank_Accounts",
        when(col("Num_Bank_Accounts").cast(IntegerType()) < 100,
             col("Num_Bank_Accounts").cast(IntegerType()))
        .otherwise(None)
    )
    
    # Clean Num_Credit_Card: int <50 (reasonable limit)
    df = df.withColumn(
        "Num_Credit_Card",
        when(col("Num_Credit_Card").cast(IntegerType()) < 50,
             col("Num_Credit_Card").cast(IntegerType()))
        .otherwise(None)
    )
    
    # Clean Interest_Rate: percent 0-99
    df = df.withColumn(
        "Interest_Rate",
        when((col("Interest_Rate").cast(IntegerType()) >= 0) &
             (col("Interest_Rate").cast(IntegerType()) < 100),
             col("Interest_Rate").cast(IntegerType()))
        .otherwise(None)
    )
    
    # Clean Num_of_Loan: remove underscores first, then validate 0-19
    df = df.withColumn(
        "Num_of_Loan_tmp",
        regexp_replace(col("Num_of_Loan").cast(StringType()), r"_+", "")
    ).withColumn(
        "Num_of_Loan",
        when(
            (col("Num_of_Loan_tmp").cast(IntegerType()) >= 0) & 
            (col("Num_of_Loan_tmp").cast(IntegerType()) < 20),
            col("Num_of_Loan_tmp").cast(IntegerType())
        ).otherwise(None)
    ).drop("Num_of_Loan_tmp")

    # Clean Num_of_Delayed_Payment: remove underscores first, then validate 0-29
    df = df.withColumn(
        "Num_of_Delayed_Payment_tmp",
        regexp_replace(col("Num_of_Delayed_Payment").cast(StringType()), r"_+", "")
    ).withColumn(
        "Num_of_Delayed_Payment",
        when(
            (col("Num_of_Delayed_Payment_tmp").cast(IntegerType()) >= 0) & 
            (col("Num_of_Delayed_Payment_tmp").cast(IntegerType()) < 30),
            col("Num_of_Delayed_Payment_tmp").cast(IntegerType())
        ).otherwise(None)
    ).drop("Num_of_Delayed_Payment_tmp")

    # Clean Changed_Credit_Limit: strip underscores, null if empty
    df = df.withColumn(
        "Changed_Credit_Limit_tmp",
        regexp_replace(col("Changed_Credit_Limit"), r"_+", "")
    ).withColumn(
        "Changed_Credit_Limit",
        when(trim(col("Changed_Credit_Limit_tmp")) == "", None)
        .otherwise(col("Changed_Credit_Limit_tmp").cast(FloatType()))
    ).drop("Changed_Credit_Limit_tmp")
    
    # Clean Credit_Mix: null if blank/underscores
    df = df.withColumn(
        "Credit_Mix",
        when(trim(col("Credit_Mix")) == "", None)
        .when(col("Credit_Mix").rlike(r"^_+$"), None)
        .otherwise(col("Credit_Mix"))
    )
    
    # Clean Outstanding_Debt: strip underscores
    df = df.withColumn(
        "Outstanding_Debt",
        regexp_replace(col("Outstanding_Debt").cast(StringType()), r"_+", "").cast(FloatType())
    )
    
    # Clean Amount_invested_monthly: digits and dot only
    df = df.withColumn(
        "Amount_invested_monthly",
        regexp_replace(col("Amount_invested_monthly"), r"[^0-9.]", "").cast(FloatType())
    )
    
    # Clean Payment_Behaviour: replace specific invalid pattern
    df = df.withColumn(
        "Payment_Behaviour",
        when(col("Payment_Behaviour") == "!@9#%8", None).otherwise(col("Payment_Behaviour"))
    )
    
    # Cast core columns
    df = df.withColumn("Customer_ID", col("Customer_ID").cast(StringType()))
    df = df.withColumn("snapshot_date", col("snapshot_date").cast(DateType()))
    df = df.withColumn("Monthly_Inhand_Salary", col("Monthly_Inhand_Salary").cast(FloatType()))
    df = df.withColumn("Delay_from_due_date", col("Delay_from_due_date").cast(IntegerType()))
    df = df.withColumn("Num_Credit_Inquiries", col("Num_Credit_Inquiries").cast(IntegerType()))
    df = df.withColumn("Credit_Utilization_Ratio", col("Credit_Utilization_Ratio").cast(FloatType()))
    df = df.withColumn("Total_EMI_per_month", col("Total_EMI_per_month").cast(FloatType()))
    df = df.withColumn("Monthly_Balance", col("Monthly_Balance").cast(FloatType()))
    
    return df


def handle_outliers(df):
    """
    Handle outliers using IQR method for key financial metrics
    First clean problematic values, then process outliers
    """
    # Apply outlier removal to key financial columns
    outlier_columns = ["Annual_Income", "Outstanding_Debt", "Monthly_Inhand_Salary", 
                      "Total_EMI_per_month", "Amount_invested_monthly"]
    
    for col_name in outlier_columns:
        if col_name in df.columns:
            print(f"🔍 Processing outliers for {col_name}...")
            
            try:
                # First, create a clean numeric version of the column
                df = df.withColumn(
                    col_name + "_numeric",
                    when(
                        col(col_name).isNull(),
                        None
                    ).otherwise(
                        col(col_name).cast("double")  # Try to cast to double
                    )
                )
                
                # Check if we have any valid numeric values
                valid_count = df.filter(col(col_name + "_numeric").isNotNull()).count()
                total_count = df.count()
                
                print(f"   Valid numeric values: {valid_count}/{total_count} ({valid_count/total_count:.1%})")
                
                if valid_count > 10:  # Only process if we have enough valid data
                    # Calculate quantiles using cleaned numeric column
                    quantiles = df.select(col_name + "_numeric").filter(
                        col(col_name + "_numeric").isNotNull()
                    ).approxQuantile(col_name + "_numeric", [0.25, 0.75], 0.01)
                    
                    if len(quantiles) == 2:
                        Q1, Q3 = quantiles
                        IQR = Q3 - Q1
                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR
                        
                        print(f"   IQR bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
                        
                        # Count outliers before removal
                        outliers_count = df.filter(
                            (col(col_name + "_numeric").isNotNull()) &
                            ((col(col_name + "_numeric") < lower_bound) | 
                             (col(col_name + "_numeric") > upper_bound))
                        ).count()
                        
                        print(f"   Outliers to be removed: {outliers_count}")
                        
                        # Replace outliers with None, keep the cleaned data
                        df = df.withColumn(
                            col_name,
                            when(
                                (col(col_name + "_numeric").isNull()) |
                                (col(col_name + "_numeric") < lower_bound) | 
                                (col(col_name + "_numeric") > upper_bound),
                                None
                            ).otherwise(col(col_name + "_numeric"))
                        )
                        
                    else:
                        print(f"   ⚠️  Could not calculate quantiles for {col_name}")
                        # Just keep the cleaned numeric values
                        df = df.withColumn(col_name, col(col_name + "_numeric"))
                        
                else:
                    print(f"   ⚠️  Too few valid values for {col_name}, skipping outlier removal")
                    # Just keep the cleaned data
                    df = df.withColumn(col_name, col(col_name + "_numeric"))
                
                # Clean up temporary column
                df = df.drop(col_name + "_numeric")
                
            except Exception as e:
                print(f"   ❌ Error processing {col_name}: {str(e)}")
                print(f"   Setting problematic values to None")
                
                # Fallback: just clean problematic values without outlier detection
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNull(),
                        None
                    ).otherwise(
                        col(col_name).cast("double")
                    )
                )
    
    return df


def add_financial_features(df):
    """
    Add financial feature engineering
    """
    # Credit History Age processing
    df = df.withColumn(
        "credit_history_years",
        regexp_extract(col("Credit_History_Age"), r"(\d+)\s+Years", 1).cast(IntegerType())
    ).withColumn(
        "credit_history_months", 
        regexp_extract(col("Credit_History_Age"), r"and\s+(\d+)\s+Months", 1).cast(IntegerType())
    ).withColumn(
        "credit_history_total_months",
        (col("credit_history_years") * 12 + col("credit_history_months")).cast(IntegerType())
    )
    
    # Debt-to-Income Ratio
    df = df.withColumn(
        "debt_to_income_ratio",
        (col("Outstanding_Debt") / (col("Annual_Income") + lit(1e-6))).cast(FloatType())
    )
    
    # Monthly debt service ratio
    df = df.withColumn(
        "monthly_debt_service_ratio", 
        (col("Total_EMI_per_month") / (col("Monthly_Inhand_Salary") + lit(1e-6))).cast(FloatType())
    )
    
    # Investment rate (what percentage of income is invested)
    df = df.withColumn(
        "investment_rate",
        (col("Amount_invested_monthly") / (col("Monthly_Inhand_Salary") + lit(1e-6))).cast(FloatType())
    )
    
    # Credit utilization category
    df = df.withColumn(
        "credit_utilization_category",
        when(col("Credit_Utilization_Ratio") < 0.1, lit("Low"))
        .when((col("Credit_Utilization_Ratio") >= 0.1) & (col("Credit_Utilization_Ratio") < 0.3), lit("Medium"))
        .when((col("Credit_Utilization_Ratio") >= 0.3) & (col("Credit_Utilization_Ratio") < 0.7), lit("High"))
        .when(col("Credit_Utilization_Ratio") >= 0.7, lit("Very_High"))
        .otherwise(lit("Unknown"))
        .cast(StringType())
    )
    
    # Credit Mix encoding
    df = df.withColumn(
        "credit_mix_score",
        when(col("Credit_Mix") == "Good", lit(3))
        .when(col("Credit_Mix") == "Standard", lit(2))
        .when(col("Credit_Mix") == "Bad", lit(1))
        .otherwise(lit(None))
        .cast(IntegerType())
    )
    
    # Payment behavior parsing
    df = df.withColumn(
        "payment_behavior_spending",
        when(col("Payment_Behaviour").contains("Low_spent"), lit("Low"))
        .when(col("Payment_Behaviour").contains("High_spent"), lit("High"))
        .otherwise(lit("Unknown"))
        .cast(StringType())
    ).withColumn(
        "payment_behavior_value",
        when(col("Payment_Behaviour").contains("Small_value"), lit("Small"))
        .when(col("Payment_Behaviour").contains("Medium_value"), lit("Medium"))
        .when(col("Payment_Behaviour").contains("Large_value"), lit("Large"))
        .otherwise(lit("Unknown"))
        .cast(StringType())
    )
    
    # Financial risk score (simple scoring based on multiple factors)
    df = df.withColumn(
        "financial_risk_score",
        (
            when(col("debt_to_income_ratio") > 0.5, lit(2)).otherwise(lit(0)) +
            when(col("credit_utilization_category") == "Very_High", lit(2))
            .when(col("credit_utilization_category") == "High", lit(1))
            .otherwise(lit(0)) +
            when(col("Num_of_Delayed_Payment") > 5, lit(2))
            .when(col("Num_of_Delayed_Payment") > 0, lit(1))
            .otherwise(lit(0)) +
            when(col("credit_mix_score") == 1, lit(2))
            .when(col("credit_mix_score") == 2, lit(1))
            .otherwise(lit(0))
        ).cast(IntegerType())
    )
    
    return df


def process_silver_features_financials(snapshot_date_str, 
                                     bronze_financials_dir,
                                     silver_financials_dir, 
                                     spark):
    """
    Process bronze financials data into silver financials table
    
    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
        bronze_financials_dir: Directory containing bronze financials data
        silver_financials_dir: Output directory for silver financials data
        spark: Spark session
    
    Returns:
        Processed DataFrame
    """
    # Parse snapshot date
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    part_suffix = snapshot_date_str.replace('-', '_')
    
    # Load bronze financials data
    financials_file = f"bronze_features_financials_{part_suffix}.csv"
    financials_path = os.path.join(bronze_financials_dir, financials_file)
    
    # Check if file exists
    if not os.path.exists(financials_path):
        raise FileNotFoundError(f"Bronze financials file not found: {financials_path}")
        
    df_financials = spark.read.csv(financials_path, header=True, inferSchema=True)
    print(f'Loaded financials from: {financials_path}, row count: {df_financials.count()}')
    
    # Clean the data in the correct order
    print("🔄 Starting data cleaning pipeline...")
    
    # Step 1: Pre-clean problematic values
    df_pre_clean = pre_clean_data(df_financials)
    
    # Step 2: Clean financial data
    print("🔄 Cleaning financial data...")
    df_clean = clean_financial_data(df_pre_clean)
    
    # Step 3: Handle outliers
    print("🔄 Handling outliers...")
    df_no_outliers = handle_outliers(df_clean)
    
    # Step 4: Add feature engineering
    print("🔄 Adding financial features...")
    df_final = add_financial_features(df_no_outliers)
    
    print("✅ Data processing pipeline completed")
    
    # Create output directory if it doesn't exist
    os.makedirs(silver_financials_dir, exist_ok=True)
    
    # Save silver financials table
    output_file = f"silver_features_financials_{part_suffix}.parquet"
    output_path = os.path.join(silver_financials_dir, output_file)
    df_final.write.mode("overwrite").parquet(output_path)
    print(f'✅ Saved to: {output_path}')
    
    return df_final


def main():
    """
    Main function to handle command line arguments and execute processing
    """
    parser = argparse.ArgumentParser(description='Process silver features financials table')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--bronze_financials_dir',
                       default='datamart/bronze/features_financials/', 
                       help='Bronze financials data directory')
    parser.add_argument('--silver_financials_dir',
                       default='datamart/silver/features_financials/',
                       help='Silver financials output directory')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = pyspark.sql.SparkSession.builder \
        .appName("SilverFeaturesFinancials") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Process the data
        df_result = process_silver_features_financials(
            snapshot_date_str=args.snapshotdate,
            bronze_financials_dir=args.bronze_financials_dir,
            silver_financials_dir=args.silver_financials_dir,
            spark=spark
        )
        
        print("🎉 Silver features financials processing completed successfully!")
        print(f"📊 Final row count: {df_result.count()}")
        print(f"📋 Final schema:")
        df_result.printSchema()
        
        # Show a sample of the final data
        print(f"📝 Sample of processed data:")
        df_result.select("Customer_ID", "Annual_Income", "debt_to_income_ratio", "financial_risk_score").show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Error processing silver features financials: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()