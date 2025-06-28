import argparse
import os
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col, coalesce, lit
from datetime import datetime


def load_silver_features(snapshot_date_str, silver_base_dir, spark):
    """
    Load silver feature tables for given snapshot date
    """
    part_suffix = snapshot_date_str.replace('-', '_')

    # Define paths
    combined_path = f"{silver_base_dir}features_combined_1/silver_features_combined_1_{part_suffix}.parquet"
    financials_path = f"{silver_base_dir}features_financials/silver_features_financials_{part_suffix}.parquet"

    # Load tables
    df_combined = spark.read.parquet(combined_path)
    df_financials = spark.read.parquet(financials_path)

    print(f"✅ Loaded combined features: {df_combined.count()} rows")
    print(f"✅ Loaded financial features: {df_financials.count()} rows")

    return df_combined, df_financials


def join_feature_tables(df_combined, df_financials):
    """
    Join silver feature tables on Customer_ID and snapshot_date
    """
    # Inner join - only keep customers present in both tables
    df_joined = df_combined.join(
        df_financials,
        on=["Customer_ID", "snapshot_date"],
        how="inner"
    )

    print(f"🔗 After join: {df_joined.count()} rows")
    return df_joined


def clean_and_select_features(df_joined):
    """
    Clean and select final features for gold table
    """
    # Select key features - excluding PII and redundant columns
    feature_columns = [
        # Primary keys
        "Customer_ID",
        "snapshot_date",

        # Clickstream features (fe_1 to fe_20)
        # *[f"fe_{i}" for i in range(1, 21)],

        # Customer attributes (excluding PII)
        "Age",
        "age_group",
        "occupation_category",

        # Clickstream derived features
        # "total_clickstream_activity",
        # "avg_clickstream_activity", 
        # "is_high_activity_user",

        # Financial features
        "Annual_Income",
        "Monthly_Inhand_Salary",
        "Num_Bank_Accounts",
        "Num_Credit_Card",
        "Interest_Rate",
        "Num_of_Loan",
        "Delay_from_due_date",
        "Num_of_Delayed_Payment",
        "Changed_Credit_Limit",
        "Num_Credit_Inquiries",
        "Outstanding_Debt",
        "Credit_Utilization_Ratio",
        "Credit_History_Age",
        "Total_EMI_per_month",
        "Amount_invested_monthly",
        "Monthly_Balance",

        # Financial derived features
        "debt_to_income_ratio",
        "monthly_debt_service_ratio",
        "investment_rate",
        "credit_utilization_category",
        "financial_risk_score",
        "income_category",
        "debt_category"
    ]

    # Select only columns that exist in the dataframe
    existing_columns = df_joined.columns
    selected_columns = [col for col in feature_columns if col in existing_columns]

    print(f"📋 Selected {len(selected_columns)} features out of {len(feature_columns)} requested")
    print(f"📋 Missing columns: {set(feature_columns) - set(existing_columns)}")

    df_clean = df_joined.select(*selected_columns)

    # Basic data quality checks
    total_rows = df_clean.count()
    print(f"📊 Final feature table: {total_rows} customers")

    return df_clean


def process_gold_feature_store(snapshot_date_str, silver_base_dir, gold_feature_dir, spark):
    """
    Main processing function for gold feature store

    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
        silver_base_dir: Base directory for silver layer data
        gold_feature_dir: Output directory for gold feature store
        spark: Spark session

    Returns:
        Processed DataFrame with customer-level features
    """
    print(f"\n🚀 Processing Gold Feature Store for {snapshot_date_str}")

    # Load silver feature tables
    df_combined, df_financials = load_silver_features(snapshot_date_str, silver_base_dir, spark)

    # Join feature tables
    df_joined = join_feature_tables(df_combined, df_financials)

    # Clean and select features
    df_gold_features = clean_and_select_features(df_joined)

    # Save gold feature store
    part_suffix = snapshot_date_str.replace('-', '_')
    output_file = f"gold_feature_store_{part_suffix}.parquet"
    output_path = os.path.join(gold_feature_dir, output_file)

    # Create output directory if it doesn't exist
    os.makedirs(gold_feature_dir, exist_ok=True)

    # Save as parquet
    df_gold_features.write.mode("overwrite").parquet(output_path)
    print(f"📂 Saved gold feature store to: {output_path}")

    # Print final schema for verification
    print(f"\n📋 Final Schema ({len(df_gold_features.columns)} columns):")
    df_gold_features.printSchema()

    return df_gold_features


def main():
    """
    Main function to handle command line arguments and execute processing
    """
    parser = argparse.ArgumentParser(description='Process gold feature store')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--silver_base_dir', 
                       default='datamart/silver/', 
                       help='Silver layer base directory')
    parser.add_argument('--gold_feature_dir',
                       default='datamart/gold/feature_store/',
                       help='Gold feature store output directory')

    args = parser.parse_args()

    # Initialize Spark session
    spark = pyspark.sql.SparkSession.builder \
        .appName("GoldFeatureStore") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        # Process the gold feature store
        df_result = process_gold_feature_store(
            snapshot_date_str=args.snapshotdate,
            silver_base_dir=args.silver_base_dir,
            gold_feature_dir=args.gold_feature_dir,
            spark=spark
        )

        print("\n🎉 Gold feature store processing completed successfully!")

    except Exception as e:
        print(f"❌ Error processing gold feature store: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
