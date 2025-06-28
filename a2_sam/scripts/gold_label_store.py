import argparse
import os
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col, max as spark_max, min as spark_min, avg, sum as spark_sum, count, when, coalesce, lit
from pyspark.sql.types import IntegerType, FloatType
from datetime import datetime


def load_silver_labels(snapshot_date_str, silver_base_dir, spark):
    """
    Load silver label data for given snapshot date
    """
    part_suffix = snapshot_date_str.replace('-', '_')
    
    # Define path
    label_path = f"{silver_base_dir}label_store/silver_label_store_{part_suffix}.parquet"
    
    # Load table
    df_labels = spark.read.parquet(label_path)
    
    print(f"✅ Loaded silver labels: {df_labels.count()} records")
    print(f"📊 Unique customers: {df_labels.select('Customer_ID').distinct().count()}")
    
    return df_labels


def aggregate_customer_risk_metrics(df_labels):
    """
    Aggregate loan-level data to customer-level risk metrics
    
    Based on EDA insights:
    - Most customers never recover from delinquency 
    - Risk increases with MOB
    - Focus on maximum risk exposure (worst-case scenario)
    """
    
    print("🔄 Aggregating customer-level risk metrics...")
    
    # Customer-level aggregations
    df_customer_agg = df_labels.groupBy("Customer_ID", "snapshot_date").agg(
        
        # === Primary Risk Indicators ===
        spark_max("dpd").alias("max_dpd"),                              # Worst DPD ever
        spark_max("is_high_risk").alias("is_high_risk"),                # Any high risk loan
        spark_max("is_severely_delinquent").alias("is_severely_delinquent"), # Any severe delinquency
        spark_max("is_current_delinquent").alias("is_current_delinquent"),   # Any current delinquency
        spark_max("early_warning_flag").alias("has_early_warning"),     # Any early warning
        
        # === Payment Performance Aggregation ===
        avg("payment_ratio").alias("avg_payment_ratio"),               # Average payment performance
        spark_min("payment_ratio").alias("min_payment_ratio"),         # Worst payment month
        
        # === Loan Portfolio Metrics ===
        count("loan_id").alias("total_observations"),                  # Total loan-months observed
        spark_sum("loan_amt").alias("total_loan_exposure"),            # Total loan amount across all loans
        avg("loan_amt").alias("avg_loan_amount"),                      # Average loan size
        spark_max("mob").alias("max_mob_observed"),                    # Maximum months on books
        
        # === Delinquency Depth Analysis ===
        spark_sum(when(col("dpd") > 0, 1).otherwise(0)).alias("months_with_delinquency"), # Count delinquent months
        spark_sum(when(col("dpd") > 30, 1).otherwise(0)).alias("months_dpd_30_plus"),     # Count DPD 30+ months
        spark_sum(when(col("dpd") > 90, 1).otherwise(0)).alias("months_dpd_90_plus"),     # Count DPD 90+ months
        
        # === Financial Stress Indicators ===
        spark_sum("overdue_amt").alias("total_overdue_amount"),        # Total overdue amount
        spark_sum("installments_missed").alias("total_missed_installments"), # Total missed payments
        avg("loan_utilization").alias("avg_loan_utilization"),         # Average loan utilization
        
        # === Payment Behavior Consistency ===
        F.stddev("payment_ratio").alias("payment_ratio_volatility"),   # Payment consistency
        
        # === Timeline Metrics ===
        spark_min("loan_start_date").alias("first_loan_date"),         # First loan origination
        spark_max("loan_start_date").alias("latest_loan_date"),        # Most recent loan
        
    ).orderBy("Customer_ID")
    
    print(f"📊 Customer aggregation complete: {df_customer_agg.count()} customers")
    return df_customer_agg


def create_derived_risk_features(df_customer_agg):
    """
    Create derived risk features based on aggregated metrics
    """
    
    print("⚙️ Creating derived risk features...")
    
    df_enhanced = df_customer_agg.withColumn(
        # Delinquency rate (% of months with issues)
        "delinquency_rate", 
        col("months_with_delinquency") / col("total_observations")
    ).withColumn(
        # Severe delinquency rate  
        "severe_delinquency_rate",
        col("months_dpd_90_plus") / col("total_observations")
    ).withColumn(
        # Risk progression indicator (early to severe)
        "risk_progression_score",
        when(col("max_dpd") == 0, 0)                                    # No risk
        .when(col("max_dpd") <= 30, 1)                                  # Minor risk
        .when(col("max_dpd") <= 60, 2)                                  # Moderate risk  
        .when(col("max_dpd") <= 90, 3)                                  # High risk
        .otherwise(4)                                                   # Severe risk
    ).withColumn(
        # Payment consistency score (lower = more consistent)
        "payment_consistency_score",
        coalesce(col("payment_ratio_volatility"), lit(0.0))
    ).withColumn(
        # === EDA-Based Weighted Risk Score ===
        "dpd_score",
        when(col("max_dpd") == 0, 0.0)                     # No risk: 0 points
        .when(col("max_dpd") <= 30, 0.2)                   # Minor: 0.2 points  
        .when(col("max_dpd") <= 60, 0.4)                   # Moderate: 0.4 points
        .when(col("max_dpd") <= 90, 0.7)                   # High: 0.7 points
        .otherwise(1.0)                                    # Severe (90+): 1.0 points
    ).withColumn(
        "mob_risk_score", 
        when(col("max_mob_observed") <= 3, 0.1)            # Early stage: low risk
        .when(col("max_mob_observed") <= 6, 0.3)           # Mid stage: medium risk  
        .when(col("max_mob_observed") <= 10, 0.5)          # Mature: high risk
        .otherwise(0.5)
    ).withColumn(
        "payment_score",
        when(col("min_payment_ratio").isNull(), 0.0)       # No payment due yet
        .when(col("min_payment_ratio") >= 0.95, 0.0)       # Excellent payment: 0 risk
        .when(col("min_payment_ratio") >= 0.8, 0.2)        # Good payment: low risk
        .when(col("min_payment_ratio") >= 0.5, 0.5)        # Fair payment: medium risk
        .when(col("min_payment_ratio") > 0, 0.8)           # Poor payment: high risk
        .otherwise(1.0)                                    # No payment: max risk
    ).withColumn(
        "multiple_risk_flags_score",
        (col("is_high_risk") * 0.4 +                       # Base high risk: 40%
         col("is_severely_delinquent") * 0.3 +             # Severe delinquency: 30%
         col("has_early_warning") * 0.2 +                  # Early warning: 20%
         col("is_current_delinquent") * 0.1)               # Current delinquent: 10%
    ).withColumn(
        # === Weighted Risk Score (0-1 scale) ===
        "weighted_risk_score",
        (col("dpd_score") * 0.35 +                         # DPD: 35% weight (most important)
         col("payment_score") * 0.30 +                     # Payment: 30% weight  
         col("multiple_risk_flags_score") * 0.25 +         # Risk flags: 25% weight
         col("mob_risk_score") * 0.10)                     # MOB: 10% weight
    ).withColumn(
        # === Final Binary Target with Threshold ===
        "target",
        when(col("weighted_risk_score") >= 0.5, 1).otherwise(0)  # 0.5 threshold
    ).withColumn(
        # === Alternative: Probability-based Target ===
        "target_prob_based", 
        when(col("weighted_risk_score") >= 0.3, 1).otherwise(0)  # Lower threshold option
    )
    
    return df_enhanced


def validate_gold_labels(df_gold_labels):
    """
    Validate the quality of gold label aggregation
    """
    
    print("\n🔍 Validating Gold Label Quality...")
    
    total_customers = df_gold_labels.count()
    high_risk_customers = df_gold_labels.filter(col("target") == 1).count()
    
    print(f"📊 Total customers: {total_customers}")
    print(f"📊 High risk customers (target=1): {high_risk_customers} ({high_risk_customers/total_customers:.1%})")
    print(f"📊 Low risk customers (target=0): {total_customers - high_risk_customers} ({(total_customers - high_risk_customers)/total_customers:.1%})")
    
    # Check for data quality issues
    null_checks = [
        ("max_dpd", df_gold_labels.filter(col("max_dpd").isNull()).count()),
        ("avg_payment_ratio", df_gold_labels.filter(col("avg_payment_ratio").isNull()).count()),
        ("total_observations", df_gold_labels.filter(col("total_observations") == 0).count())
    ]
    
    print("\n🔍 Data Quality Checks:")
    for field, null_count in null_checks:
        if null_count > 0:
            print(f"⚠️  {field}: {null_count} null/zero values")
        else:
            print(f"✅ {field}: No issues")
    
    # Show target distribution
    print("\n📈 Binary Classification Target Distribution:")
    df_gold_labels.groupBy("target").count().orderBy("target").show()
    
    return True


def process_gold_label_store(snapshot_date_str, silver_base_dir, gold_label_dir, spark):
    """
    Main processing function for gold label store
    
    Aggregates loan-level silver labels to customer-level risk metrics
    
    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
        silver_base_dir: Base directory for silver layer data
        gold_label_dir: Output directory for gold label store
        spark: Spark session
    
    Returns:
        Processed DataFrame with customer-level labels
    """
    print(f"\n🚀 Processing Gold Label Store for {snapshot_date_str}")
    
    # Load silver labels
    df_silver_labels = load_silver_labels(snapshot_date_str, silver_base_dir, spark)
    
    # Aggregate to customer level
    df_customer_agg = aggregate_customer_risk_metrics(df_silver_labels)
    
    # Create derived features
    df_gold_labels = create_derived_risk_features(df_customer_agg)
    
    # Validate results
    validate_gold_labels(df_gold_labels)
    
    # Save gold label store
    part_suffix = snapshot_date_str.replace('-', '_')
    output_file = f"gold_label_store_{part_suffix}.parquet"
    output_path = os.path.join(gold_label_dir, output_file)
    
    # Create output directory if it doesn't exist
    os.makedirs(gold_label_dir, exist_ok=True)
    
    # Save as parquet
    df_gold_labels.write.mode("overwrite").parquet(output_path)
    print(f"💾 Saved gold label store to: {output_path}")
    
    # Print final schema for verification
    print(f"\n📋 Final Schema ({len(df_gold_labels.columns)} columns):")
    df_gold_labels.printSchema()
    
    return df_gold_labels


def main():
    """
    Main function to handle command line arguments and execute processing
    """
    parser = argparse.ArgumentParser(description='Process gold label store')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--silver_base_dir', 
                       default='datamart/silver/', 
                       help='Silver layer base directory')
    parser.add_argument('--gold_label_dir',
                       default='datamart/gold/label_store/',
                       help='Gold label store output directory')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = pyspark.sql.SparkSession.builder \
        .appName("GoldLabelStore") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Process the gold label store
        df_result = process_gold_label_store(
            snapshot_date_str=args.snapshotdate,
            silver_base_dir=args.silver_base_dir,
            gold_label_dir=args.gold_label_dir,
            spark=spark
        )
        
        print("\n🎉 Gold label store processing completed successfully!")
        
    except Exception as e:
        print(f"❌ Error processing gold label store: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()