"""
Gold layer data processing utility functions
"""
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, coalesce, lit, 
    sum as sum_col, avg, min, max, 
    countDistinct, current_timestamp,
    abs as spark_abs
)

def create_gold_directories():
    """Create Gold layer data directory"""
    os.makedirs("datamart/gold", exist_ok=True)
    print("Created gold directory")

def process_gold_features(silver_dfs):
    """
    Process Silver layer data to create Gold layer feature table
    
    Args:
        silver_dfs: Dictionary containing all Silver layer DataFrames
        
    Returns:
        DataFrame: Processed Gold layer feature table
    """
    # Step 1: Create base customer table
    print("Creating Gold layer base customer table...")
    
    # Get all customer IDs from attributes table
    base_customers = silver_dfs['attributes'].select("Customer_ID")
    
    # Flag customers with clickstream data
    clickstream_customers_df = silver_dfs['clickstream_aggregated'].select("Customer_ID").distinct()
    
    # Create base customer table with clickstream availability flag
    base_customers_with_flag = base_customers.join(
        clickstream_customers_df,
        "Customer_ID",
        "left"
    ).withColumn(
        "has_clickstream_data",
        clickstream_customers_df.Customer_ID.isNotNull()
    )
    
    # Step 2: Merge customer attributes and financial data
    print("Merging customer attributes and financial data...")
    
    # Get customer attributes data (from silver_attributes table)
    attributes_df = silver_dfs['attributes'].select(
        "Customer_ID",
        col("age"),
        col("occupation"),
        col("is_valid_age")
    )
    
    # Get customer financial data (from silver_financials table)
    financials_df = silver_dfs['financials'].select(
        "Customer_ID",
        col("annual_income"),
        col("Monthly_Inhand_Salary"),
        col("Num_Bank_Accounts"),
        col("Num_Credit_Card"),
        col("Interest_Rate"),
        col("Credit_Utilization_Ratio"),
        col("credit_history_total_months"),
        col("outstanding_debt"),
        col("num_delayed_payments"),
        col("credit_mix"),
        col("payment_behavior_spent_level"),
        col("payment_behavior_value_level")
    )
    
    # Merge attributes and financial data
    customer_profile_df = base_customers_with_flag.join(
        attributes_df, 
        "Customer_ID", 
        "left"
    ).join(
        financials_df, 
        "Customer_ID", 
        "left"
    )
    
    # Step 3: Add clickstream data
    print("Adding clickstream data...")
    
    # Get clickstream aggregated data
    clickstream_agg_df = silver_dfs['clickstream_aggregated'].select(
        "Customer_ID",
        # Select mean, stddev and outlier count for each feature
        # Using only first 5 features as example
        *[col(f"fe_{i}_mean") for i in range(1, 6)],
        *[col(f"fe_{i}_stddev") for i in range(1, 6)],
        *[col(f"fe_{i}_outlier_count") for i in range(1, 6)],
        col("record_count")
    )
    
    # Left join clickstream data to customer profile
    customer_profile_with_clicks_df = customer_profile_df.join(
        clickstream_agg_df, 
        "Customer_ID", 
        "left"
    )
    
    # Handle missing clickstream features - fill with 0
    processed_df = customer_profile_with_clicks_df
    for feature in clickstream_agg_df.columns:
        if feature != "Customer_ID":
            processed_df = processed_df.withColumn(
                feature,
                coalesce(col(feature), lit(0))  # Use 0 if value is NULL
            )
    
    # Add metadata indicating feature source
    processed_df = processed_df.withColumn(
        "clickstream_features_imputed", 
        when(col("has_clickstream_data") == False, True).otherwise(False)
    )
    
    # Step 4: Add loan features and create labels
    print("Adding loan features and creating labels...")
    
    # Calculate loan summary statistics for each customer
    loan_summary = silver_dfs['loan'].groupBy("Customer_ID").agg(
        # Calculate total number of loans
        countDistinct("loan_id").alias("total_loans"),
        # Calculate number of defaulted loans
        sum_col(when(col("is_default") == True, 1).otherwise(0)).alias("num_defaulted_loans"),
        # Calculate average missed payments
        avg("consecutive_missed_payments").alias("avg_missed_payments"),
        # Calculate max consecutive missed payments
        max("consecutive_missed_payments").alias("max_missed_payments"),
        # Calculate average percent paid
        avg("percent_paid").alias("avg_percent_paid"),
        # Get maximum overdue amount
        max("overdue_amt").alias("max_overdue_amt"),
        # Get minimum account balance
        min("balance").alias("min_balance")
    )
    
    # Add loan summary statistics to customer profile
    gold_features_df = processed_df.join(
        loan_summary, 
        "Customer_ID", 
        "left"
    )
    
    # Create target variable (label) - has defaulted loan
    gold_features_df = gold_features_df.withColumn(
        "has_defaulted", 
        col("num_defaulted_loans") > 0
    )
    
    # Create comprehensive default risk score (value between 0-120)
    gold_features_df = gold_features_df.withColumn(
        "default_risk_score",
        (
            # Base score - set base score based on default status
            when(col("num_defaulted_loans") > 0, 50).otherwise(0) +
            
            # Default history factor (0-20 points)
            (col("num_defaulted_loans") * 5).cast("int") + 
            
            # Late payment behavior factor (0-15 points)
            (col("max_missed_payments") * 1.5).cast("int") + 
            
            # Debt burden factor (0-10 points)
            when(
                (col("outstanding_debt") / col("annual_income") * 100) > 50, 10
            ).when(
                (col("outstanding_debt") / col("annual_income") * 100) > 30, 6
            ).when(
                (col("outstanding_debt") / col("annual_income") * 100) > 10, 3
            ).otherwise(0) +
            
            # Credit rating factor (0-10 points)
            when(col("credit_mix") == "BAD", 10)
            .when(col("credit_mix") == "STANDARD", 5)
            .when(col("credit_mix") == "GOOD", 0)
            .otherwise(5) +
            
            # Credit history factor (0-5 points) - short history adds points, long history reduces
            when(col("credit_history_total_months") < 120, 5)  # Under 10 years
            .when(col("credit_history_total_months") < 240, 3)  # Under 20 years
            .otherwise(0) +
            
            # Payment behavior factor (0-5 points)
            when(col("payment_behavior_spent_level") == "HIGH", 3).otherwise(0) +
            when(col("payment_behavior_value_level") == "LARGE", 2).otherwise(0) +
            
            # Income factor (0-5 points) - low income adds points
            when(col("annual_income") < 50000, 5)
            .when(col("annual_income") < 100000, 3)
            .otherwise(0) +
            
            # Loan count factor (0-5 points)
            when(col("total_loans") > 3, 5)
            .when(col("total_loans") > 1, 3)
            .otherwise(0) +
            
            # Overdue amount factor (0-5 points)
            when(col("max_overdue_amt") > 5000, 5)
            .when(col("max_overdue_amt") > 1000, 3)
            .when(col("max_overdue_amt") > 0, 1)
            .otherwise(0)
            
        ).cast("int")  # Convert to integer
    )
    
    # Ensure score is within 0-120 range
    gold_features_df = gold_features_df.withColumn(
        "default_risk_score",
        when(col("default_risk_score") > 120, 120)  # If over 120, set to 120
        .when(col("default_risk_score") < 0, 0)     # If under 0, set to 0
        .otherwise(col("default_risk_score"))
    )
    
    # Add final processing timestamp
    gold_features_df = gold_features_df.withColumn(
        "gold_process_timestamp", 
        current_timestamp()
    )
    
    return gold_features_df

def save_gold_features(spark):
    """
    Load data from Silver layer, process and save as Gold layer feature table
    
    Args:
        spark: Spark session
        
    Returns:
        str: Output path
    """
    # Load all Silver layer tables
    silver_dfs = {
        "loan": spark.read.parquet("datamart/silver/silver_loan"),
        "attributes": spark.read.parquet("datamart/silver/silver_attributes"),
        "financials": spark.read.parquet("datamart/silver/silver_financials"),
        "clickstream_aggregated": spark.read.parquet("datamart/silver/silver_clickstream_aggregated")
    }
    
    # Process Gold layer features
    gold_features_df = process_gold_features(silver_dfs)
    
    # Save results
    output_path = "datamart/gold/gold_features"
    gold_features_df.write.mode("overwrite").parquet(output_path)
    
    print(f"Saved Gold layer feature table to: {output_path}")
    print(f"Feature table row count: {gold_features_df.count()}")
    print(f"Feature table column count: {len(gold_features_df.columns)}")
    
    # Analyze label distribution
    print("\nLabel distribution:")
    gold_features_df.groupBy("has_defaulted").count().show()
    
    # Risk score distribution
    print("\nRisk score distribution:")
    gold_features_df.select("default_risk_score").summary("min", "25%", "50%", "75%", "max").show()
    
    return output_path