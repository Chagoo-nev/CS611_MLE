"""
Silver layer data processing utility functions
"""
# Standard library imports
import os
from datetime import datetime

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    # Column operations
    col, lit, 
    
    # Conditional expressions
    when, coalesce,
    
    # String processing
    regexp_replace, regexp_extract, trim, upper, lower, initcap,
    
    # Array and split operations
    split, size, array_contains, explode,
    
    # Date and time operations
    to_date, current_timestamp, datediff, months_between, date_add,
    
    # Aggregation and window functions
    sum as sum_col, avg, min, max, count, stddev,
    
    isnan, isnull,
    
    abs, month, year, quarter, row_number,
    avg  
)

from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, 
    StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType, 
    ArrayType
)


def create_silver_directories():
    """Create directory structure for the Silver layer"""
    os.makedirs("datamart/silver", exist_ok=True)
    print("Created silver directory")

def process_silver_loan(bronze_loan_df: DataFrame) -> DataFrame:
    """
    Process loan data from the Bronze layer to create the Silver layer loan table.
    
    Args:
        bronze_loan_df: Loan DataFrame from the Bronze layer
        
    Returns:
        DataFrame: Transformed Silver layer loan DataFrame
    """
    # Define a window partitioned by loan_id and ordered by installment_num
    loan_window = Window.partitionBy("loan_id").orderBy("installment_num")
    
    # 1. Basic status flags
    silver_loan_df = bronze_loan_df.withColumn(
        # Flag indicating whether payment was made in this installment (1 = paid, 0 = unpaid)
        "payment_made_flag", 
        when(col("paid_amt") >= col("due_amt"), 1).otherwise(0)
    ).withColumn(
        # Flag indicating whether the installment is overdue (1 = overdue, 0 = not overdue)
        "monthly_overdue_flag",
        when((col("installment_num") > 0) & (col("paid_amt") < col("due_amt")), 1).otherwise(0)
    )
    
    # 2. Cumulative payment calculations
    silver_loan_df = silver_loan_df.withColumn(
        # Total due amount up to the current installment
        "total_due_amount", 
        col("installment_num") * col("due_amt")
    ).withColumn(
        # Total paid amount (cumulative)
        "total_paid_amount", 
        sum_col("paid_amt").over(loan_window)
    ).withColumn(
        # Payment ratio for the current installment
        "payment_ratio", 
        when(col("due_amt") > 0, col("paid_amt") / col("due_amt")).otherwise(1.0)
    )
    
    # 3. Payment status classification
    silver_loan_df = silver_loan_df.withColumn(
        "payment_status",
        when(col("installment_num") == 0, "LOAN_INITIATED")
        .when(col("monthly_overdue_flag") == 0, "PAID_ON_TIME")
        .when((col("monthly_overdue_flag") == 1) & (col("overdue_amt") <= col("due_amt")), "MISSED_PAYMENT")
        .when((col("monthly_overdue_flag") == 1) & (col("overdue_amt") <= col("due_amt") * 3), "OVERDUE")
        .when(col("monthly_overdue_flag") == 1, "SEVERELY_OVERDUE")
        .otherwise("UNKNOWN")
    )
    
    # 4. Consecutive overdue calculation
    silver_loan_df = silver_loan_df.withColumn(
        # Count consecutive overdue installments (reset after a successful payment)
        "consecutive_missed_payments",
        sum_col(when(col("monthly_overdue_flag") == 1, 1).otherwise(0))
        .over(Window.partitionBy("loan_id")
              .orderBy("installment_num")
              .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        - sum_col(when((col("monthly_overdue_flag") == 0) & (col("installment_num") > 0), 
                        sum_col(when(col("monthly_overdue_flag") == 1, 1).otherwise(0))
                        .over(Window.partitionBy("loan_id")
                              .orderBy("installment_num")
                              .rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)))
                  .otherwise(0))
        .over(Window.partitionBy("loan_id")
              .orderBy("installment_num")
              .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    )
    
    # 5. Date-related calculations
    silver_loan_df = silver_loan_df.withColumn(
        # Age of the loan in months
        "loan_age_months",
        datediff(col("snapshot_date"), col("loan_start_date")) / 30
    ).withColumn(
        # Remaining loan tenure in months
        "remaining_tenure",
        col("tenure") - col("installment_num")
    ).withColumn(
        # Expected loan end date
        "expected_end_date",
        date_add(col("loan_start_date"), col("tenure") * 30)
    ).withColumn(
        # Whether the loan is still active
        "is_active_loan",
        (col("installment_num") < col("tenure")) & 
        (col("snapshot_date") <= date_add(col("loan_start_date"), col("tenure") * 30))
    )
    
    # 6. Business-level fields
    silver_loan_df = silver_loan_df.withColumn(
        # Whether the loan is defaulted (3 or more consecutive missed payments)
        "is_default",
        col("consecutive_missed_payments") >= 3
    ).withColumn(
        # Percentage of total due amount that has been paid
        "percent_paid",
        when(col("total_due_amount") > 0, 
             col("total_paid_amount") / col("total_due_amount") * 100).otherwise(0)
    )
    
    # 7. Add processing metadata
    silver_loan_df = silver_loan_df.withColumn(
        "silver_process_timestamp", 
        current_timestamp()
    )
    
    return silver_loan_df

def save_silver_loan(spark: SparkSession) -> str:
    """
    Load loan data from the Bronze layer, process it, and save it to the Silver layer.
    
    Args:
        spark: Spark session
        
    Returns:
        str: Output path of the saved Silver table
    """
    # Load loan data from the Bronze layer
    bronze_loan_df = spark.read.parquet("datamart/bronze/bronze_loan_daily")
    
    # Transform into Silver layer
    silver_loan_df = process_silver_loan(bronze_loan_df)
    
    # Save Silver-layer data
    output_path = "datamart/silver/silver_loan"
    silver_loan_df.write.mode("overwrite").parquet(output_path)
    
    print(f"Saved silver loan table to {output_path}")
    return output_path




def process_silver_attributes(bronze_attributes_df):
    """
    Process Bronze layer customer attributes data to create Silver layer attributes table
    
    Args:
        bronze_attributes_df: Bronze layer attributes DataFrame
        
    Returns:
        DataFrame: Processed Silver layer attributes DataFrame
    """
    # 1. Process Age field
    attributes_df = bronze_attributes_df.withColumn(
        "Age_cleaned", 
        regexp_replace(col("Age"), "[^0-9-]", "")  # Keep numbers and minus signs
    ).withColumn(
        "Age_int", 
        when(
            col("Age_cleaned").rlike("^-?\\d+$"),  # Validate valid integer
            col("Age_cleaned").cast("int")
        ).otherwise(None)  # Invalid values set to NULL
    ).withColumn(
        "is_valid_age", 
        when(
            col("Age_int").isNotNull() & 
            col("Age_int").between(1, 100),  # Reasonable range of 1-100
            True
        ).otherwise(False)
    ).withColumn(
        "age_processed", 
        when(
            col("is_valid_age") == True,
            col("Age_int")
        ).otherwise(None)  # Invalid ages set to NULL
    )
    
    # 2. Process SSN field
    attributes_df = attributes_df.withColumn(
        "is_valid_ssn", 
        regexp_extract(col("SSN"), "^\\d{3}-\\d{2}-\\d{4}$", 0) != ""
    )
    
    # 3. Process Occupation field
    attributes_df = attributes_df.withColumn(
        "occupation_cleaned", 
        when(
            col("Occupation").isNull() | 
            (col("Occupation") == "_______") |
            (col("Occupation") == ""),
            "Unknown"
        ).otherwise(initcap(trim(col("Occupation"))))  # First letter uppercase, rest lowercase
    )
    
    # 4. Process date fields
    current_date = "2025-05-01"  # Fixed current date point
    attributes_df = attributes_df.withColumn(
        "data_age_months",
        months_between(lit(current_date).cast("date"), col("snapshot_date"))
    )
    
    # 5. Create final Silver layer table
    silver_attributes_df = attributes_df.select(
        # Original fields
        "Customer_ID",
        "Name",
        "snapshot_date",
        # Processed fields
        col("age_processed").alias("age"),
        col("is_valid_age"),
        "SSN",
        col("is_valid_ssn"),
        col("occupation_cleaned").alias("occupation"),
        col("data_age_months"),
        # Metadata fields
        col("bronze_ingest_timestamp"),
        current_timestamp().alias("silver_process_timestamp")
    )
    
    return silver_attributes_df

def save_silver_attributes(spark):
    """
    Load Bronze layer customer attributes data, process and save as Silver layer table
    
    Args:
        spark: Spark session
        
    Returns:
        str: Output table path
    """
    # Load Bronze layer data
    bronze_attributes_df = spark.read.parquet("datamart/bronze/bronze_attributes")
    
    # Process Silver layer transformation
    silver_attributes_df = process_silver_attributes(bronze_attributes_df)
    
    # Save results
    output_path = "datamart/silver/silver_attributes"
    silver_attributes_df.write.mode("overwrite").parquet(output_path)
    
    print(f"Saved silver attributes table to {output_path}")
    return output_path

def process_silver_financials(bronze_financials_df):
    """
    Process Bronze layer financial data to create Silver layer financials table
    
    Args:
        bronze_financials_df: Bronze layer financials DataFrame
        
    Returns:
        DataFrame: Processed Silver layer financials DataFrame
    """
    # 1. Process numeric fields
    financials_df = bronze_financials_df
    
    # 1.1 Process Annual_Income
    financials_df = financials_df.withColumn(
        "annual_income_cleaned", 
        regexp_replace(col("Annual_Income"), "[^0-9\\.]", "")
    ).withColumn(
        "annual_income", 
        when(
            col("annual_income_cleaned").rlike("^\\d+(\\.\\d+)?$"),
            col("annual_income_cleaned").cast("double")
        ).otherwise(None)
    )
    
    # 1.2 Process Outstanding_Debt
    financials_df = financials_df.withColumn(
        "outstanding_debt_cleaned", 
        regexp_replace(col("Outstanding_Debt"), "[^0-9\\.]", "")
    ).withColumn(
        "outstanding_debt", 
        when(
            col("outstanding_debt_cleaned").rlike("^\\d+(\\.\\d+)?$"),
            col("outstanding_debt_cleaned").cast("double")
        ).otherwise(None)
    )
    
    # 1.3 Process Amount_invested_monthly
    financials_df = financials_df.withColumn(
        "amount_invested_monthly_cleaned", 
        regexp_replace(col("Amount_invested_monthly"), "[^0-9\\.]", "")
    ).withColumn(
        "amount_invested_monthly", 
        when(
            col("amount_invested_monthly_cleaned").rlike("^\\d+(\\.\\d+)?$"),
            col("amount_invested_monthly_cleaned").cast("double")
        ).otherwise(None)
    )
    
    # 2. Process text format metrics
    
    # 2.1 Parse Credit_History_Age
    financials_df = financials_df.withColumn(
        "credit_history_years",
        when(
            col("Credit_History_Age").rlike("^\\d+ Years?.*"),
            regexp_extract(col("Credit_History_Age"), "^(\\d+) Years?.*", 1).cast("int")
        ).otherwise(0)
    ).withColumn(
        "credit_history_months",
        when(
            col("Credit_History_Age").rlike(".*and (\\d+) Months?.*"),
            regexp_extract(col("Credit_History_Age"), ".*and (\\d+) Months?.*", 1).cast("int")
        ).otherwise(0)
    ).withColumn(
        "credit_history_total_months",
        col("credit_history_years") * 12 + col("credit_history_months")
    )
    
    # 2.2 Standardize Payment_of_Min_Amount
    financials_df = financials_df.withColumn(
        "payment_of_min_amount_standardized",
        when(col("Payment_of_Min_Amount") == "Yes", "YES")
        .when(col("Payment_of_Min_Amount") == "No", "NO")
        .when(col("Payment_of_Min_Amount") == "NM", "NOT_SPECIFIED")
        .otherwise("UNKNOWN")
    ).withColumn(
        "is_min_payment_made",
        when(col("Payment_of_Min_Amount") == "Yes", True)
        .when(col("Payment_of_Min_Amount") == "No", False)
        .otherwise(None)  # NULL for NM and unknown values
    )
    
    # 2.3 Process Credit_Mix
    financials_df = financials_df.withColumn(
        "credit_mix_standardized",
        when(col("Credit_Mix") == "_", "UNKNOWN")
        .when(col("Credit_Mix") == "Bad", "BAD")
        .when(col("Credit_Mix") == "Good", "GOOD")
        .when(col("Credit_Mix") == "Standard", "STANDARD")
        .otherwise(upper(col("Credit_Mix")))  # Handle other possible values, standardize to uppercase
    )
    
    # 2.4 Analyze Payment_Behaviour
    financials_df = financials_df.withColumn(
        "payment_behavior_standardized",
        upper(col("Payment_Behaviour"))
    ).withColumn(
        "payment_behavior_spent_level",
        when(col("Payment_Behaviour").contains("High_spent"), "HIGH")
        .when(col("Payment_Behaviour").contains("Low_spent"), "LOW")
        .otherwise("UNKNOWN")
    ).withColumn(
        "payment_behavior_value_level",
        when(col("Payment_Behaviour").contains("Large_value"), "LARGE")
        .when(col("Payment_Behaviour").contains("Medium_value"), "MEDIUM")
        .when(col("Payment_Behaviour").contains("Small_value"), "SMALL")
        .otherwise("UNKNOWN")
    )
    
    # 3. Process loan-related fields
    
    # 3.1 Process Num_of_Loan
    financials_df = financials_df.withColumn(
        "num_of_loan_cleaned",
        regexp_replace(col("Num_of_Loan"), "[^0-9]", "")  # Remove non-numeric characters
    ).withColumn(
        "num_of_loans",
        when(
            col("num_of_loan_cleaned").rlike("^\\d+$"),
            col("num_of_loan_cleaned").cast("int")
        ).otherwise(0)  # Default value 0
    )
    
    # 3.2 Process Num_of_Delayed_Payment
    financials_df = financials_df.withColumn(
        "num_delayed_payment_cleaned",
        regexp_replace(col("Num_of_Delayed_Payment"), "[^0-9]", "")  # Remove non-numeric characters
    ).withColumn(
        "num_delayed_payments",
        when(
            col("num_delayed_payment_cleaned").rlike("^\\d+$"),
            col("num_delayed_payment_cleaned").cast("int")
        ).otherwise(0)  # Default value 0
    )
    
    # 3.3 Process Type_of_Loan
    # Convert loan types to array
    financials_df = financials_df.withColumn(
        "loan_types_array",
        when(
            col("Type_of_Loan").isNotNull(),
            split(col("Type_of_Loan"), ",")
        ).otherwise(split(lit(""), ","))  # Empty array
    ).withColumn(
        "loan_types_count",
        size(col("loan_types_array"))
    )
    
    # Create flags for common loan types
    loan_type_flags = [
        "Personal Loan", 
        "Student Loan", 
        "Mortgage Loan", 
        "Auto Loan", 
        "Home Equity Loan", 
        "Debt Consolidation Loan", 
        "Credit-Builder Loan", 
        "Payday Loan"
    ]
    
    # Create flag for each common loan type
    for loan_type in loan_type_flags:
        clean_name = loan_type.replace("-", "_").replace(" ", "_").lower()
        financials_df = financials_df.withColumn(
            f"has_{clean_name}",
            when(
                array_contains(col("loan_types_array"), loan_type) |
                array_contains(col("loan_types_array"), f" {loan_type}"),  # Handle leading spaces
                True
            ).otherwise(False)
        )
    
    # 3.4 Process Changed_Credit_Limit
    financials_df = financials_df.withColumn(
        "changed_credit_limit_cleaned",
        regexp_replace(col("Changed_Credit_Limit"), "[^0-9\\.-]", "")  # Remove non-numeric characters
    ).withColumn(
        "changed_credit_limit",
        when(
            col("changed_credit_limit_cleaned").rlike("^-?\\d+(\\.\\d+)?$"),
            col("changed_credit_limit_cleaned").cast("double")
        ).otherwise(None)  # Invalid values set to NULL
    )
    
    # 4. Process monthly balance and snapshot date
    
    # 4.1 Process monthly_balance
    financials_df = financials_df.withColumn(
        "monthly_balance_cleaned", 
        regexp_replace(col("Monthly_Balance"), "[^0-9\\.-]", "")  # Remove non-numeric characters
    ).withColumn(
        "monthly_balance_processed", 
        when(
            col("monthly_balance_cleaned").rlike("^-?\\d+(\\.\\d+)?$"),
            col("monthly_balance_cleaned").cast("double")
        ).otherwise(None)  # Invalid values set to NULL
    )
    
    # # 4.2 Handle NULL snapshot_date issues
    # # Since all snapshot_dates are NULL, we can use a default date
    # default_date = "2023-01-01"  # Use a reasonable default date
    # financials_df = financials_df.withColumn(
    #     "silver_snapshot_date",
    #     coalesce(col("snapshot_date"), to_date(lit(default_date)))
    # )
    
    # 5. Create final Silver layer table
    silver_financials_df = financials_df.select(
        # Customer ID and date
        "Customer_ID",
        col("snapshot_date").alias("snapshot_date"),
        
        # Income and salary
        col("annual_income").alias("annual_income"),
        "Monthly_Inhand_Salary",
        
        # Bank and credit cards
        "Num_Bank_Accounts",
        "Num_Credit_Card",
        
        # Loan information
        "Interest_Rate",
        col("num_of_loans").alias("num_of_loans"),
        col("loan_types_count").alias("loan_types_count"),
        
        # Common loan type flags
        "has_personal_loan",
        "has_student_loan", 
        "has_mortgage_loan",
        "has_auto_loan",
        "has_home_equity_loan",
        "has_debt_consolidation_loan", 
        "has_credit_builder_loan",
        "has_payday_loan",
        
        # Delayed payments
        "Delay_from_due_date",
        col("num_delayed_payments").alias("num_delayed_payments"),
        
        # Credit related
        col("changed_credit_limit").alias("changed_credit_limit"),
        "Num_Credit_Inquiries",
        col("credit_mix_standardized").alias("credit_mix"),
        col("outstanding_debt").alias("outstanding_debt"),
        "Credit_Utilization_Ratio",
        col("credit_history_years").alias("credit_history_years"),
        col("credit_history_months").alias("credit_history_months"),
        col("credit_history_total_months").alias("credit_history_total_months"),
        
        # Payment behavior
        col("is_min_payment_made").alias("is_min_payment_made"),
        "Total_EMI_per_month",
        col("amount_invested_monthly").alias("amount_invested_monthly"),
        col("payment_behavior_spent_level").alias("payment_behavior_spent_level"),
        col("payment_behavior_value_level").alias("payment_behavior_value_level"),
        col("monthly_balance_processed").alias("monthly_balance"),
        
        # Metadata
        "bronze_ingest_timestamp",
        current_timestamp().alias("silver_process_timestamp")
    )
    
    return silver_financials_df

def save_silver_financials(spark):
    """
    Load Bronze layer financial data, process and save as Silver layer table
    
    Args:
        spark: Spark session
        
    Returns:
        str: Output table path
    """
    # Load Bronze layer data
    bronze_financials_df = spark.read.parquet("datamart/bronze/bronze_financials")
    
    # Process Silver layer transformation
    silver_financials_df = process_silver_financials(bronze_financials_df)
    
    # Save results
    output_path = "datamart/silver/silver_financials"
    silver_financials_df.write.mode("overwrite").parquet(output_path)
    
    print(f"Saved silver financials table to {output_path}")
    return output_path

def process_silver_clickstream_detailed(bronze_clickstream_df):
    """
    Process Bronze layer clickstream data to create Silver layer detailed clickstream table
    
    Args:
        bronze_clickstream_df: Bronze layer clickstream DataFrame
        
    Returns:
        DataFrame: Processed Silver layer detailed clickstream DataFrame
    """
    # 1. Define feature columns and other columns
    feature_cols = [f"fe_{i}" for i in range(1, 21)]
    id_cols = ["Customer_ID", "snapshot_date"]
    metadata_cols = ["bronze_ingest_timestamp", "bronze_source_file"]

    # 2. Add normalized features and outlier flags for each feature
    detailed_df = bronze_clickstream_df

    # Calculate mean and stddev for each feature
    feature_stats = {}
    for col_name in feature_cols:
        stats = bronze_clickstream_df.select(
            avg(col(col_name)).alias("mean"),
            stddev(col(col_name)).alias("stddev")
        ).collect()[0]
        feature_stats[col_name] = {
            "mean": stats["mean"], 
            "stddev": stats["stddev"]
        }

    # Add normalized features and outlier flags
    for col_name in feature_cols:
        mean_val = feature_stats[col_name]["mean"]
        stddev_val = feature_stats[col_name]["stddev"]
        
        # Add Z-score normalized feature
        detailed_df = detailed_df.withColumn(
            f"{col_name}_normalized", 
            (col(col_name) - lit(mean_val)) / lit(stddev_val)
        )
        
        # Add outlier flag (outside ±3 standard deviations)
        detailed_df = detailed_df.withColumn(
            f"{col_name}_is_outlier",
            (abs(col(f"{col_name}_normalized")) > 3)
        )

    # 3. Add time-related features
    # Group by customer, order by date
    window_spec = Window.partitionBy("Customer_ID").orderBy("snapshot_date")

    # Add row number, month, quarter and year
    detailed_df = detailed_df.withColumn("row_num_by_customer", row_number().over(window_spec)) \
        .withColumn("month", month(col("snapshot_date"))) \
        .withColumn("quarter", quarter(col("snapshot_date"))) \
        .withColumn("year", year(col("snapshot_date")))

    # Get first snapshot date for each customer
    first_snapshot = detailed_df.groupBy("Customer_ID") \
        .agg(min(col("snapshot_date")).alias("first_snapshot_date"))

    # Join first snapshot date to detailed data
    detailed_df = detailed_df.join(first_snapshot, "Customer_ID", "left")

    # Calculate months difference from first snapshot
    detailed_df = detailed_df.withColumn(
        "months_since_first",
        (year(col("snapshot_date")) - year(col("first_snapshot_date"))) * 12 + 
        (month(col("snapshot_date")) - month(col("first_snapshot_date")))
    )

    # 4. Add processing metadata
    detailed_df = detailed_df.withColumn(
        "silver_process_timestamp", 
        current_timestamp()
    )

    # 5. Create final silver_clickstream_detailed table
    silver_clickstream_detailed = detailed_df.select(
        # ID columns
        *id_cols,
        # Original features
        *feature_cols,
        # Normalized features
        *[f"{col}_normalized" for col in feature_cols],
        # Outlier flags
        *[f"{col}_is_outlier" for col in feature_cols],
        # Time features
        "month", "quarter", "year", 
        "row_num_by_customer", "months_since_first",
        "first_snapshot_date",  # Add this line to ensure column in result
        # Metadata
        *metadata_cols,
        "silver_process_timestamp"
    )
    
    return silver_clickstream_detailed

def process_silver_clickstream_aggregated(silver_clickstream_detailed):
    """
    Create aggregated clickstream table based on detailed clickstream data
    
    Args:
        silver_clickstream_detailed: Silver layer detailed clickstream DataFrame
        
    Returns:
        DataFrame: Processed Silver layer aggregated clickstream DataFrame
    """
    # Define feature columns
    feature_cols = [f"fe_{i}" for i in range(1, 21)]
    
    # Calculate aggregated statistics by customer ID
    agg_exprs = []
    for col_name in feature_cols:
        # Mean
        agg_exprs.append(avg(col(col_name)).alias(f"{col_name}_mean"))
        # Max
        agg_exprs.append(max(col(col_name)).alias(f"{col_name}_max"))
        # Min
        agg_exprs.append(min(col(col_name)).alias(f"{col_name}_min"))
        # Standard deviation
        agg_exprs.append(stddev(col(col_name)).alias(f"{col_name}_stddev"))
        # Outlier count
        agg_exprs.append(
            sum_col(when(col(f"{col_name}_is_outlier"), 1).otherwise(0)).alias(f"{col_name}_outlier_count")
        )

    # Additional aggregation metrics
    agg_exprs.extend([
        max(col("snapshot_date")).alias("latest_snapshot_date"),
        count("*").alias("record_count"),
        max(col("months_since_first")).alias("max_months"),
        min(col("first_snapshot_date")).alias("first_snapshot_date")  # This should work now
    ])

    # Perform aggregation
    silver_clickstream_aggregated = silver_clickstream_detailed.groupBy("Customer_ID").agg(*agg_exprs)

    # Add processing metadata
    silver_clickstream_aggregated = silver_clickstream_aggregated.withColumn(
        "silver_process_timestamp", 
        current_timestamp()
    )
    
    return silver_clickstream_aggregated

def save_silver_clickstream(spark):
    """
    Load Bronze layer clickstream data, process and save as Silver layer tables
    
    Args:
        spark: Spark session
        
    Returns:
        tuple: (detailed_output_path, aggregated_output_path)
    """
    # Load Bronze layer data
    bronze_clickstream_df = spark.read.parquet("datamart/bronze/bronze_clickstream")
    
    # Process detailed view
    silver_clickstream_detailed = process_silver_clickstream_detailed(bronze_clickstream_df)
    
    # Process aggregated view
    silver_clickstream_aggregated = process_silver_clickstream_aggregated(silver_clickstream_detailed)
    
    # Save detailed view
    detailed_output_path = "datamart/silver/silver_clickstream_detailed"
    silver_clickstream_detailed.write.mode("overwrite").parquet(detailed_output_path)
    print(f"Saved silver clickstream detailed table to {detailed_output_path}")
    
    # Save aggregated view
    aggregated_output_path = "datamart/silver/silver_clickstream_aggregated"
    silver_clickstream_aggregated.write.mode("overwrite").parquet(aggregated_output_path)
    print(f"Saved silver clickstream aggregated table to {aggregated_output_path}")
    
    return (detailed_output_path, aggregated_output_path)