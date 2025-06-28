import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col, regexp_replace, when, trim, regexp_extract, lit, split, size
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


# def clean_clickstream_data(df):
#     """
#     Clean and process clickstream bronze data
#     """
#     # Ensure all fe_ columns are integers
#     for i in range(1, 21):
#         col_name = f"fe_{i}"
#         if col_name in df.columns:
#             df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
    
#     # Cast core columns
#     df = df.withColumn("Customer_ID", col("Customer_ID").cast(StringType()))
#     df = df.withColumn("snapshot_date", col("snapshot_date").cast(DateType()))
#     return df


def clean_attributes_data(df):
    """
    Clean and process attributes bronze data
    """
    # Clean Name: remove non-letter characters except spaces, dots, apostrophes, hyphens
    df = df.withColumn(
        "Name",
        trim(regexp_replace(col("Name"), r"[^A-Za-z\s\.'-]", ""))
    )

    # Clean Age: extract numeric part and validate range 0-120
    numeric_regex = r'([-+]?\d*\.?\d+)'
    df = df.withColumn(
        "Age_tmp",
        regexp_extract(col("Age").cast(StringType()), numeric_regex, 1)
    ).withColumn(
        "Age",
        when(
            (col("Age_tmp").cast(IntegerType()) >= 0) & 
            (col("Age_tmp").cast(IntegerType()) <= 120),
            col("Age_tmp").cast(IntegerType())
        ).otherwise(None)
    ).drop("Age_tmp")

    # Clean SSN: enforce XXX-XX-XXXX format
    df = df.withColumn(
        "SSN_tmp",
        regexp_replace(col("SSN"), r"[^0-9-]", "")
    ).withColumn(
        "SSN",
        when(col("SSN_tmp").rlike(r"^\d{3}-\d{2}-\d{4}$"), col("SSN_tmp"))
        .otherwise(None)
    ).drop("SSN_tmp")

    # Clean Occupation: null out blanks or underscores only
    df = df.withColumn(
        "Occupation",
        when(trim(col("Occupation")) == "", None)
        .when(col("Occupation").rlike(r"^_+$"), None)
        .otherwise(col("Occupation"))
    )

    # Cast core columns
    df = df.withColumn("Customer_ID", col("Customer_ID").cast(StringType()))
    df = df.withColumn("Name", col("Name").cast(StringType()))
    df = df.withColumn("SSN", col("SSN").cast(StringType()))
    df = df.withColumn("Occupation", col("Occupation").cast(StringType()))
    df = df.withColumn("snapshot_date", col("snapshot_date").cast(DateType()))

    return df


def add_basic_features(df):
    """
    Add basic feature engineering to the combined dataset
    """
    # # Calculate total clickstream activity (sum of all fe_ columns)
    # fe_cols = [f"fe_{i}" for i in range(1, 21)]
    # existing_fe_cols = [col_name for col_name in fe_cols if col_name in df.columns]

    # if existing_fe_cols:
    #     # Sum all clickstream features
    #     sum_expr = col(existing_fe_cols[0])
    #     for col_name in existing_fe_cols[1:]:
    #         sum_expr = sum_expr + col(col_name)

    #     df = df.withColumn("total_clickstream_activity", sum_expr.cast(IntegerType()))

    #     # Calculate average clickstream activity
    #     df = df.withColumn(
    #         "avg_clickstream_activity", 
    #         (col("total_clickstream_activity") / lit(len(existing_fe_cols))).cast(FloatType())
    #     )

    #     # High activity flag (above 75th percentile)
    #     percentile_75 = df.approxQuantile("total_clickstream_activity", [0.75], 0.01)[0]
    #     df = df.withColumn(
    #         "is_high_activity_user",
    #         when(col("total_clickstream_activity") > percentile_75, lit(1))
    #         .otherwise(lit(0))
    #         .cast(IntegerType())
    #     )

    # Age group categorization
    df = df.withColumn(
        "age_group",
        when(col("Age") < 25, lit("Young"))
        .when((col("Age") >= 25) & (col("Age") < 35), lit("Adult"))
        .when((col("Age") >= 35) & (col("Age") < 50), lit("Middle"))
        .when((col("Age") >= 50) & (col("Age") < 65), lit("Senior"))
        .when(col("Age") >= 65, lit("Elder"))
        .otherwise(lit("Unknown"))
        .cast(StringType())
    )

    # Professional category based on occupation
    df = df.withColumn(
        "occupation_category",
        when(col("Occupation").isin(["Doctor", "Lawyer", "Engineer", "Scientist"]), lit("Professional"))
        .when(col("Occupation").isin(["Manager", "Entrepreneur", "Media_Manager"]), lit("Management"))
        .when(col("Occupation").isin(["Teacher", "Journalist", "Writer"]), lit("Education_Media"))
        .when(col("Occupation").isin(["Accountant", "Architect", "Developer"]), lit("Technical"))
        .when(col("Occupation").isin(["Musician", "Mechanic"]), lit("Skilled_Trade"))
        .otherwise(lit("Other"))
        .cast(StringType())
    )

    return df


def process_silver_features_combined_1(snapshot_date_str, 
                                     bronze_clickstream_dir,
                                     bronze_attributes_dir, 
                                     silver_combined_dir, 
                                     spark):
    """
    Process bronze attributes data into silver combined table
    """
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    part_suffix = snapshot_date_str.replace('-', '_')

    # # Load bronze clickstream data
    # clickstream_file = f"bronze_features_clickstream_{part_suffix}.csv"
    # clickstream_path = os.path.join(bronze_clickstream_dir, clickstream_file)
    # if not os.path.exists(clickstream_path):
    #     raise FileNotFoundError(f"Bronze clickstream file not found: {clickstream_path}")
    # df_clickstream = spark.read.csv(clickstream_path, header=True, inferSchema=True)
    # print(f'Loaded clickstream from: {clickstream_path}, row count: {df_clickstream.count()}')

    # Load bronze attributes data
    attributes_file = f"bronze_features_attributes_{part_suffix}.csv"
    attributes_path = os.path.join(bronze_attributes_dir, attributes_file)
    if not os.path.exists(attributes_path):
        raise FileNotFoundError(f"Bronze attributes file not found: {attributes_path}")
    df_attributes = spark.read.csv(attributes_path, header=True, inferSchema=True)
    print(f'Loaded attributes from: {attributes_path}, row count: {df_attributes.count()}')

    # Clean attributes only
    # df_clickstream_clean = clean_clickstream_data(df_clickstream)
    df_attributes_clean = clean_attributes_data(df_attributes)

    # df_combined = df_clickstream_clean.join(df_attributes_clean, on=["Customer_ID", "snapshot_date"], how="inner")
    df_combined = df_attributes_clean  # Only attributes used
    print(f'Combined row count (attributes only): {df_combined.count()}')

    df_final = add_basic_features(df_combined)

    output_file = f"silver_features_combined_1_{part_suffix}.parquet"
    output_path = os.path.join(silver_combined_dir, output_file)
    df_final.write.mode("overwrite").parquet(output_path)
    print(f'Saved to: {output_path}')

    return df_final


def main():
    parser = argparse.ArgumentParser(description='Process silver features combined table 1')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--bronze_clickstream_dir', 
                       default='datamart/bronze/features_clickstream/',
                       help='Bronze clickstream data directory')
    parser.add_argument('--bronze_attributes_dir',
                       default='datamart/bronze/features_attributes/', 
                       help='Bronze attributes data directory')
    parser.add_argument('--silver_combined_dir',
                       default='datamart/silver/features_combined_1/',
                       help='Silver combined output directory')

    args = parser.parse_args()

    spark = pyspark.sql.SparkSession.builder \
        .appName("SilverFeaturesCombined1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    try:
        df_result = process_silver_features_combined_1(
            snapshot_date_str=args.snapshotdate,
            bronze_clickstream_dir=args.bronze_clickstream_dir,
            bronze_attributes_dir=args.bronze_attributes_dir,
            silver_combined_dir=args.silver_combined_dir,
            spark=spark
        )

        print("Silver features combined 1 processing completed successfully!")
        print(f"Final schema:")
        df_result.printSchema()

    except Exception as e:
        print(f"Error processing silver features combined 1: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
