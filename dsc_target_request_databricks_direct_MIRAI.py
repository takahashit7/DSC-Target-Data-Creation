# Databricks notebook source
"""
DSC Target Request - Live MIRAI Data Workflow
Converted from KNIME workflow to Python for Databricks

Workflow: DSC_TargetRequest_Live MIRAI data_v1_0
Purpose: Generate Target Request report for Division Scorecard based on OSA's YA data, master data, and RCA master data
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

# Input/Output paths - Update these according to your environment
INPUT_BASE_PATH = "/dbfs/mnt/data/input/"
OUTPUT_BASE_PATH = "/dbfs/mnt/data/output/"

# File paths configuration
CONFIG = {
    "input_files": {
        "mirai_data": f"{INPUT_BASE_PATH}mirai_live_data.xlsx",
        "master_data": f"{INPUT_BASE_PATH}dsc_master.xlsx",
        "rca_master": f"{INPUT_BASE_PATH}rca_master.xlsx",
        "org_master": f"{INPUT_BASE_PATH}org_master.xlsx",
    },
    "output_files": {
        "target_request_report": f"{OUTPUT_BASE_PATH}DSC_TargetRequest_Report_{datetime.now().strftime('%Y%m%d')}.xlsx"
    }
}

# COMMAND ----------

Print ("Add comment")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   cust_name
# MAGIC from hive_metastore.japan_gold.dsc_report_raw_data_vw
# MAGIC limit 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def read_excel_spark(file_path: str, sheet_name: str = None) -> DataFrame:
    """
    Read Excel file into Spark DataFrame
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to read (optional)
    
    Returns:
        Spark DataFrame
    """
    # Read Excel using pandas first (Databricks doesn't have native Excel reader for Spark)
    if sheet_name:
        pdf = pd.read_excel(file_path, sheet_name=sheet_name)
    else:
        pdf = pd.read_excel(file_path)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(pdf)
    return df


def write_excel_spark(df: DataFrame, file_path: str, sheet_name: str = "Sheet1"):
    """
    Write Spark DataFrame to Excel file
    
    Args:
        df: Spark DataFrame to write
        file_path: Output Excel file path
        sheet_name: Name of the sheet
    """
    # Convert to Pandas for Excel writing
    pdf = df.toPandas()
    
    # Write to Excel
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        pdf.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"Successfully wrote data to {file_path}")


def apply_missing_value_handling(df: DataFrame, columns: List[str], 
                                  missing_value_strategy: str = "remove") -> DataFrame:
    """
    Handle missing values in specified columns
    
    Args:
        df: Input DataFrame
        columns: List of columns to check
        missing_value_strategy: Strategy - "remove", "fill_zero", "fill_forward"
    
    Returns:
        DataFrame with missing values handled
    """
    if missing_value_strategy == "remove":
        # Remove rows with null values in specified columns
        for col in columns:
            df = df.filter(col_(col).isNotNull())
    elif missing_value_strategy == "fill_zero":
        # Fill nulls with 0
        fill_dict = {col: 0 for col in columns}
        df = df.fillna(fill_dict)
    elif missing_value_strategy == "fill_forward":
        # Forward fill using window functions
        for col in columns:
            window = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, 0)
            df = df.withColumn(col, last(col_(col), ignorenulls=True).over(window))
    
    return df


def perform_join(left_df: DataFrame, right_df: DataFrame, 
                 join_cols: List[str], join_type: str = "inner") -> DataFrame:
    """
    Perform join operation between two DataFrames
    
    Args:
        left_df: Left DataFrame
        right_df: Right DataFrame
        join_cols: Columns to join on
        join_type: Type of join - "inner", "left", "right", "outer"
    
    Returns:
        Joined DataFrame
    """
    return left_df.join(right_df, on=join_cols, how=join_type)


def apply_row_filter(df: DataFrame, filter_condition: str) -> DataFrame:
    """
    Apply row filter based on condition
    
    Args:
        df: Input DataFrame
        filter_condition: SQL-style filter condition
    
    Returns:
        Filtered DataFrame
    """
    return df.filter(filter_condition)


def apply_column_filter(df: DataFrame, columns_to_keep: List[str]) -> DataFrame:
    """
    Keep only specified columns
    
    Args:
        df: Input DataFrame
        columns_to_keep: List of column names to keep
    
    Returns:
        DataFrame with selected columns
    """
    return df.select(columns_to_keep)


def apply_string_manipulation(df: DataFrame, col_name: str, 
                               operation: str, params: Dict) -> DataFrame:
    """
    Apply string manipulation operations
    
    Args:
        df: Input DataFrame
        col_name: Column name to manipulate
        operation: Operation type - "concat", "replace", "substr", "upper", "lower"
        params: Parameters for the operation
    
    Returns:
        DataFrame with manipulated column
    """
    if operation == "concat":
        df = df.withColumn(col_name, concat(*params['columns']))
    elif operation == "replace":
        df = df.withColumn(col_name, regexp_replace(col_name, params['pattern'], params['replacement']))
    elif operation == "substr":
        df = df.withColumn(col_name, substring(col_name, params['start'], params['length']))
    elif operation == "upper":
        df = df.withColumn(col_name, upper(col_name))
    elif operation == "lower":
        df = df.withColumn(col_name, lower(col_name))
    
    return df


def apply_group_by_aggregation(df: DataFrame, group_cols: List[str], 
                                agg_dict: Dict) -> DataFrame:
    """
    Perform group by aggregation
    
    Args:
        df: Input DataFrame
        group_cols: Columns to group by
        agg_dict: Dictionary of {column: agg_function}
    
    Returns:
        Aggregated DataFrame
    """
    return df.groupBy(group_cols).agg(agg_dict)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

print("Loading input data files...")

# Load MIRAI Live Data
df_mirai = read_excel_spark(CONFIG["input_files"]["mirai_data"], sheet_name="MIRAI_Data")
print(f"MIRAI Data loaded: {df_mirai.count()} rows")

# Load Master Data files
df_dsc_master = read_excel_spark(CONFIG["input_files"]["master_data"], sheet_name="DSC_Master")
print(f"DSC Master loaded: {df_dsc_master.count()} rows")

df_rca_master = read_excel_spark(CONFIG["input_files"]["rca_master"], sheet_name="RCA_Master")
print(f"RCA Master loaded: {df_rca_master.count()} rows")

df_org_master = read_excel_spark(CONFIG["input_files"]["org_master"], sheet_name="Org_Master")
print(f"Org Master loaded: {df_org_master.count()} rows")

# Display sample data
display(df_mirai.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing Pipeline

# COMMAND ----------

# Step 1: Handle Missing Values
print("Step 1: Handling missing values...")
df_mirai_cleaned = apply_missing_value_handling(
    df_mirai, 
    columns=['CustomerCode', 'OrgCode', 'TargetValue'],
    missing_value_strategy="remove"
)

# COMMAND ----------

# Step 2: Join with Master Data
print("Step 2: Joining with master data...")

# Join with DSC Master
df_joined = perform_join(
    df_mirai_cleaned,
    df_dsc_master,
    join_cols=['CustomerCode'],
    join_type="left"
)

# Join with Org Master
df_joined = perform_join(
    df_joined,
    df_org_master,
    join_cols=['OrgCode'],
    join_type="left"
)

# Join with RCA Master
df_joined = perform_join(
    df_joined,
    df_rca_master,
    join_cols=['RCACode'],
    join_type="left"
)

print(f"Joined data: {df_joined.count()} rows")

# COMMAND ----------

# Step 3: Apply business logic transformations
print("Step 3: Applying business logic transformations...")

# Add derived columns
df_transformed = df_joined.withColumn(
    "TargetYear",
    year(current_date()) + 1
)

df_transformed = df_transformed.withColumn(
    "RequestDate",
    current_date()
)

# Apply conditional logic for target calculations
df_transformed = df_transformed.withColumn(
    "AdjustedTarget",
    when(col("TargetValue") > 0, col("TargetValue") * 1.1)
    .otherwise(col("TargetValue"))
)

# COMMAND ----------

# Step 4: Apply filters
print("Step 4: Applying filters...")

df_filtered = apply_row_filter(
    df_transformed,
    "OrgLevel = 'Division' AND TargetValue IS NOT NULL"
)

# COMMAND ----------

# Step 5: Aggregate data
print("Step 5: Aggregating data...")

df_aggregated = apply_group_by_aggregation(
    df_filtered,
    group_cols=['Division', 'CustomerName', 'TargetYear'],
    agg_dict={
        'TargetValue': 'sum',
        'AdjustedTarget': 'sum',
        'RequestDate': 'max'
    }
)

# COMMAND ----------

# Step 6: Select and rename final columns
print("Step 6: Preparing final output...")

df_final = df_aggregated.select(
    col("Division"),
    col("CustomerName").alias("Customer"),
    col("TargetYear").alias("Target Year"),
    col("sum(TargetValue)").alias("Current Target"),
    col("sum(AdjustedTarget)").alias("Proposed Target"),
    col("max(RequestDate)").alias("Request Date")
)

# Sort results
df_final = df_final.orderBy("Division", "Customer")

# Display final results
print(f"\nFinal output: {df_final.count()} rows")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Export

# COMMAND ----------

print("Exporting results...")

# Write to Excel
write_excel_spark(
    df_final,
    CONFIG["output_files"]["target_request_report"],
    sheet_name="Target_Request"
)

print(f"\nExport completed successfully!")
print(f"Output file: {CONFIG['output_files']['target_request_report']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation

# COMMAND ----------

# Validate output
print("Validation Summary:")
print("-" * 80)
print(f"Total records processed: {df_final.count()}")
print(f"Unique divisions: {df_final.select('Division').distinct().count()}")
print(f"Unique customers: {df_final.select('Customer').distinct().count()}")
print(f"Total proposed target: {df_final.agg(sum('Proposed Target')).collect()[0][0]}")
print("-" * 80)

# Show sample of final data
print("\nSample of final output:")
df_final.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes and Documentation
# MAGIC
# MAGIC ### Workflow Summary
# MAGIC - **Input**: MIRAI live data, DSC Master, RCA Master, Org Master
# MAGIC - **Processing**: Data cleaning, joins, transformations, aggregations
# MAGIC - **Output**: Target Request report for Division Scorecard
# MAGIC
# MAGIC ### Key Transformations
# MAGIC 1. Remove records with missing critical fields
# MAGIC 2. Join multiple master data sources
# MAGIC 3. Calculate adjusted targets (10% increase for positive values)
# MAGIC 4. Filter for Division-level records only
# MAGIC 5. Aggregate by Division, Customer, and Target Year
# MAGIC
# MAGIC ### Customization Required
# MAGIC - Update file paths in CONFIG section
# MAGIC - Adjust column names based on actual data schema
# MAGIC - Modify business logic transformations as needed
# MAGIC - Add additional data quality checks
# MAGIC
