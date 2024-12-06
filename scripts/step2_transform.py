# Importing necessary libraries and functions for data processing
import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame

# Define paths
DATA_DIR = pathlib.Path("C:/Users/4harg/Documents/smart-store-marco/data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR = DATA_DIR.joinpath("prepared")
sales_data_path = RAW_DATA_DIR.joinpath("sales_data.csv")
prepared_sales_path = PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv")

def load_data(spark: SparkSession, file_path: pathlib.Path) -> DataFrame:
    """Load a CSV file into a PySpark DataFrame."""
    return spark.read.csv(str(file_path), header=True, inferSchema=True)

def clean_sales_data(sales_df: DataFrame) -> DataFrame:
    """Clean the sales data by removing duplicates and handling outliers."""
    print("Cleaning Sales Data...")

    sales_df = sales_df.dropDuplicates()
    discount_mode = sales_df.groupBy("DiscountPercent").count().orderBy("count", ascending=False).first()[0]
    sales_df = sales_df.withColumn("DiscountPercent", when(col("DiscountPercent") == "five", 5).otherwise(col("DiscountPercent")))
    sales_df = sales_df.withColumn("DiscountPercent", col("DiscountPercent").cast("double"))
    sales_df = sales_df.withColumn("DiscountPercent", when(col("DiscountPercent").isin(0, 100), discount_mode).otherwise(col("DiscountPercent")))
    sales_df = sales_df.fillna({"DiscountPercent": discount_mode})
    
    print(f"After Cleaning Sales Data - Columns: {len(sales_df.columns)}, Records: {sales_df.count()}")
    return sales_df

def save_data(df: DataFrame, file_path: pathlib.Path) -> None:
    """Save cleaned data to CSV."""
    df.write.option("header", "true").csv(str(file_path), mode="overwrite")
    print(f"Cleaned data saved to {file_path}")

def process_sales_data():
    """Process sales data (load, clean, save)."""
    spark = SparkSession.builder.appName("SalesDataCleaning").getOrCreate()

    sales_df = load_data(spark, sales_data_path)
    cleaned_sales_df = clean_sales_data(sales_df)
    save_data(cleaned_sales_df, prepared_sales_path)

    spark.stop()
