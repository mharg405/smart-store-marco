# Importing necessary libraries and functions for data extraction
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from utils.logger import logger  # Assuming logger is configured properly

# Define paths
DW_DIR = Path("data").joinpath("dw")
DB_PATH = Path("data").joinpath("dw", "smart_sales.db")
SALES_DATA_PATH = Path(r"C:\Users\4harg\OneDrive\Documents\smart-store-marco\data\raw\sales_data.csv")

def read_csv(spark: SparkSession, file_path: Path) -> DataFrame:
    """Read a CSV file into a Spark DataFrame."""
    logger.info(f"Attempting to read CSV file from: {file_path}")
    try:
        df = spark.read.csv(str(file_path), header=True, inferSchema=True)
        logger.info(f"Successfully read CSV file: {file_path}")
        logger.info(f"Schema of DataFrame:\n{df.printSchema()}")
        return df
    except AnalysisException as e:
        logger.error(f"Failed to read CSV file: {file_path}. Error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while reading CSV file: {file_path}. Error: {e}")
        raise

def main():
    """Main function to extract and log sales data."""
    logger.info("Starting the CSV Read Process")

    spark = SparkSession.builder.appName("SalesDataExtraction").getOrCreate()
    try:
        logger.info("Reading sales data")
        sales_df = read_csv(spark, SALES_DATA_PATH)
        logger.info(f"Data successfully loaded. Rows: {sales_df.count()}, Columns: {len(sales_df.columns)}")
        sales_df.show(5)  # Optionally, show first 5 rows
    except Exception as e:
        logger.error(f"An error occurred during the extraction process: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
