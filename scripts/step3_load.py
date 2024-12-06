import pathlib
import sqlite3
import pandas as pd
import sys  # Import sys module here
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from utils.logger import logger  # Assuming logger is configured properly

# Constants
PROJECT_ROOT = pathlib.Path(r"C:/Users/4harg/OneDrive/Documents/smart-store-marco")
PREPARED_DATA_DIR = PROJECT_ROOT.joinpath("data", "prepared")
DB_PATH = PROJECT_ROOT.joinpath("data", "dw", "smart_sales.db")

CUSTOMERS_DATA = PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv")
PRODUCTS_DATA = PREPARED_DATA_DIR.joinpath("products_data_prepared.csv")
SALES_DATA = PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv")

# Ensure project root is in sys.path for local module imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Initialize Spark session
spark = SparkSession.builder.appName("ETL_Process").getOrCreate()

def load_csv_to_spark(file_path: pathlib.Path) -> DataFrame:
    """
    Load CSV into a Spark DataFrame.

    Args:
        file_path (Path): Path to the CSV file.

    Returns:
        DataFrame: Loaded Spark DataFrame.
    """
    logger.info(f"Reading data from {file_path}")
    return spark.read.csv(str(file_path), header=True, inferSchema=True)

def validate_schema(cursor: sqlite3.Cursor, table_name: str, required_columns: set) -> bool:
    """
    Validate that the table in the database contains the required columns.

    Args:
        cursor: SQLite cursor object.
        table_name: Name of the table to validate.
        required_columns: A set of column names expected in the table.

    Returns:
        True if the schema is valid; False if columns are missing.
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}  # Extract column names from PRAGMA results
    missing_columns = required_columns - existing_columns

    if missing_columns:
        logger.error(f"Missing columns in {table_name} table: {missing_columns}")
        return False
    return True

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables."""
    try:
        cursor.execute("DELETE FROM customers")
        cursor.execute("DELETE FROM products")
        cursor.execute("DELETE FROM sales")
        logger.info("Existing records deleted from all tables.")
    except sqlite3.Error as e:
        logger.error(f"Error deleting records: {e}")
        raise

def insert_data_to_db(df: pd.DataFrame, table_name: str, cursor: sqlite3.Cursor) -> None:
    """
    Insert data into the given table from a DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        table_name (str): The name of the table in the database.
        cursor (sqlite3.Cursor): The SQLite cursor.
    """
    try:
        df.to_sql(table_name, cursor.connection, if_exists="append", index=False)
        logger.info(f"Data inserted into the {table_name} table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting {table_name}: {e}")
        raise

def process_and_load_sales_data():
    """Process and load the sales data into the data warehouse."""
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Validate and clean the schema
        delete_existing_records(cursor)

        # Load data from CSV into Spark DataFrames
        customers_df = load_csv_to_spark(CUSTOMERS_DATA).toPandas()
        products_df = load_csv_to_spark(PRODUCTS_DATA).toPandas()
        sales_df = load_csv_to_spark(SALES_DATA).toPandas()

        # Insert the data into the database
        insert_data_to_db(customers_df, "customers", cursor)
        insert_data_to_db(products_df, "products", cursor)
        insert_data_to_db(sales_df, "sales", cursor)

        # Commit and close the connection
        conn.commit()
        logger.info("Data successfully loaded into the data warehouse.")

    except sqlite3.Error as e:
        logger.error(f"Error during database load: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during ETL: {e}")
    finally:
        conn.close()

def main():
    """Main function for running the ETL process."""
    logger.info("Starting ETL process to load data into the data warehouse.")
    process_and_load_sales_data()
    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()
