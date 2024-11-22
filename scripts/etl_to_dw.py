import pandas as pd
import sqlite3
import sys
import pathlib

# Set PROJECT_ROOT to the root directory of your project
PROJECT_ROOT = pathlib.Path(r"C:/Users/4harg/OneDrive/Documents/smart-store-marco")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))  # Add the project root to sys.path

# Import logger from utils
from utils.logger import logger

# Constants
PREPARED_DATA_DIR: pathlib.Path = pathlib.Path("C:/Users/4harg/OneDrive/Documents/smart-store-marco/data/prepared")
DB_PATH: str = "C:/Users/4harg/OneDrive/Documents/smart-store-marco/data/dw/smart_sales.db"

CUSTOMERS_DATA = PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv")
PRODUCTS_DATA = PREPARED_DATA_DIR.joinpath("products_data_prepared.csv")
SALES_DATA = PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv")


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables."""
    try:
        cursor.execute("DELETE FROM customer")
        cursor.execute("DELETE FROM product")
        cursor.execute("DELETE FROM sale")
        logger.info("Existing records deleted from all tables.")
    except sqlite3.Error as e:
        logger.error(f"Error deleting records: {e}")
        raise

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customers data into the customer table with explicit mapping."""
    try:
        # Verify if required columns exist in the DataFrame
        required_columns = {"CustomerID", "Name", "Region", "JoinDate","LoyaltyPoints","PreferredContactMethod"}
        if not required_columns.issubset(customers_df.columns):
            logger.error(f"Missing columns in customers DataFrame: {required_columns - set(customers_df.columns)}")
            return

        # Map CSV columns to database table columns
        customers_df = customers_df.rename(
            columns={
                "CustomerID": "customer_id",  # Map CSV column to DB column
                "Name": "name",
                "Region": "region",
                "JoinDate": "join_date",
                "LoyaltyPoints": "loyalty_points",
                "PreferredContactMethod": "preferred_contact_method"
            }
        )
        customers_df.to_sql("customers", cursor.connection, if_exists="append", index=False)
        logger.info("Customers data inserted into the customers table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting customers: {e}")
        raise

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
        """Insert products data into the products table with explicit mapping."""
    try:
        # Verify if required columns exist in the DataFrame
        required_columns = {"ProductID","ProductName","Category","UnitPrice","StockQuantity","Supplier"}
        if not required_columns.issubset(products_df.columns):
            logger.error(f"Missing columns in products DataFrame: {required_columns - set(products_df.columns)}")
            return

        # Map CSV columns to database table columns
        products_df = products_df.rename(
            columns={
                "ProductID": "product_id",  # Map CSV column to DB column
                "ProductName": "product_name",
                "Category": "category",
                "UnitPrice": "unit_price",
                "StockQuantity": "stock_quantity",
                "Supplier": "supplier"
            }
        )
        products_df.to_sql("products", cursor.connection, if_exists="append", index=False)
        logger.info("Products data inserted into the products table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting products: {e}")
        raise

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
            """Insert sales data into the sales table with explicit mapping."""
    try:
        # Verify if required columns exist in the DataFrame
        required_columns = {"TransactionID","SaleDate","CustomerID","ProductID","StoreID","CampaignID","SaleAmount","DiscountPercent","State"}
        if not required_columns.issubset(sales_df.columns):
            logger.error(f"Missing columns in sales DataFrame: {required_columns - set(sales_df.columns)}")
            return

        # Map CSV columns to database table columns
        sales_df = sales_df.rename(
            columns={
                "TransactionID": "product_id",  # Map CSV column to DB column
                "SaleDate": "product_name",
                "CustomerID": "category",
                "ProductID": "unit_price",
                "StoreID": "stock_quantity",
                "CampaignID": "supplier",
                "SaleAmount": "sale_amount",
                "DiscountPercent": "discount_percent",
                "State": "state"
            }
        )
        sales_df.to_sql("sales", cursor.connection, if_exists="append", index=False)
        logger.info("Sales data inserted into the sales table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting sales: {e}")
        raise


def load_data_to_db() -> None:
    """Load prepared data into the data warehouse using the correct table names."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_PATH)
        # Create a cursor object
        cursor = conn.cursor()

        # Delete existing records in the data warehouse by calling helper function
        delete_existing_records(cursor)

        # Load prepared data using the pandas read_csv() method and pass in path to data file
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))

        # Insert data into the database, pass in the DF with info and the cursor object
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        # Commit and close the connection
        conn.commit()
        conn.close()
        logger.info("Prepared data successfully loaded into the data warehouse.")

    except sqlite3.Error as e:
        logger.error(f"Error during database load: {e}")

    except Exception as e:
        logger.error(f"An unexpected error occurred during ETL: {e}")

    finally:
        if conn:
            conn.close()


# main
def main() -> None:
    """Main function for running the ETL process."""
    logger.info("Starting etl_to_dw ...")
    load_data_to_db()
    logger.info("Finished etl_to_dw complete.")


if __name__ == "__main__":
    main()
