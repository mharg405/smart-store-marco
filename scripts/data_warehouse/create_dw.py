import sqlite3
import sys
import pathlib

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger  # noqa: E402

# Constants
DW_DIR: pathlib.Path = pathlib.Path("data").joinpath("dw")
DB_PATH: pathlib.Path = DW_DIR.joinpath("smart_sales.db")

# Ensure the 'data/dw' directory exists
DW_DIR.mkdir(parents=True, exist_ok=True)


def create_dw() -> None:
    """Create the data warehouse by creating customer, product, and sale tables."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # SQL for creating the 'customers' dimension table
        create_customers_table = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT NOT NULL,
            join_date DATE NOT NULL,
            email TEXT NOT NULL
        );
        """

        # SQL for creating the 'products' dimension table
        create_products_table = """
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            supplier TEXT NOT NULL,
            unit_price REAL NOT NULL
        );
        """

        # SQL for creating the 'sales' fact table
        create_sales_table = """
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            customer_id TEXT,
            product_id TEXT,
            quantity INTEGER NOT NULL,
            sales_amount REAL NOT NULL,
            payment_type TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """

        # Execute the SQL statements to create the tables
        cursor.execute(create_customers_table)
        cursor.execute(create_products_table)
        cursor.execute(create_sales_table)

        # Commit the changes to the database
        conn.commit()

        # Close the connection
        conn.close()
        logger.info("Data warehouse created successfully.")

    except sqlite3.Error as e:
        logger.error(f"Error connecting to the database: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

def main() -> None:
    """Main function to create the data warehouse."""
    logger.info("Starting data warehouse creation...")
    create_dw()
    logger.info("Data warehouse creation complete.")

if __name__ == "__main__":
    main()
