import pandas as pd
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
PREPARED_DATA_DIR: pathlib.Path = pathlib.Path("C:/Users/4harg/OneDrive/Documents/smart-store-marco/data/prepared")
DB_PATH: str = "C:/Users/4harg/OneDrive/Documents/smart-store-marco/data/dw/smart_sales.db"

CUSTOMERS_DATA = PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv")
PRODUCTS_DATA = PREPARED_DATA_DIR.joinpath("products_data_prepared.csv")
SALES_DATA = PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv")


def extract_data(file_path: pathlib.Path) -> pd.DataFrame:
    """Extract data from a CSV file."""
    logger.info(f"Extracting data from {file_path} ...")
    try:
        data = pd.read_csv(file_path)
        logger.info(f"Successfully extracted data from {file_path}.")
        return data
    except Exception as e:
        logger.error(f"Error extracting data from {file_path}: {e}")
        raise


def load_data_to_db() -> None:
    """Load prepared data into the data warehouse using the correct table names."""
    try:
        # Ensure the parent directory for the database exists
        db_dir = pathlib.Path(DB_PATH).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # Extract data
        customers_data = extract_data(CUSTOMERS_DATA)
        products_data = extract_data(PRODUCTS_DATA)
        sales_data = extract_data(SALES_DATA)

        # Connect to the SQLite database
        logger.info(f"Connecting to database at {DB_PATH} ...")
        conn = sqlite3.connect(DB_PATH)

        # Load data into respective tables
        logger.info("Loading data into database tables ...")
        customers_data.to_sql("customers", conn, if_exists="replace", index=False)
        logger.info("Loaded customers data into 'customers' table.")

        products_data.to_sql("products", conn, if_exists="replace", index=False)
        logger.info("Loaded products data into 'products' table.")

        sales_data.to_sql("sales", conn, if_exists="replace", index=False)
        logger.info("Loaded sales data into 'sales' table.")

        # Close connection
        conn.commit()
        conn.close()
        logger.info("Data successfully loaded into database and connection closed.")

    except Exception as e:
        logger.error(f"Error during ETL: {e}")
        raise


# main
def main() -> None:
    """Main function for running the ETL process."""
    logger.info("Starting etl_to_dw ...")
    load_data_to_db()
    logger.info("Finished etl_to_dw complete.")


if __name__ == "__main__":
    main()
