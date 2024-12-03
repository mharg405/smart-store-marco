import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.logger import logger  # noqa: E402

# Constants
DW_DIR: pathlib.Path = pathlib.Path("data").joinpath("dw")
DB_PATH = PROJECT_ROOT.joinpath("data", "dw", "smart_sales.db")
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")

# Create output directory if it does not exist
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def ingest_sales_data_from_dw() -> pd.DataFrame:
    """Ingest sales data from SQLite data warehouse."""
    try:
        conn = sqlite3.connect(DB_PATH)
        sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
        conn.close()
        logger.info("Sales data successfully loaded from SQLite data warehouse.")
        return sales_df
    except Exception as e:
        logger.error(f"Error loading sale table data from data warehouse: {e}")
        raise

def create_olap_cube(sales_df: pd.DataFrame, dimensions: list, metrics: dict) -> pd.DataFrame:
    """Create an OLAP cube by aggregating data across multiple dimensions."""
    try:
        # Group by the specified dimensions and aggregate metrics
        grouped = sales_df.groupby(dimensions)
        cube = grouped.agg(metrics).reset_index()
        
        # Add a list of sale IDs for traceability
        cube["sale_ids"] = grouped["TransactionID"].apply(list).reset_index(drop=True)
        
        # Generate explicit column names
        explicit_columns = generate_column_names(dimensions, metrics)
        explicit_columns.append("sale_ids")
        cube.columns = explicit_columns

        # Round numeric columns to two decimal places
        for col in explicit_columns:
            if "sum" in col or "mean" in col:
                cube[col] = cube[col].round(2)
        
        logger.info(f"OLAP cube created with dimensions: {dimensions}")
        return cube
    except Exception as e:
        logger.error(f"Error creating OLAP cube: {e}")
        raise

def generate_column_names(dimensions: list, metrics: dict) -> list:
    """Generate explicit column names for OLAP cube, ensuring no trailing underscores."""
    column_names = dimensions.copy()
    for column, agg_funcs in metrics.items():
        if isinstance(agg_funcs, list):
            for func in agg_funcs:
                column_names.append(f"{column}_{func}")
        else:
            column_names.append(f"{column}_{agg_funcs}")
    return column_names

def write_cube_to_csv(cube: pd.DataFrame, filename: str) -> None:
    """Write the OLAP cube to a CSV file."""
    try:
        output_path = OLAP_OUTPUT_DIR.joinpath(filename)
        cube.to_csv(output_path, index=False)
        logger.info(f"OLAP cube saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving OLAP cube to CSV file: {e}")
        raise

def main():
    """Main function for OLAP cubing."""
    logger.info("Starting OLAP Cubing process...")
    
    # Step 1: Ingest sales data
    sales_df = ingest_sales_data_from_dw()
    
    # Step 2: Convert SaleDate to datetime format
    sales_df["SaleDate"] = pd.to_datetime(sales_df["SaleDate"], errors='coerce')
    
    # Step 3: Add additional columns for time-based dimensions
    sales_df["DayOfWeek"] = sales_df["SaleDate"].dt.day_name()
    sales_df["Month"] = sales_df["SaleDate"].dt.month
    sales_df["Year"] = sales_df["SaleDate"].dt.year
    
    # Step 4: Define dimensions and metrics for the cube
    dimensions = ["DayOfWeek", "ProductID", "CustomerID"]
    metrics = {
        "SaleAmount": ["sum", "mean"],
        "TransactionID": "count"
    }
    
    # Step 5: Create the cube
    olap_cube = create_olap_cube(sales_df, dimensions, metrics)
    
    # Step 6: Filter low-performing products
    low_sales_threshold = 100  # Total sales below $100
    low_transactions_threshold = 5  # Fewer than 5 transactions
    low_performing_products = olap_cube[
        (olap_cube["SaleAmount_sum"] < low_sales_threshold) &
        (olap_cube["TransactionID_count"] < low_transactions_threshold)
    ]
    
    # Step 7: Sort by lowest total sales
    low_performing_products = low_performing_products.sort_values(by="SaleAmount_sum", ascending=True)
    
    # Step 8: Add a count of how many times each product appears in the final list
    low_performing_products["ProductCount"] = low_performing_products.groupby("ProductID")["ProductID"].transform("size")
    
    # Step 9: Sort by ProductCount in descending order
    low_performing_products = low_performing_products.sort_values(by="ProductCount", ascending=False)
    
    # Step 10: Save the cube to a CSV file
    write_cube_to_csv(low_performing_products, "olap_goal_cube.csv")

    logger.info("OLAP Cubing process completed successfully.")
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")

if __name__ == "__main__":
    main()
