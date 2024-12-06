import pathlib
import sys
import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, month, year, sum as _sum, count as _count
from utils.logger import logger  # noqa: E402

# Constants
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DW_DIR: pathlib.Path = pathlib.Path("data").joinpath("dw")
DB_PATH = PROJECT_ROOT.joinpath("data", "dw", "smart_sales.db")
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")

# Create output directory if it does not exist
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OLAP Cubing") \
    .getOrCreate()

def ingest_sales_data_from_dw():
    """Ingest sales data from SQLite data warehouse into a Spark DataFrame."""
    try:
        # Read data from SQLite database into a Spark DataFrame
        sales_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{DB_PATH}") \
            .option("dbtable", "sales") \
            .load()

        logger.info("Sales data successfully loaded from SQLite data warehouse.")
        return sales_df
    except Exception as e:
        logger.error(f"Error loading sale table data from data warehouse: {e}")
        raise

def create_olap_cube(sales_df, dimensions, metrics):
    """Create an OLAP cube by aggregating data across multiple dimensions."""
    try:
        # Group by the specified dimensions and aggregate metrics
        group_by_cols = [col(dim) for dim in dimensions]
        agg_exprs = [*[
            _sum(col(metric)).alias(f"{metric}_sum") if isinstance(agg_func, str) and agg_func == 'sum' else 
            _sum(col(metric)).alias(f"{metric}_sum") if isinstance(agg_func, list) and 'sum' in agg_func else
            _count(col(metric)).alias(f"{metric}_count") for metric, agg_func in metrics.items()
        ]]
        
        # Create the OLAP cube
        cube = sales_df.groupBy(*group_by_cols).agg(*agg_exprs)
        
        # Log the cube creation
        logger.info(f"OLAP cube created with dimensions: {dimensions}")
        return cube
    except Exception as e:
        logger.error(f"Error creating OLAP cube: {e}")
        raise

def write_cube_to_csv(cube, filename):
    """Write the OLAP cube to a CSV file."""
    try:
        output_path = OLAP_OUTPUT_DIR.joinpath(filename)
        cube.coalesce(1).write.option("header", "true").csv(str(output_path))
        logger.info(f"OLAP cube saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving OLAP cube to CSV file: {e}")
        raise

def plot_low_performing_products(low_performing_products):
    """Plot a bar chart of low-performing products and save it as an image file."""
    try:
        # Convert the Spark DataFrame to Pandas for plotting
        low_performing_pd = low_performing_products.toPandas()

        # Define the file path where the image will be saved
        image_path = r"C:\Users\4harg\OneDrive\Documents\smart-store-marco\images\olapCubeGoals.png"
        
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(image_path), exist_ok=True)
        
        # Plot the data using Matplotlib
        plt.figure(figsize=(10, 6))
        plt.bar(low_performing_pd['ProductID'], low_performing_pd['SaleAmount_sum'], color='red')
        plt.xlabel('Product ID')
        plt.ylabel('Total Sales Amount')
        plt.title('Low-Performing Products (Total Sales < $100)')
        plt.xticks(rotation=90)  # Rotate product IDs for better visibility
        plt.tight_layout()
        
        # Save the plot to the specified image path
        plt.savefig(image_path)
        plt.close()  # Close the plot to avoid it showing in the GUI
        logger.info(f"Bar chart saved successfully to {image_path}.")
    except Exception as e:
        logger.error(f"Error saving the bar chart: {e}")
        raise

def main():
    """Main function for OLAP cubing."""
    logger.info("Starting OLAP Cubing process...")

    # Step 1: Ingest sales data
    sales_df = ingest_sales_data_from_dw()

    # Step 2: Convert SaleDate to datetime format (handled by Spark's date functions)
    sales_df = sales_df.withColumn("SaleDate", col("SaleDate").cast("timestamp"))

    # Step 3: Add additional columns for time-based dimensions
    sales_df = sales_df.withColumn("DayOfWeek", dayofweek(col("SaleDate"))) \
                       .withColumn("Month", month(col("SaleDate"))) \
                       .withColumn("Year", year(col("SaleDate")))

    # Step 4: Define dimensions and metrics for the cube
    dimensions = ["DayOfWeek", "ProductID", "CustomerID"]
    metrics = {
        "SaleAmount": "sum",
        "TransactionID": "count"
    }

    # Step 5: Create the cube
    olap_cube = create_olap_cube(sales_df, dimensions, metrics)

    # Step 6: Filter low-performing products
    low_sales_threshold = 100  # Total sales below $100
    low_transactions_threshold = 5  # Fewer than 5 transactions
    low_performing_products = olap_cube.filter(
        (col("SaleAmount_sum") < low_sales_threshold) & 
        (col("TransactionID_count") < low_transactions_threshold)
    )

    # Step 7: Sort by lowest total sales
    low_performing_products = low_performing_products.orderBy("SaleAmount_sum")

    # Step 8: Add a count of how many times each product appears in the final list
    low_performing_products = low_performing_products.withColumn("ProductCount", 
        low_performing_products.groupBy("ProductID").count())

    # Step 9: Sort by ProductCount in descending order
    low_performing_products = low_performing_products.orderBy(col("ProductCount").desc())

    # Step 10: Save the cube to a CSV file
    write_cube_to_csv(low_performing_products, "olap_goal_cube.csv")

    # Step 11: Plot the results and save as an image
    plot_low_performing_products(low_performing_products)

    logger.info("OLAP Cubing process completed successfully.")
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")

if __name__ == "__main__":
    main()
