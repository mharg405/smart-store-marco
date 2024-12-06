import sys
import pathlib

# Add the parent directory of 'utils' to sys.path
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

# Print sys.path to check
#print(sys.path)

# Import the logger
from utils.logger import logger

# Importing necessary functions from Step 1 to Step 4
from step1_extract import main as step1_main
from step2_transform import process_sales_data
from step3_load import process_and_load_sales_data
from step4_visualize import visualize_sales_count, visualize_cubed_sales_stacked

def main():
    """Main function to run the ETL pipeline."""
    print("Starting the pipeline execution...")

    # Step 1: Data extraction (Read raw sales data)
    print("Starting data extraction...")
    step1_main()  # Calling the main function from Step 1 for data extraction

    # Step 2: Data processing (Clean the sales data)
    print("Starting data processing...")
    process_sales_data()  # Calling the function from Step 2 to clean the sales data

    # Step 3: ETL process (Load processed data into the database)
    print("Starting ETL process...")
    process_and_load_sales_data()  # Calling the function from Step 3 to load data into the database

    # Step 4: Visualization (Visualize the sales data)
    print("Starting data visualization...")
    sales_count_file_path = "path/to/sales_count.csv"  # Set path to sales count CSV file
    cubed_sales_file_path = "path/to/cubed_sales.csv"  # Set path to cubed sales CSV file

    visualize_sales_count(sales_count_file_path)  # Visualize sales count data
    visualize_cubed_sales_stacked(cubed_sales_file_path)  # Visualize cubed sales data

    print("Pipeline execution completed.")

if __name__ == "__main__":
    main()
