import argparse
import pathlib
import sys
import pandas as pd
import os


# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger  # noqa: E402
from scripts.data_scrubber import DataScrubber  # noqa: E402

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"Reading raw data from {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs

def save_prepared_data(df: pd.DataFrame, original_file_name: str) -> None:
    """
    Save the prepared DataFrame to the 'prepared' folder, appending 'prepared' to the original file name.

    Parameters:
        df (pd.DataFrame): The prepared DataFrame to save.
        original_file_name (str): The original file name to append 'prepared' to.
    """
    # Define the path to the 'prepared' folder
    prepared_dir = pathlib.Path("C:/Users/4harg/Documents/smart-store-marco/data/prepared")
    prepared_dir.mkdir(parents=True, exist_ok=True)  # Create the directory if it doesn't exist

    # Create the new file name by appending 'prepared' to the original file name (without extension)
    base_name = os.path.splitext(original_file_name)[0]
    prepared_file_name = f"{base_name}_prepared.csv"
    
    # Save the DataFrame to the prepared directory
    prepared_file_path = prepared_dir / prepared_file_name
    df.to_csv(prepared_file_path, index=False)  # Save DataFrame as CSV without the index
    logger.info(f"Prepared data saved to: {prepared_file_path}")

def process_data(file_name: str) -> None:
    """Process raw data by reading it into a pandas DataFrame object."""
    df = read_raw_data(file_name)
    if df.empty:
        logger.warning(f"No data to process for {file_name}.")
        return
    
    logger.info(f"Processing {file_name} with {len(df)} records.")
    # Placeholder for data processing logic here
    
    # After processing, save the data as 'prepared'
    save_prepared_data(df, file_name)

def main(data_files: list = None) -> None:
    """Main function for processing data."""
    data_files = data_files or ["customers_data.csv", "products_data.csv", "sales_data.csv"]
    logger.info("Starting data preparation...")

    for file_name in data_files:
        process_data(file_name)

    logger.info("Data preparation complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process raw data files.")
    parser.add_argument('files', nargs='*', default=[], help="List of raw data filenames.")
    args = parser.parse_args()
    main(args.files if args.files else None)
