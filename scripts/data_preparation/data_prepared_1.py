import pathlib
import pandas as pd
from statistics import mode

# Directory paths for raw and prepared data
DATA_DIR = pathlib.Path("C:/Users/4harg/Documents/smart-store-marco/data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR = DATA_DIR.joinpath("prepared")

# Ensure the "prepared" folder exists
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Define paths for each dataset
customers_data_path = RAW_DATA_DIR.joinpath("customers_data.csv")
sales_data_path = RAW_DATA_DIR.joinpath("sales_data.csv")
products_data_path = RAW_DATA_DIR.joinpath("products_data.csv")


def load_data(file_path: pathlib.Path) -> pd.DataFrame:
    """Load a CSV file into a pandas DataFrame."""
    return pd.read_csv(file_path)


def clean_customers_data(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Clean customers data by removing records where Name is 'Hermione Grager'."""
    print("Cleaning Customers Data...")
    customers_df = customers_df[customers_df['Name'] != 'Hermione Grager']
    print(f"After Cleaning Customers Data - Columns: {len(customers_df.columns)}, Records: {len(customers_df)}")
    return customers_df


def save_data(df: pd.DataFrame, file_path: pathlib.Path) -> None:
    """Save the cleaned DataFrame to a CSV file."""
    df.to_csv(file_path, index=False)


def clean_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Clean sales data by removing duplicates and handling outliers in DiscountPercent."""
    print("Cleaning Sales Data...")
    sales_df.drop_duplicates(inplace=True)

    # Replace outlier values in 'DiscountPercent' (0 and 100) with the mode
    discount_mode = mode(sales_df['DiscountPercent'])

    # Replace "five" with 5 and convert to numeric
    sales_df['DiscountPercent'] = sales_df['DiscountPercent'].apply(lambda x: 5 if x == "five" else x)
    sales_df['DiscountPercent'] = pd.to_numeric(sales_df['DiscountPercent'], errors='coerce')

    # Replace outliers 0 and 100 with the mode, and fill NaN values (from invalid conversions) with the mode
    sales_df['DiscountPercent'] = sales_df['DiscountPercent'].replace({0: discount_mode, 100: discount_mode}).fillna(discount_mode)

    print(f"After Cleaning Sales Data - Columns: {len(sales_df.columns)}, Records: {len(sales_df)}")
    return sales_df


def clean_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """Clean products data by removing duplicates and filling missing StockQuantity."""
    print("Cleaning Products Data...")
    products_df.drop_duplicates(inplace=True)

    # Replace empty values in 'StockQuantity' with the average rounded to two decimal places
    average_stock = round(products_df['StockQuantity'].mean(), 2)
    products_df['StockQuantity'] = products_df['StockQuantity'].fillna(average_stock)

    print(f"After Cleaning Products Data - Columns: {len(products_df.columns)}, Records: {len(products_df)}")
    return products_df


def process_data():
    """Main function to process all data files."""
    # Load and process customers data
    customers_df = load_data(customers_data_path)
    cleaned_customers_df = clean_customers_data(customers_df)
    prepared_customers_path = PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv")
    save_data(cleaned_customers_df, prepared_customers_path)

    # Load and process sales data
    sales_df = load_data(sales_data_path)
    cleaned_sales_df = clean_sales_data(sales_df)
    prepared_sales_path = PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv")
    save_data(cleaned_sales_df, prepared_sales_path)

    # Load and process products data
    products_df = load_data(products_data_path)
    cleaned_products_df = clean_products_data(products_df)
    prepared_products_path = PREPARED_DATA_DIR.joinpath("products_data_prepared.csv")
    save_data(cleaned_products_df, prepared_products_path)

    print("\nData processing complete.")


if __name__ == "__main__":
    process_data()
