import pandas as pd
from statistics import mode

# Paths to files
customers_data_path = r'C:\Users\4harg\Documents\smart-store-marco\data\raw\customers_data.csv'
sales_data_path = r'C:\Users\4harg\Documents\smart-store-marco\data\raw\sales_data.csv'
products_data_path = r'C:\Users\4harg\Documents\smart-store-marco\data\raw\products_data.csv'
prepared_customers_path = r'C:\Users\4harg\Documents\smart-store-marco\data\prepared\prepared_customers_data.csv'
prepared_sales_path = r'C:\Users\4harg\Documents\smart-store-marco\data\prepared\prepared_sales_data.csv'
prepared_products_path = r'C:\Users\4harg\Documents\smart-store-marco\data\prepared\prepared_products_data.csv'

# 1) Load 'customers_data.csv'
customers_df = pd.read_csv(customers_data_path)
print("Customers Data:")
print(f"Columns: {len(customers_df.columns)}, Records: {len(customers_df)}")

# 2) Remove record where Name is 'Hermione Grager'
customers_df = customers_df[customers_df['Name'] != 'Hermione Grager']

# Count columns and records after cleaning
print(f"After Cleaning Customers Data - Columns: {len(customers_df.columns)}, Records: {len(customers_df)}")

# 3) Save cleaned customers data
customers_df.to_csv(prepared_customers_path, index=False)

# 4) Load 'sales_data.csv'
sales_df = pd.read_csv(sales_data_path)
print("\nSales Data:")
print(f"Columns: {len(sales_df.columns)}, Records: {len(sales_df)}")

# 5) Remove duplicate records
sales_df.drop_duplicates(inplace=True)

# 6) Replace outlier values in 'DiscountPercent' with the mode
discount_mode = mode(sales_df['DiscountPercent'])
sales_df['DiscountPercent'] = sales_df['DiscountPercent'].apply(lambda x: 5 if x == "five" else x)
sales_df['DiscountPercent'] = pd.to_numeric(sales_df['DiscountPercent'], errors='coerce')
sales_df['DiscountPercent'] = sales_df['DiscountPercent'].fillna(discount_mode)

# Count columns and records after cleaning
print(f"After Cleaning Sales Data - Columns: {len(sales_df.columns)}, Records: {len(sales_df)}")

# 7) Save cleaned sales data
sales_df.to_csv(prepared_sales_path, index=False)

# 8) Load 'products_data.csv'
products_df = pd.read_csv(products_data_path)
print("\nProducts Data:")
print(f"Columns: {len(products_df.columns)}, Records: {len(products_df)}")

# 9) Remove duplicate records
products_df.drop_duplicates(inplace=True)

# 10) Replace empty values in 'StockQuantity' with the average rounded to two decimal places
average_stock = round(products_df['StockQuantity'].mean(), 2)
products_df['StockQuantity'] = products_df['StockQuantity'].fillna(average_stock)

# Count columns and records after cleaning
print(f"After Cleaning Products Data - Columns: {len(products_df.columns)}, Records: {len(products_df)}")

# 11) Save cleaned products data
products_df.to_csv(prepared_products_path, index=False)

print("\nData processing complete.")
