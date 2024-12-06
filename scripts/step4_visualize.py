# Importing necessary libraries for visualization
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from utils.logger import logger

def visualize_sales_count(csv_file_path: pathlib.Path) -> None:
    """Visualize sales count data from CSV."""
    try:
        df = pd.read_csv(csv_file_path)
        df.rename(columns={"sum(Count)": "TotalSalesCount"}, inplace=True)

        plt.figure(figsize=(10, 6))
        sns.barplot(x="ProductID", y="TotalSalesCount", data=df, hue="ProductID", palette="viridis")
        plt.title("Sales Count by Product")
        plt.xlabel("Product ID")
        plt.ylabel("Total Sales Count")
        plt.xticks(rotation=45)
        plt.tight_layout()
        output_plot_path = csv_file_path.parent.joinpath("sales_count_visualization.png")
        plt.savefig(output_plot_path)
        logger.info(f"Sales count visualization saved to: {output_plot_path}")
        plt.show()
    except Exception as e:
        logger.error(f"Error during sales count visualization: {e}")

def visualize_cubed_sales_stacked(cubed_df_path: pathlib.Path) -> None:
    """Visualize cubed sales data as a stacked bar chart."""
    try:
        df = pd.read_csv(cubed_df_path)
        pivot_df = df.pivot_table(index="DayOfWeek", columns="StoreID", values="TotalSales", aggfunc="sum").fillna(0)

        pivot_df.plot(kind="bar", stacked=True, figsize=(10, 6), colormap="viridis")
        plt.title("Total Sales by Day of the Week (Stacked by StoreID)")
        plt.xlabel("Day of Week")
        plt.ylabel("Total Sales")
        plt.xticks(rotation=45)
        plt.legend(title="StoreID", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        plt.show()
    except Exception as e:
        logger.error(f"Error during cubed sales visualization: {e}")
