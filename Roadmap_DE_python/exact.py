import pandas as pd
import os
from datetime import datetime

# File path to the source CSV
source_csv_path = '/home/wipawee/Roadmap_DE_python/olist_order_items_dataset.csv'
raw_data_path = '/home/wipawee/Roadmap_DE_python/raw'
csv_file_path = 'raw_order_items_dataset.csv'

expected_columns = ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value']

# Load the CSV file into a DataFrame
def load_source_data(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

# Check for missing or duplicated data
def validate_schema(df: pd.DataFrame, expected_columns: list[str]) -> None:
    expected_set = set(expected_columns)
    actual_set = set(df.columns)

    missing = expected_set - actual_set
    extra = actual_set - expected_set
    
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if extra:
        raise ValueError(f"Unexpected columns: {extra}")

# Check for schema consistency (column names)
def validate_columns(df: pd.DataFrame) -> None:
    if df.duplicated().any():
        raise ValueError("DataFrame contains duplicated rows.")
    if df.isnull().any().any():
        raise ValueError("DataFrame contains null values.")
    if df['price'].lt(0).any():
        raise ValueError("DataFrame contains negative prices.")
    today = pd.Timestamp.today().normalize()
    if (df['shipping_limit_date'] < today).any():
        raise ValueError("DataFrame contains invalid shipping limit dates.")

#normalize 'price', 'order_item_id' and 'freight_value' columns
def normalize_data_types(df: pd.DataFrame) -> pd.DataFrame:
    df['price'] = df['price'].astype(float)
    df['order_item_id'] = df['order_item_id'].astype(int)
    df['freight_value'] = df['freight_value'].astype(float)
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='raise')
    return df

# csv file path to be used in further analysis
def save_data(df: pd.DataFrame, output_path: str, filename: str) -> None:
    temp_file = filename
    dst_path = os.path.join(output_path, temp_file)
    df.to_csv(temp_file, index=False)
    os.replace(temp_file, dst_path)
    
# Main execution
def main():
    df = load_source_data(source_csv_path)
    validate_schema(df, expected_columns)
    validate_columns(df)
    df = normalize_data_types(df)
    save_data(df, raw_data_path, csv_file_path)
    
if __name__ == "__main__":
    main()