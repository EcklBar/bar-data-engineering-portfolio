from models.database import get_dataframe_from_db, save_dataframe_to_db, is_data_fresh
from models.extract import fetch_raw_json_from_api
from models.transform import process_json_to_dataframe
import pandas as pd

def get_stock_data(symbol: str, time_period: str) -> pd.DataFrame | None:
    """
    Main data retrieval function that orchestrates caching and ETL.

    1. Checks the database for fresh data.
    2. If not found, fetches from API, transforms, and saves to DB.

    Args:
        symbol (str): The stock symbol.
        time_period (str): The API time period (e.g., TIME_SERIES_DAILY).

    Returns:
        pd.DataFrame | None: A clean DataFrame or None if an error occurs.
    """
    # 1. Check cache first
    if is_data_fresh(symbol):
        print(f"Cache hit: Found fresh data for {symbol} in the database.")
        return get_dataframe_from_db(symbol)

    print(f"Cache miss: No fresh data for {symbol}. Fetching from API.")

    # 2. EXTRACT: If no fresh data, fetch from API
    raw_data = fetch_raw_json_from_api(symbol, time_period)
    if not raw_data:
        return None

    # 3. TRANSFORM: Process the raw data
    processed_df = process_json_to_dataframe(raw_data)
    if processed_df is None:
        return None

    # 4. LOAD (to DB): Save the new data to the database for future use
    save_dataframe_to_db(processed_df, symbol)

    return processed_df
