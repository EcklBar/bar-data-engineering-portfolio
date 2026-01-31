import pandas as pd

def process_json_to_dataframe(raw_data: dict) -> pd.DataFrame | None:
    """
    Processes the raw stock data JSON from Alpha Vantage into a cleaned pandas DataFrame.
    """
    if not raw_data:
        return None

    time_series_key = next((key for key in raw_data if 'Time Series' in key), None)

    if not time_series_key:
        print(f"Could not find time series data in the API response: {raw_data}")
        return None

    time_series_data = raw_data[time_series_key]
    df = pd.DataFrame.from_dict(time_series_data, orient='index')

    df.rename(columns=lambda col: col.split('. ')[1], inplace=True)
    df.index = pd.to_datetime(df.index)
    df.index.name = 'Date'
    df = df.apply(pd.to_numeric)
    df.sort_index(ascending=True, inplace=True)

    return df

def filter_dataframe_by_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """
    Filters a DataFrame to include only the last N days (rows) of data.
    """
    if df.empty or days <= 0:
        return df
    return df.tail(days)