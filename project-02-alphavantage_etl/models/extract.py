import os
from dotenv import load_dotenv
import requests

def fetch_raw_json_from_api(symbol: str, time_period: str) -> dict | None:
    """
    Fetches raw stock data JSON from the Alpha Vantage API.
    This function's sole responsibility is to interact with the external API.
    """
    load_dotenv()
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("ERROR: ALPHA_VANTAGE_API_KEY not found in environment variables.")
        return None

    url = f'https://www.alphavantage.co/query?function={time_period}&symbol={symbol}&apikey={api_key}'
    if time_period == 'TIME_SERIES_INTRADAY':
        url += '&interval=5min'

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()

        # Basic validation of the response
        if "Error Message" in data or "Note" in data:
            print(f"API returned an error or note: {data}")
            return None

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# --- Testing Block ---
def main():
    """
    This function is for testing the get_stock_data function directly.
    It prompts the user for a symbol and time period and prints the raw DataFrame.
    """
    print("--- Running get_stock_data in test mode ---")
    symbol = input("Please enter a stock symbol (e.g., IBM): ")
    time_period = input("Please enter a time period (e.g., TIME_SERIES_DAILY): ")

    # Call the function to get the raw DataFrame
    raw_dataframe = get_stock_data(symbol.upper(), time_period.upper())

    # Check if we got a result and print it
    if raw_dataframe is not None:
        print("\n--- Raw DataFrame successfully fetched: ---")
        print(raw_dataframe.head())
    else:
        print("\n--- Failed to fetch DataFrame. ---")

if __name__ == "__main__":
    main()