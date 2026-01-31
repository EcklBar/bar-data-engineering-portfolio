import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os

DB_FILE = "stock_data.db"

def _get_db_connection():
    """Establishes a connection to the SQLite database."""
    return sqlite3.connect(DB_FILE)

def save_dataframe_to_db(df: pd.DataFrame, symbol: str):
    """
    Saves a clean DataFrame to a specific table named after the stock symbol.
    It overwrites the table if it already exists.
    """
    if df is None or df.empty:
        return

    try:
        conn = _get_db_connection()
        # Use the symbol as the table name. Add a metadata table later for more robustness.
        df.to_sql(name=symbol, con=conn, if_exists='replace', index=True)
        print(f"Data for {symbol} saved to database.")
    except Exception as e:
        print(f"Error saving data to DB for {symbol}: {e}")
    finally:
        if conn:
            conn.close()

def get_dataframe_from_db(symbol: str) -> pd.DataFrame | None:
    """
    Retrieves a DataFrame for a given symbol from the database.
    Converts the index back to datetime objects.
    """
    if not os.path.exists(DB_FILE):
        return None

    try:
        conn = _get_db_connection()
        # Check if table exists first to avoid errors
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{symbol}';")
        if cursor.fetchone() is None:
            return None

        df = pd.read_sql_query(f"SELECT * FROM {symbol}", conn, index_col='index')
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Date' # Set index name
        return df
    except Exception as e:
        print(f"Error reading data from DB for {symbol}: {e}")
        return None
    finally:
        if conn:
            conn.close()

def is_data_fresh(symbol: str, max_age_hours: int = 24) -> bool:
    """
    Checks if the data for a symbol in the database is recent enough.
    This basic version checks the file modification time of the DB file.
    A more robust solution would involve a separate metadata table.
    """
    if not os.path.exists(DB_FILE):
        return False

    try:
        # Get the last modification time of the database file
        mod_time = os.path.getmtime(DB_FILE)
        last_modified_date = datetime.fromtimestamp(mod_time)

        # Check if the file was modified within the allowed age
        if datetime.now() - last_modified_date < timedelta(hours=max_age_hours):
             # A simple check: if the DB was updated recently, assume the data might be fresh.
             # This is not perfect, as any write to the DB updates the time.
             # We also need to ensure the table for this symbol actually exists.
            conn = _get_db_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{symbol}';")
            exists = cursor.fetchone() is not None
            conn.close()
            return exists
    except Exception:
        return False

    return False
