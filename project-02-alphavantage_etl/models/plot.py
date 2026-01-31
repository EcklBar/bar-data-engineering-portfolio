import io
import pandas as pd
import matplotlib.pyplot as plt

def create_stock_plot(df: pd.DataFrame, symbol: str):
    """
    Creates a plot of the stock's closing price and returns it as a bytes buffer.

    Args:
        df (pd.DataFrame): The processed DataFrame with stock data.
        symbol (str): The stock symbol for the plot title.

    Returns:
        io.BytesIO: A bytes buffer containing the PNG image of the plot.
    """
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(df.index, df['close'], label='Close Price', color='cyan')

    ax.set_title(f'{symbol} Stock Price', fontsize=18)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Price (USD)', fontsize=12)
    ax.legend()

    # Rotate date labels for better readability
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot to a bytes buffer in memory so we can send it via Telegram
    buf = io.BytesIO()
    plt.savefig(buf, format='PNG')
    buf.seek(0)

    # Close the plot to free up memory
    plt.close(fig)

    return buf
