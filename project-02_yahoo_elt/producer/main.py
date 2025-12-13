import os
import requests
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
if not RAPIDAPI_KEY:
    raise ValueError("RAPIDAPI_KEY environment variable not set")

producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

url = "https://yahoo-finance166.p.rapidapi.com/api/stock/get-financial-data"
headers = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "yahoo-finance166.p.rapidapi.com"
}
symbols = ["AAPL", "GOOGL", "MSFT"]

def fetch_and_produce(symbol):
    querystring = {"symbol": symbol, "region": "US"}
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        print(f"Successfully fetched data for {symbol}")
        producer.send('yahoo_finance_data', {'symbol': symbol, 'data': data})
        producer.flush()
        print(f"Successfully produced data for {symbol} to Kafka")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for {symbol}: {e}")


if __name__ == "__main__":
    while True:
        for symbol in symbols:
            fetch_and_produce(symbol)
        print("Waiting for 10 seconds before next fetch cycle...")
        time.sleep(10)