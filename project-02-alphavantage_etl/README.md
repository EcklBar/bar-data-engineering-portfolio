# Telegram Stock Bot

This project is a Telegram bot that provides stock charts based on user requests. It's built with a modular ETL (Extract, Transform, Load) architecture, featuring data caching with SQLite to minimize API calls.

## Features

- Fetches stock data from the Alpha Vantage API.
- Caches data in a local SQLite database to improve performance and reduce API usage.
- Processes raw data into a clean, usable format.
- Generates and sends stock chart images directly to the user on Telegram.
- Supports user-friendly commands (e.g., `daily`, `weekly`, or a specific number of days).

## Project Structure

```
.
├── models/
│   ├── database.py       # Handles all SQLite database interactions (caching).
│   ├── extract.py        # Fetches raw JSON data from the Alpha Vantage API.
│   ├── transform.py      # Cleans and processes data into a pandas DataFrame.
│   ├── data_manager.py   # Orchestrates the data flow (cache logic, ETL calls).
│   └── plot.py           # Creates chart images from DataFrames.
├── .env                  # Stores secret keys (API key, bot token). Not version controlled.
├── .gitignore            # Specifies files for Git to ignore.
├── bot.py                # Main application file: runs the Telegram bot.
├── requirements.txt      # Lists project dependencies.
└── stock_data.db         # The SQLite database file (created automatically).
```

## Setup and Installation

### 1. Prerequisites

- Python 3.9+
- A Telegram account
- Alpha Vantage API Key
- Telegram Bot Token

### 2. Get API Keys

- **Alpha Vantage:** Get a free API key from their [website](https://www.alphavantage.co/support/#api-key).
- **Telegram Bot:**
    1. Open Telegram and search for the `@BotFather`.
    2. Send the `/newbot` command and follow the instructions.
    3. BotFather will give you a unique token. Keep it safe.

### 3. Clone and Install

```bash
# Clone the repository (or just use your existing project folder)
# git clone <repository-url>
# cd <repository-name>

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install the required packages
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a file named `.env` in the root directory of the project. Add your secret keys to this file:

```ini
ALPHA_VANTAGE_API_key="YOUR_ALPHA_VANTAGE_API_KEY"
TELEGRAM_BOT_TOKEN="YOUR_TELEGRAM_BOT_TOKEN"
```
**Important:** The `.gitignore` file is configured to ignore the `.env` file, so your keys will not be committed to version control.

## How to Run the Bot

With your virtual environment activated and the `.env` file configured, start the bot with a single command:

```bash
python bot.py
```
You should see a log message in your terminal indicating that the bot has started successfully.

## How to Use the Bot

Open a chat with your bot on Telegram and use the `/stock` command.

- **/start**: Shows the welcome message.
- **/stock `<SYMBOL>` `<PERIOD>`**: Gets a chart for a specific stock.

**`<PERIOD>` can be:**
- `daily`, `weekly`, `monthly`, `intraday`
- A number of days, e.g., `30`, `90`, `365`

**Examples:**
- `/stock NVDA daily`
- `/stock MSFT 90`
- `/stock BTC-USD weekly` (Example for crypto)
