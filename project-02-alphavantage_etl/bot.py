import os
import logging
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Import the main data orchestrator and other modules
from models.data_manager import get_stock_data
from models.transform import filter_dataframe_by_days
from models.plot import create_stock_plot

# --- Setup ---
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- Mappings ---
TIME_PERIOD_MAP = {
    "DAILY": "TIME_SERIES_DAILY",
    "WEEKLY": "TIME_SERIES_WEEKLY",
    "MONTHLY": "TIME_SERIES_MONTHLY",
    "INTRADAY": "TIME_SERIES_INTRADAY",
}

# --- Bot Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message."""
    await update.message.reply_text(
        "Welcome to the Stock Bot!\n\n"
        "Use /stock <SYMBOL> <PERIOD>\n"
        "PERIOD can be `daily`, `weekly`, `monthly`, or a number of days (e.g., `90`).\n\n"
        "Examples:\n/stock IBM daily\n/stock TSLA 90"
    )

async def stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /stock command and orchestrates the data pipeline."""
    if len(context.args) != 2:
        await update.message.reply_text("Invalid format. Use: /stock <SYMBOL> <PERIOD>")
        return

    symbol = context.args[0].upper()
    period_arg = context.args[1]

    api_function = ""
    days_to_filter = 0

    if period_arg.isdigit():
        api_function = "TIME_SERIES_DAILY"
        days_to_filter = int(period_arg)
    else:
        api_function = TIME_PERIOD_MAP.get(period_arg.upper())

    if not api_function:
        await update.message.reply_text(f"Invalid period '{period_arg}'. Use `daily`, `weekly`, `monthly`, or a number of days.")
        return

    await update.message.reply_text(f"Processing request for {symbol}... One moment.")

    try:
        # 1. Get data (from cache or new ETL)
        logger.info(f"Requesting data for {symbol}...")
        stock_df = get_stock_data(symbol, api_function)

        if stock_df is None or stock_df.empty:
            await update.message.reply_text(f"Sorry, I couldn't get data for {symbol}. The symbol may be invalid or the API limit was reached.")
            return

        # 2. Filter if needed
        if days_to_filter > 0:
            stock_df = filter_dataframe_by_days(stock_df, days_to_filter)

        # 3. Create plot
        plot_buffer = create_stock_plot(stock_df, symbol)

        # 4. Send plot
        await update.message.reply_photo(photo=plot_buffer, caption=f"Chart for {symbol} ({period_arg.lower()}).")

    except Exception as e:
        logger.error(f"Error in /stock command for {symbol}: {e}", exc_info=True)
        await update.message.reply_text("An unexpected error occurred. Please try again later.")

# --- Main Bot Execution ---
def main() -> None:
    """Sets up and runs the Telegram bot."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.critical("FATAL: TELEGRAM_BOT_TOKEN not found in .env file.")
        return

    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stock", stock))

    logger.info("Bot is starting...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
