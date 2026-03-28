import json
import os
import time
from datetime import datetime
from growwapi import GrowwAPI

# 1. The list of stocks to track
STOCK_LIST = [
    "HDFCBANK", "BHARTIARTL", "RELIANCE", "ICICIBANK", "SBIN", "ONGC", "INFY", 
    "LT", "HCLTECH", "TCS", "AXISBANK", "SHRIRAMFIN", "KOTAKBANK", "M&M", 
    "MARUTI", "BAJFINANCE", "ETERNAL", "BEL", "POWERGRID", "COALINDIA", "NTPC", 
    "TATASTEEL", "INDIGO", "ITC", "GRASIM", "HINDUNILVR", "TMPV", "SUNPHARMA", 
    "WIPRO", "ADANIENT", "ULTRACEMCO", "JIOFIN", "TRENT", "BAJAJFINSV", "TITAN", 
    "APOLLOHOSP", "EICHERMOT", "TECHM", "ADANIPORTS", "HINDALCO", "JSWSTEEL", 
    "BAJAJ-AUTO", "DRREDDY", "CIPLA", "HDFCLIFE", "MAXHEALTH", "ASIANPAINT", 
    "NESTLEIND", "SBILIFE", "TATACONSUM"
]

exchange_symbols = tuple(f"NSE_{symbol}" for symbol in STOCK_LIST)

# 2. Read the API token directly from the environment
API_AUTH_TOKEN = os.getenv('MY_API_KEY')

if not API_AUTH_TOKEN:
    print("Error: The 'MY_API_KEY' environment variable was not found.")
    exit(1)

# 3. Initialize Groww API
print("Initializing Groww API...")
groww = GrowwAPI(API_AUTH_TOKEN)

# 4. Fetch all market data
print(f"Fetching data for {len(STOCK_LIST)} stocks...")
live_quotes = {}
ltp_response = {}
ohlc_response = {}

try:
    # A. Fetch Live Quotes 
    print("Pulling individual live quotes (this may take a moment)...")
    for symbol in STOCK_LIST:
        try:
            live_quotes[symbol] = groww.get_quote(
                exchange=groww.EXCHANGE_NSE,
                segment=groww.SEGMENT_CASH,
                trading_symbol=symbol
            )
            time.sleep(0.1) 
        except Exception as e:
            print(f"  -> Warning: Could not fetch quote for {symbol}: {e}")
            live_quotes[symbol] = None

    # B. Fetch batched LTP and OHLC as a backup/verification
    print("Pulling batched LTP and OHLC data...")
    ltp_response = groww.get_ltp(segment=groww.SEGMENT_CASH, exchange_trading_symbols=exchange_symbols)
    ohlc_response = groww.get_ohlc(segment=groww.SEGMENT_CASH, exchange_trading_symbols=exchange_symbols)
    print("All data fetched successfully.")

except Exception as e:
    print(f"Error during data fetching: {e}")
    exit(1)

# 5. Flatten and Structure the Data for Power BI
print("Structuring data for dashboard integration...")
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
flat_market_data = []

for symbol in STOCK_LIST:
    nse_symbol = f"NSE_{symbol}"
    
    # Safely extract data, defaulting to an empty dictionary if API failed
    quote = live_quotes.get(symbol) or {}
    ltp = ltp_response.get(nse_symbol)
    ohlc = ohlc_response.get(nse_symbol) or {}
    quote_ohlc = quote.get("ohlc") if isinstance(quote.get("ohlc"), dict) else {}

    # Build a clean, single-level record for this specific stock
    stock_record = {
        "timestamp": current_time,
        "symbol": symbol,
        "last_price": ltp if ltp is not None else quote.get("last_price"),
        "day_change": quote.get("day_change"),
        "day_change_perc": quote.get("day_change_perc"),
        "open": ohlc.get("open") if ohlc else quote_ohlc.get("open"),
        "high": ohlc.get("high") if ohlc else quote_ohlc.get("high"),
        "low": ohlc.get("low") if ohlc else quote_ohlc.get("low"),
        "close": ohlc.get("close") if ohlc else quote_ohlc.get("close"),
        "volume": quote.get("volume"),
        "week_52_high": quote.get("week_52_high"),
        "week_52_low": quote.get("week_52_low")
    }
    
    flat_market_data.append(stock_record)

# 6. OVERWRITE previous data with the flat snapshot
json_filename = 'market_data_50.json'

with open(json_filename, 'w') as json_file:
    json.dump(flat_market_data, json_file, indent=4)

print(f"Success: Market data flattened and saved to {json_filename}")
