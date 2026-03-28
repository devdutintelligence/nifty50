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

# Dynamically create the ("NSE_SYMBOL1", "NSE_SYMBOL2", ...) tuple needed for LTP and OHLC
exchange_symbols = tuple(f"NSE_{symbol}" for symbol in STOCK_LIST)

# 2. Read the API token directly from the environment (No file needed!)
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

    # B. Fetch LTP for all instruments at once
    print("Pulling batched LTP data...")
    ltp_response = groww.get_ltp(
        segment=groww.SEGMENT_CASH,
        exchange_trading_symbols=exchange_symbols
    )

    # C. Fetch OHLC for all instruments at once
    print("Pulling batched OHLC data...")
    ohlc_response = groww.get_ohlc(
        segment=groww.SEGMENT_CASH,
        exchange_trading_symbols=exchange_symbols
    )
    
    print("All data fetched successfully.")

except Exception as e:
    print(f"Error during batched data fetching: {e}")
    exit(1)

# 5. Structure the combined data
structured_data = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_symbols_tracked": len(STOCK_LIST),
    "data": {
        "live_quotes": live_quotes,
        "ltp": ltp_response,
        "ohlc": ohlc_response
    }
}

# 6. OVERWRITE previous data with the latest snapshot
json_filename = 'market_data_50.json'

with open(json_filename, 'w') as json_file:
    json.dump(structured_data, json_file, indent=4)

print(f"Success: Old data deleted. Latest market data saved to {json_filename}")
