from urllib.parse import quote, urlparse, parse_qs
from playwright.sync_api import Playwright, sync_playwright
from credentials import API_KEY, SECRET_KEY, RURL, TOTP_KEY, MOBILE_NO, PIN
import requests as rq
import pyotp
import pandas as pd
from datetime import datetime, timedelta, time
import time as t
import json
import math
from Login import get_access_token
from Instrument_keys import get_instrument_keys
from Instrument_keys import get_open_value
import subprocess

access_token = get_access_token()
websocket_file_path = 'websocket_df.csv'

filename = "access_token.txt"
with open(filename, "r") as file:
    access_token = file.read()

filename = "config.txt"
config_dict = {}
with open(filename, "r") as file:
    for line in file:
        # Skip comments (lines starting with '#')
        if line.startswith('#'):
            continue
        # Split the line into key and value
        parts = line.strip().split("=")
        # Check if there are enough parts
        if len(parts) == 2:
            key, value = parts
            config_dict[key.strip()] = value.strip()
        elif len(parts) == 1:
            # Handle lines with only one value
            key = parts[0].strip()
            config_dict[key] = None  # You can set a default value or leave it as None

# Access the configuration parameters
hours = int(config_dict.get('hours', 9))
minutes = int(config_dict.get('minutes', 15))
seconds = int(config_dict.get('seconds', 2))
sell_time_condition = int(config_dict.get('sell_time_condition', 5))
Target = int(config_dict.get('Target', 35))
stop_loss = int(config_dict.get('stop_loss', 13))
quantity = int(config_dict.get('quantity', 1))
MACD_symbol = config_dict.get('MACD_symbol', 'CE')

count = 1
start_time = datetime.now().replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
end_time = start_time + timedelta(seconds=2)

# Now you can use these variables in your script
print("Entry Time:")
print(f"   Hours: {hours}\n   Minutes: {minutes}\n   Seconds: {seconds}")
print(f"Sell Time Condition: {sell_time_condition}")
print(f"Target: {Target}")
print(f"Stop Loss: {stop_loss}")
print(f"Quantity: {quantity}")
print(f"MACD Symbol: {MACD_symbol}")

def place_order(instrument_key, quantity, transaction_type, access_token):
    url = "https://api.upstox.com/v2/order/place"
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    payload = {
        "quantity": quantity,
        "product": "I",
        "validity": "DAY",
        "price": 0,
        "tag": "string",
        "instrument_token": instrument_key,
        "order_type": "MARKET",
        "transaction_type": transaction_type,
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False
    }
    data = json.dumps(payload)
    response = rq.post(url, headers=headers, data=data)
    return response.json()

def retry_read_csv(websocket_file_path, max_attempts=200, retry_interval=0.01):
    attempts = 0
    while attempts < max_attempts:
        try:
            dataframe = pd.read_csv(websocket_file_path)
            return dataframe
        except pd.errors.EmptyDataError:
            # EmptyDataError indicates an empty or improperly formatted file
            attempts += 1
            t.sleep(retry_interval)
    raise RuntimeError(f"Failed to read CSV file after {max_attempts} attempts")

# Define the target time (9:15 AM)
target_time = datetime.now().replace(hour=9, minute=10, second=2, microsecond=0)
count_y = 1
while True:
    current_time = datetime.now()
    # Check if the current time is greater than or equal to 9:15
    if current_time >= target_time:
        print("Market is Open . Connecting to Websocket.")
        break
    eta = target_time - current_time
    # Print the ETA every 30 seconds
    if count_y % 10 == 0 :
        print(f"Time left for Market Open: {eta}")
    count_y = count_y + 1    
    t.sleep(0.5)

process = subprocess.Popen(["python", "websocket_data.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, shell=True, start_new_session=True)
t.sleep(5)
print("Websocket Connected")
open_value = get_open_value(access_token)
BNDF = get_instrument_keys(access_token)


if MACD_symbol == 'CE':
    strike_price_ce = math.ceil(open_value / 100) * 100
    print("Strike price and symbol for Entry: " + str(strike_price_ce) + " " + MACD_symbol)
    pref_ikey_open = BNDF[(BNDF['strike'] == strike_price_ce) & (BNDF['symbol'] == 'CE')]
elif MACD_symbol == 'PE':
    strike_price_pe = math.floor(open_value / 100) * 100
    print("Strike price and symbol for Entry: " + str(strike_price_pe) + " " + MACD_symbol)
    pref_ikey_open = BNDF[(BNDF['strike'] == strike_price_pe) & (BNDF['symbol'] == 'PE')]
instrument_key = str(pref_ikey_open.iloc[0]['Instrument Key'])
# Display the pref_ikey_open
print("Preferred Instrument key:")
print(instrument_key)

while True:
    t.sleep(0.1)
    current_time = datetime.now()
    count = count + 1
    if start_time <= current_time <= end_time:
        transaction_type = 'BUY'
        response = place_order(instrument_key, quantity, transaction_type, access_token)
        websocket_df = retry_read_csv(websocket_file_path)
        instrument_row = websocket_df[websocket_df['Instrument Key'] == instrument_key]
        open_ltp_buy = instrument_row['LTP'].values[0]
        print("LTP bought at :")
        print(open_ltp_buy)
        if response['status'] == 'success':
            print(response)
            order_placed = True
            buy_time = datetime.now()
            print("Order placed successfully.")
            # You may print additional information if needed
        else:
            print("Order placement failed.")
        break  # Break out of the loop after placing the order
    else:
        if count % 10 == 0:
            remaining_time = start_time - datetime.now()
            print(f"Time remaining for entry: {remaining_time}")
count = 1
while True:
    t.sleep(0.05)
    transaction_type = 'SELL'
    if order_placed:
        try:
            # Read the current LTP (Last Traded Price) for the instrument_key with retry
            websocket_df = retry_read_csv(websocket_file_path)
            instrument_row = websocket_df[websocket_df['Instrument Key'] == instrument_key]
            current_ltp_sell = instrument_row['LTP'].values[0]
            if count % 30 == 0:
                print(f"Current LTP of selected instrument: {current_ltp_sell}, Checking conditions for exit")
            count = count + 1
            # Check selling conditions
            if current_ltp_sell > open_ltp_buy + Target or current_ltp_sell < open_ltp_buy - stop_loss or datetime.now() > (buy_time + timedelta(minutes=sell_time_condition)):
                response = place_order(instrument_key, quantity, transaction_type, access_token)
                print(response)
                print(f"Sell order placed - Condition {'1' if current_ltp_sell > open_ltp_buy + Target else '2' if current_ltp_sell < open_ltp_buy - stop_loss else '3'}")
                break  # Exit the loop after placing the sell order
        except RuntimeError as e:
            print(f"Error: {e}")
            continue
    if not order_placed:
        print("Buy order not placed")
        break
print("Condition 1 - Target Hit")
print("Condition 2 - SL Hit")
print("Condition 3 - Square off time")
process.terminate()
