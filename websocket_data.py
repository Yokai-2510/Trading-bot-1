# Import necessary modules
import asyncio
import json
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import MarketDataFeed_pb2 as pb
from time import sleep
import pandas as pd
global merged_df
from Instrument_keys import get_instrument_keys
global data_dict
import os

filename =f"access_token.txt"
with open(filename,"r") as file:
    access_token = file.read()

BNDF = get_instrument_keys(access_token)
BNDF = get_instrument_keys(access_token)
BNDF = pd.DataFrame(BNDF)

# Add an entry for 'NSE_INDEX|Nifty Bank'
BNDF.loc[len(BNDF)] = ['NSE_INDEX|Nifty Bank', 'NA', 'NA']
instrument_keys_string = ','.join(['"{}"'.format(key) for key in BNDF['Instrument Key']])
instrument_keys_list = BNDF['Instrument Key'].tolist()

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

async def fetch_market_data():
    global data_dict
    """Fetch market data using WebSocket and print it."""

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    api_version = '2.0'
    configuration.access_token = access_token

    # Get market data feed authorization
    response = get_market_data_feed_authorize(
        api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys":instrument_keys_list
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)

            # Print the dictionary representation
            # print(json.dumps(data_dict))


# Execute the function to fetch market data
# asyncio.run(fetch_market_data())
def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(fetch_market_data())

# Start the WebSocket connection in a separate thread
websocket_thread = Thread(target=run_websocket)
websocket_thread.start()
previous_instrument_data = None  # To store the previous state of instrument data
df_instruments = pd.DataFrame()  # Declare df_instruments outside the loop
output_filename = "websocket_df.csv"
data_dict = {}  # Declare data_dict as a global variable
instrument_keys_data = {}  # Keep track of instrument keys and their latest values

sleep(3)

while True:
    sleep(0.1)
    instrument_data = []

    for instrument_key in instrument_keys_list:
        instrument_info = data_dict.get("feeds", {}).get(instrument_key, {})
        
        # Retrieve the last known values if the instrument key is not present in the current iteration
        if not instrument_info:
            last_known_values = instrument_keys_data.get(instrument_key, {})
            ltp = last_known_values.get("LTP", "NA")
            theta = last_known_values.get("Theta", "NA")
            delta = last_known_values.get("Delta", "NA")
        else:
            if instrument_key == "NSE_INDEX|Nifty Bank":
                ltp = instrument_info.get("ff", {}).get("indexFF", {}).get(
                    "ltpc", {}
                ).get("ltp")
            else:
                ltp = instrument_info.get("ff", {}).get("marketFF", {}).get(
                    "ltpc", {}
                ).get("ltp")

            theta = instrument_info.get("ff", {}).get("marketFF", {}).get(
                "optionGreeks", {}
            ).get("theta")
            delta = instrument_info.get("ff", {}).get("marketFF", {}).get(
                "optionGreeks", {}
            ).get("delta")

        # Update the instrument keys data
        instrument_keys_data.setdefault(instrument_key, {})
        instrument_keys_data[instrument_key]["LTP"] = ltp
        instrument_keys_data[instrument_key]["Theta"] = theta
        instrument_keys_data[instrument_key]["Delta"] = delta

        instrument_data.append({
            "Instrument Key": instrument_key,
            "LTP": ltp,
            "Theta": theta,
            "Delta": delta,
        })

    df_instruments = pd.DataFrame(instrument_data)
    df_instruments = pd.merge(df_instruments, BNDF, on="Instrument Key", how="right")
    df_instruments.to_csv(output_filename, mode='w', index=False)
