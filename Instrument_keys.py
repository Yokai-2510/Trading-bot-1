import requests as rq
import pandas as pd
from pprint import pprint

def get_open_value(access_token):
    # Fetching open value from market quotes
    url = "https://api.upstox.com/v2/market-quote/quotes"
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Authorization': f'Bearer {access_token}'
    }
    payload = {'symbol': "NSE_INDEX|Nifty Bank"}
    response = rq.get(url, headers=headers, params=payload)
    response_data = response.json()
    open_value = response_data['data']['NSE_INDEX:Nifty Bank']['ohlc']['open']
    return open_value

def get_instrument_keys(access_token):
    open_value = get_open_value(access_token)  # Call the get_open_value function with the access_token
    print(open_value)
    rounded_open = round(open_value / 100) * 100
    strike_price_cap = 100
    upper_limit_ikey = rounded_open + strike_price_cap
    lower_limit_ikey = rounded_open - strike_price_cap

    # Reading instrument data from CSV
    df = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")

    # Filtering instruments based on criteria
    BNDF = df[(df['exchange'] == 'NSE_FO') & 
              (df['instrument_type'] == 'OPTIDX') & 
              (df['lot_size'] == 15) & 
              (df['option_type'].isin(['CE', 'PE']))]

    BNDF = BNDF.sort_values(by='expiry')
    min_expiry = min(BNDF['expiry'].unique())
    BNDF = BNDF[BNDF['expiry'] == min_expiry][['instrument_key', 'strike', 'option_type']]
    BNDF = BNDF.rename(columns={'instrument_key': 'Instrument Key', 'option_type': 'symbol'})
    
    # Filter based on strike price limits
    BNDF['strike'] = pd.to_numeric(BNDF['strike'], errors='coerce')  # Convert 'strike' column to numeric, handle errors by converting to NaN
    BNDF = BNDF.dropna(subset=['strike'])  # Drop rows with NaN in 'strike' column
    BNDF['strike'] = BNDF['strike'].round(-2)  # Round to the nearest 100
    BNDF = BNDF[(BNDF['strike'] >= lower_limit_ikey) & (BNDF['strike'] <= upper_limit_ikey)]
    
    # Formatting instrument keys
    instrument_keys_list = ['NSE_INDEX|Nifty Bank'] + BNDF['Instrument Key'].tolist()    
    return BNDF 

if __name__ == "__main__":
    # Example usage
    filename = "access_token.txt"
    with open(filename, "r") as file:
        access_token = file.read()
    open_value = get_open_value
    BNDF = get_instrument_keys(access_token)
    BNDF = pd.DataFrame(BNDF)
    print(BNDF)
