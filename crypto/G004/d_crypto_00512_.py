# ****************************************************************************************
#   0506 Specific TS methods 
#   0507 Standardised TS method
#   0508 Minor upgrades to 0507
#   0509 Parametized
#   0510 Fix ts json
#   0510 Force Liqs
#   0511 Prod prep and EH-Disconnect
# ****************************************************************************************

# ------------------------------------------------------------------------------------
# 0101. self.messages = deque()
# 0200. def process_message(self, message)
# 0201. data = json.loads(message)
# 0202. self.messages.append(data)
# 0203. def extract_timestamp_and_data()

# 0301. def process_batches(self):
# 0302. time.sleep(3600) 
# 0303. self.process_hourly_batch()
# 0310. def process_hourly_batch(self)
# 0311. depth_data = [], trade_data = [], kline_data = [], agg_trade_data = []
# 0312. for data in; if in stream_type; l all_data: append
# 0313. if depth_data: pd.concat(...

# 0411. def fetch_and_process_order_book
# 0421. def create_df_l2_02
# 0431. def create_df_l2b
# 0441. def create_df_trade
# 0451. def create_df_agg_trade

# 0501. def create_trade_df
# 0502. def create_df_liq, 
# 0503. def create_order_side_df

# 0601. def convert_unix_to_est .                            

# 1101. async def consume_message
# 1101. def start_stream(
    # async with websockets.connect(uri) as websocket:
    #     async for message in websocket:
    #         processor.process_message(message)

# 1201. if __name__ == "__main__":    
# ------------------------------------------------------------------------------------


import os
import json
import pandas as pd
import pytz
from datetime import datetime
import logging
import threading
import time
from collections import deque
import requests
import asyncio
import websockets
import argparse

global yprint
yprint=True

def xprint(x):
    if yprint==True:
         print(x) 

class BinanceStreamProcessor:
    def __init__(self, ticker, dir_updates_root, db_ver, batch_seconds,timezone="US/Eastern"):
        self.ticker = ticker
        self.dir_updates_root = dir_updates_root
        self.db_ver = db_ver
        self.batch_seconds = batch_seconds
        self.timezone = pytz.timezone(timezone)
        self.messages = deque()
        self.lock = threading.Lock()
        logging.basicConfig(filename=f"ws_bin_{self.ticker}.log", level=logging.DEBUG, format='%(asctime)s %(message)s')
        self.d_thresh = {
                        'BTCUSDT' : 500000,
                        'ETHUSDT' : 100000,
                        'SOLUSDT' : 10000, 
                    }

        # Create directory for feather files if it doesn't exist
        if not os.path.exists(self.dir_updates_root):
            os.makedirs(self.dir_updates_root)

        # Fetch and process the L2 order book snapshot immediately
        self.fetch_and_process_order_book()

        # Start the batching thread
        self.batching_thread = threading.Thread(target=self.process_batches)
        self.batching_thread.daemon = True
        self.batching_thread.start()

    def log_data(self, data):
        logging.debug(json.dumps(data, indent=4))

# ==================================================================================
#   GET message CONVERT to JSON & APPEND to dq buffer object
# ==================================================================================

    ### Append message to dq
    def process_message(self, message):
        data = json.loads(message)
        # data['ts_e_est'] = self.extract_ts_ws(data)
        data['ts_p_est'] = datetime.now().astimezone(self.timezone).strftime('%Y-%m-%d %H:%M:%S.%f')
        with self.lock:
            self.messages.append(data)

    ### Extract data 
    def extract_ts_ws(self, data):
        # Extract timestamp from within the message's data fields
        event_time = None
        if 'data' in data:
            event_data = data['data']
            if 'E' in event_data:
                event_time = event_data['E']
            elif 'T' in event_data:
                event_time = event_data['T']

        if event_time:
            # Convert the event time to a formatted string in the specified timezone
            # ts_e_est = datetime.fromtimestamp(event_time / 1000, self.timezone).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            ts_e_est = datetime.fromtimestamp(event_time / 1000, self.timezone).strftime('%Y-%m-%d %H:%M:%S,%f')#[:-3]
        else:
            # If no event time is found, use the current time
            # ts_e_est = datetime.now().astimezone(self.timezone).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            ts_e_est = datetime.now().astimezone(self.timezone).strftime('%Y-%m-%d %H:%M:%S,%f')#[:-3]            
        
        return ts_e_est

# ==================================================================================
#   Process WS Messages
# ==================================================================================

    def process_batches(self):
        while True:
            time.sleep(self.batch_seconds)  # Sleep for one hour
            self.process_hourly_batch()

    def process_hourly_batch(self):
        with self.lock:
            if not self.messages:
                return

            # Process accumulated messages
            all_data = list(self.messages)
            self.messages.clear()

        # Initialize DataFrames for each stream type
        lodf_depth_data = []
        lodf_trade_data = []
        lodf_agg_trade_data = []
        lodf_kline_data = []
        lodf_force_order_data = []        

# ----------------------------------------------------------------------------------
#   PROCESS Buffered Messages in DQ Obj, RETURN DF & APPEND to List of DF
# ----------------------------------------------------------------------------------

        # Categorize messages by stream type
        for data in all_data:
            stream_type = data['stream'].split('@')[1]
            # ts_e_est = data['ts_e_est']
            ts_p_est = data['ts_p_est']
            if 'depth' in stream_type:
                xprint("#---------------------------------------------------------------")                
                xprint("----->>>>>'depth' in stream_type: lodf_depth_data")
                xprint("#---------------------------------------------------------------")                
                lodf_depth_data.append(self.create_df_l2b(data['data'], ts_p_est))
                xprint("***lodf_depth_data***")                
                xprint(lodf_depth_data[-2:])
            elif 'trade' in stream_type:
                xprint("#---------------------------------------------------------------")
                xprint("----->>>>>'trade' in stream_type: lodf_trade_data")
                xprint("#---------------------------------------------------------------")                
                lodf_trade_data.append(self.create_df_trade(data['data'], ts_p_est))
                xprint("***lodf_trade_data***")                
                xprint(lodf_trade_data[-2:])
            elif 'aggTrade' in stream_type:
                xprint("#---------------------------------------------------------------")                
                xprint("----->>>>>'aggTrade' in stream_type: lodf_agg_trade_data")        
                xprint("#---------------------------------------------------------------")                        
                lodf_agg_trade_data.append(self.create_df_agg_trade(data['data'], ts_p_est))
                xprint("***lodf_agg_trade_data***")
                xprint(lodf_agg_trade_data[-2:])                
            elif 'kline' in stream_type:
                xprint("#---------------------------------------------------------------")                
                xprint("----->>>>>'kline' in stream_type: lodf_kline_data")               
                xprint("#---------------------------------------------------------------")                 
                lodf_kline_data.append(self.create_df_kline(data['data'], ts_p_est))
                xprint("***lodf_kline_data***")                
                xprint(lodf_kline_data[-2:])                
            elif 'forceOrder' in stream_type:
                xprint("#---------------------------------------------------------------")                
                xprint("----->>>>>'forceOrder' in stream_type: lodf_force_order_data")               
                xprint("#---------------------------------------------------------------")                 
                lodf_force_order_data.append(self.create_df_force_order(data['data'], ts_p_est))                
                xprint("***lodf_kline_data***")                
                xprint(lodf_kline_data[-2:])                                
        # Concatenate data into single DataFrames and write to feather files

# ----------------------------------------------------------------------------------
#   APPEND List of DF to DF
# ----------------------------------------------------------------------------------

        current_time = datetime.now().strftime('%Y%m%d_%H%M')
        if lodf_depth_data:
            df_depth = pd.concat(lodf_depth_data, ignore_index=True)
            self.log_data(df_depth.to_dict())
            df_depth.to_feather(f"{self.dir_updates_root}/depth_{self.ticker.lower()}_{current_time}_{self.db_ver}.feather")
        if lodf_trade_data:
            df_trade = pd.concat(lodf_trade_data, ignore_index=True)
            self.log_data(df_trade.to_dict())
            df_trade.to_feather(f"{self.dir_updates_root}/trade_{self.ticker.lower()}_{current_time}_{self.db_ver}.feather")
        if lodf_agg_trade_data:
            df_agg_trade = pd.concat(lodf_agg_trade_data, ignore_index=True)
            self.log_data(df_agg_trade.to_dict())
            df_agg_trade.to_feather(f"{self.dir_updates_root}/aggTrade_{self.ticker.lower()}_{current_time}_{self.db_ver}.feather")
        if lodf_kline_data:
            df_kline = pd.concat(lodf_kline_data, ignore_index=True)
            self.log_data(df_kline.to_dict())
            df_kline.to_feather(f"{self.dir_updates_root}/kline_{self.ticker.lower()}_{current_time}_{self.db_ver}.feather")
        if lodf_force_order_data:
            df_force_order = pd.concat(lodf_force_order_data, ignore_index=True)
            self.log_data(df_force_order.to_dict())
            df_force_order.to_feather(f"{self.dir_updates_root}/forceOrder_{self.ticker.lower()}_{current_time}_{self.db_ver}.feather")

        # Fetch and process the L2 order book snapshot
        self.fetch_and_process_order_book()

# ==================================================================================
#   DEFINE Streams Handling
# ==================================================================================

    def fetch_and_process_order_book(self):
        url = "https://api.binance.com/api/v3/depth"
        params = {"symbol": self.ticker, "limit": 5000}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            ts_ws = datetime.now().astimezone(self.timezone).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            df_order_book = self.create_df_l2_02(data, ts_ws)
            self.log_data(df_order_book.head(2).to_dict())
            # Save the order book snapshot to a feather file
            current_time = datetime.now().strftime('%Y%m%d_%H%M')
            df_order_book.to_feather(f"{self.dir_updates_root}/order_book_{self.ticker.lower()}_{current_time}_{self.db_ver}.feather")
        else:
            logging.error(f"Failed to fetch order book data: {response.status_code}")

    def create_df_l2_02(self, data_dict, ts_p_est):
        last_update_id = data_dict["lastUpdateId"]
        bids = pd.DataFrame(data_dict["bids"], columns=["Price", "Quantity"]).astype({"Price": str, "Quantity": str})
        asks = pd.DataFrame(data_dict["asks"], columns=["Price", "Quantity"]).astype({"Price": str, "Quantity": str})
        bids["Side"] = "BID"
        asks["Side"] = "ASK"
        bids["LastUpdateId"] = last_update_id
        asks["LastUpdateId"] = last_update_id

        df = pd.concat([bids, asks], ignore_index=True)
        df["Ticker"] = self.ticker
        df["ts_p_est"] = ts_p_est
        # df["TimestampEvent"] = data_dict["E"]
        # df['ts_e_est'] = self.convert_unix_to_est(df['TimestampEvent'][0])
        # df = df[["Ticker", "Side", "Price", "Quantity","LastUpdateId", "ts_p_est", "ts_e_est"]]
        return df

    def create_df_l2b(self, data_dict, ts_p_est):
        last_update_id = data_dict["u"]
        bids = self.create_order_side_df(data_dict["b"], "BID", last_update_id)
        asks = self.create_order_side_df(data_dict["a"], "ASK", last_update_id)

        df = pd.concat([bids, asks], ignore_index=True)
        df["Ticker"] = self.ticker
        df["ts_p_est"] = ts_p_est
        df["TimestampEvent"] = data_dict["E"]
        df['ts_e_est'] = self.convert_unix_to_est(df['TimestampEvent'][0])
        df = df[["Ticker", "Side", "Price", "Quantity","LastUpdateId", "ts_p_est", "ts_e_est"]]
        xprint("***create_df_l2b***")
        xprint(df.head())
        return df

    def create_df_trade(self, data_dict, ts_ws):
        df = self.create_trade_df(data_dict, ts_ws)
        xprint("***create_df_trade***")
        xprint(df.head())
        return df

    def create_df_agg_trade(self, data_dict, ts_ws):
        df = self.create_trade_df(data_dict, ts_ws)
        df2 = df[(df['Val_USD']>=self.d_thresh[self.ticker])]
        xprint("***create_df_agg_trade***")
        xprint(df2.head())
        # df['Side'] = df.apply(lambda row: "BUY" if not row['IsBuyerMaker'] else "SELL", axis=1)
        return df2

    def create_trade_df(self, data_dict, ts_p_est):
        columns = ["Ticker", "Val_USD", "Price", "Quantity", "IsBuyerMaker", "EventTime_EST", "EventTime_Unix", "Aggr_Trade_ID"]
        df = pd.DataFrame([data_dict]).rename(columns={
            's': 'Ticker',
            'a': 'Aggr_Trade_ID',
            'p': 'Price',
            'q': 'Quantity',
            'm': 'IsBuyerMaker',
            "E": 'TimestampEvent'
        })
        df["Price"] = df["Price"].astype(str)
        df["Quantity"] = df["Quantity"].astype(str)
        df['Val_USD'] = df['Price'].astype(float) * df['Quantity'].astype(float)
        # df['EventTime_Unix'] = df['E']
        # df['EventTime_EST'] = self.convert_unix_to_est(df['EventTime_Unix'][0])
        df['Side'] = df.apply(lambda row: "BUY" if not row['IsBuyerMaker'] else "SELL", axis=1)
        df["ts_p_est"] = ts_p_est
        # df["TimestampEvent"] = data_dict["E"]
        df['ts_e_est'] = self.convert_unix_to_est(df['TimestampEvent'][0])
        df = df[["Ticker","Side", "Price", "Quantity","Val_USD", "ts_p_est", "ts_e_est","Aggr_Trade_ID"]]
        # logging.debug(f"Trade DataFrame: {df.head()}")
        return df
    
    def create_df_kline(self, data_dict, ts_ws):
        kline = data_dict['k']
        df = pd.DataFrame([{
            'Ticker': self.ticker,
            'Open': float(kline['o']),
            'Close': float(kline['c']),
            'High': float(kline['h']),
            'Low': float(kline['l']),
            'Volume': float(kline['v']),
            'ts_ws': ts_ws,
            'OpenTime': kline['t'],
            'CloseTime': kline['T'],
            'Interval': kline['i']
        }])
        # logging.debug(f"Kline DataFrame: {df.head()}")
        xprint("***create_df_kline***")
        xprint(df.head())
        return df

    def create_df_liq(self, data_dict, ts_ws):
        columns = ["Ticker", "Val_USD", "Price", "Quantity_Filled", "Quantity", "Liq_Type", "Side", "EventTime_EST", "EventTime_Unix"]
        data_dict = data_dict['o']
        df = pd.DataFrame([data_dict]).rename(columns={
            's': 'Ticker',
            'S': 'Side',
            'p': 'Price',
            'q': 'Quantity',
            'z': 'Quantity_Filled'
        })
        df["Price"] = df["Price"].astype(float)
        df["Quantity"] = df["Quantity"].astype(float)
        df["Quantity_Filled"] = df["Quantity_Filled"].astype(float)
        df['Val_USD'] = df['Price'] * df['Quantity_Filled']
        df['EventTime_Unix'] = df['T']
        df['EventTime_EST'] = self.convert_unix_to_est(df['EventTime_Unix'][0])
        df['Liq_Type'] = df.apply(lambda row: "S_LIQ" if row['Side'] == 'SELL' else 'L_LIQ', axis=1)
        df['ts_ws'] = ts_ws
        logging.debug(f"Liq DataFrame: {df.head()}")
        return df[columns]

    def create_df_force_order(self, data_dict, ts_p_est):
        # columns = ["Ticker", "Side", "Price", "Quantity", "ts_ws", "LastUpdateId"]
        df = pd.DataFrame([data_dict['o']]).rename(columns={
            's': 'Ticker',
            'S': 'Side',
            'p': 'Price',
            'q': 'Quantity',
            'T': 'LastUpdateId'
        })
        df["Price"] = df["Price"].astype(float)
        df["Quantity"] = df["Quantity"].astype(float)
        df['Val_USD'] = df['Price'] * df['Quantity_Filled']        
        df['ts_p_est'] = ts_p_est
        # logging.debug(f"Force Order DataFrame: {df.head()}")
        xprint("***create_df_force_order***")
        xprint(df.head())
        return df#[columns]

    def create_order_side_df(self, data, side, last_update_id):
        df = pd.DataFrame(data, columns=["Price", "Quantity"]).astype({"Price": str, "Quantity": str})
        # df["Price"] = df["Price"].astype(str)
        # df["Quantity"] = df["Quantity"].astype(str)
        df["Side"] = side
        df["LastUpdateId"] = last_update_id
        logging.debug(f"Order Side DataFrame ({side}): {df.head()}")
        return df

    def convert_unix_to_est(self, unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp / 1000, self.timezone).strftime('%Y-%m-%d %H:%M:%S.%f')#[:-3]

async def consume_messages(uri, processor):
    retry_attempts = 0
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logging.info("Connected to WebSocket")
                retry_attempts = 0
                async for message in websocket:
                    processor.process_message(message)
        except (websockets.ConnectionClosed, websockets.InvalidStatusCode) as e:
            logging.error(f"WebSocket connection closed: {e}")
            retry_attempts += 1
            wait_time = min(2 ** retry_attempts, 60)  # Exponential backoff
            logging.info(f"Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)

def main(p_ticker, p_dir_updates_root, p_db_ver,p_batch_seconds):
    processor = BinanceStreamProcessor(ticker=p_ticker, dir_updates_root=p_dir_updates_root, db_ver=p_db_ver,batch_seconds=p_batch_seconds)
    uri = f"wss://stream.binance.com:9443/stream?streams={p_ticker.lower()}@depth@100ms/{p_ticker.lower()}@trade/{p_ticker.lower()}@kline_1m/{p_ticker.lower()}@aggTrade/{p_ticker.lower()}@forceOrder"    
    asyncio.get_event_loop().run_until_complete(consume_messages(uri, processor))

if __name__ == "__main__":
    n_ticker = "BTCUSDT"
    n_dir_updates_root = os.path.join(os.getcwd(), "data/001_raw")
    n_db_ver = os.path.basename(__file__).replace(".py", "")
    n_batch_seconds = 10

    parser = argparse.ArgumentParser()

    parser.add_argument("--a_ticker", type=str, default=n_ticker)
    parser.add_argument("--a_dir_updates_root", type=str, default=n_dir_updates_root)
    parser.add_argument("--a_db_ver", type=str, default=n_db_ver)
    parser.add_argument("--a_batch_seconds", type=int, default=n_batch_seconds)    

    args = parser.parse_args()

    main(p_ticker=args.a_ticker,
         p_dir_updates_root=args.a_dir_updates_root,
         p_db_ver=args.a_db_ver,
         p_batch_seconds=args.a_batch_seconds)
