########################################################################################
# 
#   base: sc_crypto_00302.py
#   updates: 
#   - Big Orders >> 00204
#   - Big Orders with Batch Snapshot >> 00205 
#   - Big Orders change to tiem batch from sieze batch >> 0206 
#   - Liquidations >> 00207    
#   - Upgrade Big & Liq to only export if buffer >0 >> 00207
#   - Clean Up >> 00301
#   - Upgrade error handling >> 00302
#   - Add logging >> 00302
########################################################################################

import asyncio
import websockets
import json
# import orjson 
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
import argparse
import time
import pytz
import pyarrow.feather as feather
import os
import logging

# Set up logging
logging.basicConfig(
    filename=f"logs/websocket_00502.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

#=======================================================================================
#  STATICS & Utilities
#=======================================================================================

# https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

## Exchange Specs                         wss://fstream.binance.com/stream?streams=btcusdt@depth.
d_exchange_01 = {'bin' : {'l2': {'url_a':  {'a' :'wss://stream.binance.com:9443/ws/','b': 'wss://stream.binance.com:9443/ws/','c': 'wss://stream.binance.com:9443/ws/'}, #wss://stream.binance.com:9443/ws/{s_sym_l}@depth20
                                  'endpoint': {'a' :'@depth20', 'b' : '@depth', 'c' : '@depth@100ms/btcusdt@trade'},
                                  'url_z': {'a' :'url+sym_l+endpoint','b' :'url+sym_l+endpoint','c' :'url+sym_l+endpoint'},
                                },
                           'fr' :{'url_a':  {'a' :'wss://fstream.binance.com/ws/','b': ''}, #"wss://fstream.binance.com/ws/bnbusdt@aggTrade
                                  'endpoint': {'a' :'@markPrice','b' :'@aggTrade'},
                                  'url_z': {'a' :'url+sym_l+endpoint'},
                                },
                           'big' :{'url_a':  {'a' :'wss://fstream.binance.com/ws/','b': ''}, #"{s_sym_l}@markPrice"
                                  'endpoint': {'a' :'@aggTrade'},
                                  'url_z': {'a' :'url+sym_l+endpoint'},
                                },                                
                                # https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
                                # https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
                           'liq' :{'url_a': 'wss://fstream.binance.com/ws/', #"{s_sym_l}@markPrice"
                                  'endpoint': {'a' :'@forceOrder','b' : "!forceOrder@arr"},
                                  'url_z': {'a' :'url+sym_l+endpoint','b':'url+endpoint'}
                                },                                

                            # https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
                            # https://dev.binance.vision/t/trade-and-diff-depth-streams-are-not-syncronized-time-to-times/17709
                            # https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect
                            # wss://fstream.binance.com/stream?streams=bnbusdt@aggTrade/btcusdt@markPric
                            'comb': {'url_a':  {'a' :'wss://fstream.binance.com/stream?streams='}, #wss://stream.binance.com:9443/ws/{s_sym_l}@depth20
                                  'endpoint': {'a' :['@depth@100ms','@trade','@aggTrade','@forceOrder']},
                                  'url_z': {'a' :'url+sym_l+endpoint',},
                                },                                
                          },
                }

# @depth@100ms
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
        # {  "e": "depthUpdate", // Event type  
        # "E": 123456789,     // Event time  
        # "T": 123456788,     // Transaction time   
        # "s": "BTCUSDT",     // Symbol  
        # "U": 157,           // First update ID in event  
        # "u": 160,           // Final update ID in event  
        # "pu": 149,          // Final update Id in last stream(ie `u` in last stream)  
        # "b": [              // Bids to be updated    
        #         [      "0.0024",       // Price level to be updated      "10"            // Quantity    ]  
        # ],  
        # "a": [              // Asks to be updated    
        #         [      "0.0026",       // Price level to be updated      "100"          // Quantity    ]  
        # ]
        # }

# @kline_<interval>
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
        # {  "e": "kline",     // Event type  
        # "E": 1638747660000,   // Event time  
        # "s": "BTCUSDT",    // Symbol  
        # "k": {      "t": 1638747660000, // Kline start time    
        #             "T": 1638747719999, // Kline close time    
        #             "s": "BTCUSDT",  // Symbol    
        #             "i": "1m",      // Interval    
        #             "f": 100,       // First trade ID    
        #             "L": 200,       // Last trade ID    
        #             "o": "0.0010",  // Open price    
        #             "c": "0.0020",  // Close price    
        #             "h": "0.0025",  // High price    
        #             "l": "0.0015",  // Low price    
        #             "v": "1000",    // Base asset volume    
        #             "n": 100,       // Number of trades    
        #             "x": false,     // Is this kline closed?    
        #             "q": "1.0000",  // Quote asset volume    
        #             "V": "500",     // Taker buy base asset volume    
        #             "Q": "0.500",   // Taker buy quote asset volume    
        #             "B": "123456"   // Ignore  
        #             }

# @trade
# https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
# {
#   "e": "trade",       // Event type
#   "E": 1672515782136, // Event time
#   "s": "BNBBTC",      // Symbol
#   "t": 12345,         // Trade ID
#   "p": "0.001",       // Price
#   "q": "100",         // Quantity
#   "b": 88,            // Buyer order ID
#   "a": 50,            // Seller order ID
#   "T": 1672515782136, // Trade time
#   "m": true,          // Is the buyer the market maker?
#   "M": true           // Ignore
# }

# @forceOrder
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
# {
#     "e":"forceOrder",                   // Event Type    
#     "E":1568014460893,                  // Event Time    
#     "o":{            
#         "s":"BTCUSDT",                   // Symbol        
#         "S":"SELL",                      // Side        
#         "o":"LIMIT",                     // Order Type        
#         "f":"IOC",                       // Time in Force        
#         "q":"0.014",                     // Original Quantity        
#         "p":"9910",                      // Price        
#         "ap":"9910",                     // Average Price        
#         "X":"FILLED",                    // Order Status        
#         "l":"0.014",                     // Order Last Filled Quantity        
#         "z":"0.014",                     // Order Filled Accumulated Quantity        
#         "T":1568014460893,               // Order Trade Time        }
# }

# @aggTrade
# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
# {
#   "e": "aggTrade",  // Event type
#   "E": 123456789,   // Event time
#   "s": "BTCUSDT",    // Symbol
#   "a": 5933014,     // Aggregate trade ID
#   "p": "0.001",     // Price
#   "q": "100",       // Quantity
#   "f": 100,         // First trade ID
#   "l": 105,         // Last trade ID
#   "T": 123456785,   // Trade time
#   "m": true,        // Is the buyer the market maker?
# }


def create_df_fr(data, timestamp, s_sym_u):
    df = pd.DataFrame([data], columns=["Symbol", "Mark Price", "Index Price", "Funding Rate"])
    df["Symbol"] = s_sym_u
    df["Mark Price"] = df["Mark Price"].astype(float)
    df["Index Price"] = df["Index Price"].astype(float)
    df["Funding Rate"] = df["Funding Rate"].astype(float)
    df["Timestamp"] = timestamp
    return df

d_functions = {
    'big' : create_df_big_01,
    'liq' : create_df_liq_01,
        
}

# -======================================================================
import json
import pandas as pd
import pytz
from datetime import datetime
import logging
import threading
import time
from collections import deque
from websocket import WebSocketApp
import requests

class BinanceStreamProcessor:
    def __init__(self, ticker, timezone="US/Eastern"):
        self.ticker = ticker
        self.timezone = pytz.timezone(timezone)
        self.messages = deque()
        self.lock = threading.Lock()
        logging.basicConfig(filename='binance_stream.log', level=logging.INFO, format='%(asctime)s %(message)s')

        # Start the batching thread
        self.batching_thread = threading.Thread(target=self.process_batches)
        self.batching_thread.daemon = True
        self.batching_thread.start()

    def log_data(self, data):
        logging.info(json.dumps(data, indent=4))

    def process_message(self, message):
        data = json.loads(message)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        data['timestamp'] = timestamp
        with self.lock:
            self.messages.append(data)

    def process_batches(self):
        while True:
            time.sleep(3600)  # Sleep for one hour
            self.process_hourly_batch()

    def process_hourly_batch(self):
        with self.lock:
            if not self.messages:
                return

            # Process accumulated messages
            all_data = list(self.messages)
            self.messages.clear()

        # Initialize DataFrames for each stream type
        depth_data = []
        trade_data = []
        agg_trade_data = []
        kline_data = []

        # Categorize messages by stream type
        for data in all_data:
            stream_type = data['stream'].split('@')[1]
            timestamp = data['timestamp']
            if 'depth' in stream_type:
                depth_data.append(self.create_df_l2b(data['data'], timestamp))
            elif 'trade' in stream_type:
                trade_data.append(self.create_df_trade(data['data'], timestamp))
            elif 'aggTrade' in stream_type:
                agg_trade_data.append(self.create_df_agg_trade(data['data'], timestamp))
            elif 'kline' in stream_type:
                kline_data.append(self.create_df_kline(data['data'], timestamp))

        # Concatenate data into single DataFrames
        if depth_data:
            df_depth = pd.concat(depth_data, ignore_index=True)
            self.log_data(df_depth.to_dict())
        if trade_data:
            df_trade = pd.concat(trade_data, ignore_index=True)
            self.log_data(df_trade.to_dict())
        if agg_trade_data:
            df_agg_trade = pd.concat(agg_trade_data, ignore_index=True)
            self.log_data(df_agg_trade.to_dict())
        if kline_data:
            df_kline = pd.concat(kline_data, ignore_index=True)
            self.log_data(df_kline.to_dict())

        # Fetch and process the L2 order book snapshot
        self.fetch_and_process_order_book()

    def fetch_and_process_order_book(self):
        url = "https://api.binance.com/api/v3/depth"
        params = {"symbol": self.ticker, "limit": 5000}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            df_order_book = self.create_df_l2_02(data, timestamp)
            self.log_data(df_order_book.to_dict())
        else:
            logging.error(f"Failed to fetch order book data: {response.status_code}")

    def create_df_l2_02(self, data_dict, timestamp):
        last_update_id = data_dict["lastUpdateId"]
        bids = pd.DataFrame(data_dict["bids"], columns=["Price", "Quantity"])
        asks = pd.DataFrame(data_dict["asks"], columns=["Price", "Quantity"])
        bids["Side"] = "BID"
        asks["Side"] = "ASK"
        bids["LastUpdateId"] = last_update_id
        asks["LastUpdateId"] = last_update_id

        df = pd.concat([bids, asks], ignore_index=True)
        df["Ticker"] = self.ticker
        df["Timestamp"] = timestamp
        df = df[["Ticker", "Side", "Price", "Quantity", "Timestamp", "LastUpdateId"]]
        return df

    def create_df_l2b(self, data_dict, timestamp):
        last_update_id = data_dict["u"]
        bids = self.create_order_side_df(data_dict["b"], "BID", last_update_id)
        asks = self.create_order_side_df(data_dict["a"], "ASK", last_update_id)

        df = pd.concat([bids, asks], ignore_index=True)
        df["Ticker"] = self.ticker
        df["Timestamp"] = timestamp
        df["TimestampEvent"] = data_dict["E"]
        return df[["Ticker", "Side", "Price", "Quantity", "Timestamp", "LastUpdateId", "TimestampEvent"]]

    def create_df_trade(self, data_dict, timestamp):
        return self.create_trade_df(data_dict, timestamp)

    def create_df_agg_trade(self, data_dict, timestamp):
        df = self.create_trade_df(data_dict, timestamp)
        df['Side'] = df.apply(lambda row: "BUY" if not row['IsBuyerMaker'] else "SELL", axis=1)
        return df

    def create_trade_df(self, data_dict, timestamp):
        columns = ["Ticker", "Val_USD", "Price", "Quantity", "Side", "IsBuyerMaker", "EventTime_EST", "EventTime_Unix", "Aggr_Trade_ID"]
        df = pd.DataFrame([data_dict]).rename(columns={
            's': 'Ticker',
            'a': 'Aggr_Trade_ID',
            'p': 'Price',
            'q': 'Quantity',
            'm': 'IsBuyerMaker'
        })
        df["Price"] = df["Price"].astype(str)
        df["Quantity"] = df["Quantity"].astype(str)
        df['Val_USD'] = df['Price'].astype(float) * df['Quantity'].astype(float)
        df['EventTime_Unix'] = df['E']
        df['EventTime_EST'] = self.convert_unix_to_est(df['EventTime_Unix'][0])
        df['Timestamp'] = timestamp
        return df[columns]

    def create_df_liq(self, data_dict, timestamp):
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
        df['Timestamp'] = timestamp
        return df[columns]

    def create_df_kline(self, data_dict, timestamp):
        kline = data_dict['k']
        df = pd.DataFrame([{
            'Ticker': self.ticker,
            'Open': float(kline['o']),
            'Close': float(kline['c']),
            'High': float(kline['h']),
            'Low': float(kline['l']),
            'Volume': float(kline['v']),
            'Timestamp': timestamp,
            'OpenTime': kline['t'],
            'CloseTime': kline['T'],
            'Interval': kline['i']
        }])
        return df

    def create_order_side_df(self, data, side, last_update_id):
        df = pd.DataFrame(data, columns=["Price", "Quantity"])
        df["Price"] = df["Price"].astype(str)
        df["Quantity"] = df["Quantity"].astype(str)
        df["Side"] = side
        df["LastUpdateId"] = last_update_id
        return df

    def convert_unix_to_est(self, unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp / 1000, self.timezone).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]

# Example usage
def on_message(ws, message):
    processor.process_message(message)

# Combined streams URL
url = "wss://stream.binance.com:9443/stream?streams=solusdt@depth@100ms/solusdt@trade/solusdt@kline_1m/solusdt@aggTrade"

# WebSocket App
processor = BinanceStreamProcessor(ticker="SOLUSDT")
ws = WebSocketApp(url, on_message=on_message)
ws.run_forever()

# -======================================================================



d_quantity = {
    'big' : 'q',
    'liq' : 'z',    
    'l2' : 'z',        
}

d_persist_to = {
    'raw' : '001_raw',
    'output' : 'z',    
}

d_thresh = {
    'BTCUSDT' : 500000,
    'ETHUSDT' : 100000,
    'SOLUSDT' : 10000, 
}

def export_to_csv(snapshots, filename):
    combined_df = pd.concat(snapshots, ignore_index=True)
    combined_df.to_csv(filename, index=False)

#=======================================================================================
#  PARAMETERS
#=======================================================================================

MAX_RETRIES = 5  # Maximum number of reconnection attempts
RETRY_DELAY = 5  # Delay in seconds between retries

#=======================================================================================
#  WEBSOCKET
#=======================================================================================

def get_params(p_exch,p_data,p_sym,p_thresh,p_snap,p_exp,p_gen,p_ver,p_endpoint):

    try:
#---------------------------------------------------------------------------------------
#  SETUP Params
#---------------------------------------------------------------------------------------

        ### Generation (for the exports)
        s_gen = p_gen+"_"+p_ver

        ### Tickers (for the url & exports)
        s_sym_l = p_sym.lower()
        s_sym_u = p_sym.upper()

#---------------------------------------------------------------------------------------
#  DETERMINE url: s_ws_url = f"wss://stream.binance.com:9443/ws/{s_sym_l}@depth20"
#---------------------------------------------------------------------------------------

        ## Exchange URL
        # GET url parameters
        s_url = d_exchange_01[p_exch][p_data]['url_a'][p_endpoint]
        s_endpoint = d_exchange_01[p_exch][p_data]['endpoint'][p_endpoint]
        s_exch_string = d_exchange_01[p_exch][p_data]['url_z'][p_endpoint]
        # DETERMINE url string
        if s_exch_string == 'url+sym_l+endpoint': 
            s_ws_url = s_url+s_sym_l+s_endpoint
        elif s_exch_string == "url+endpoint":
            s_ws_url = s_url+s_endpoint 
        else:    
            s_ws_url = s_url+s_sym_l+s_endpoint
        
        print("s_ws_url: "+str(s_ws_url))
        return s_sym_u, s_sym_l, s_ws_url, s_gen

    except KeyError as e:
        logger.error(f"Invalid key: {e}")
        raise e

# =========================================================================================
#   STREAM: Messages
# =========================================================================================

# --------------------------------------------------------------------------------------
# SPECIFY Websocket
# --------------------------------------------------------------------------------------

# Function to connect and handle WebSocket errors with retries
async def wss_stream(p_exch, p_data, p_sym, p_thresh, p_snap, p_exp,p_endpoint, p_gen, p_ver, p_persist_to):


    try:     
        # Get WebSocket URL and other parameters
        s_sym_u, s_sym_l, s_ws_url, s_gen = get_params(
            p_exch=p_exch,
            p_data=p_data,
            p_sym=p_sym,
            p_thresh=p_thresh,
            p_snap=p_snap,
            p_exp=p_exp,
            p_endpoint=p_endpoint,            
            p_gen=p_gen,
            p_ver=p_ver,
        )

        s_ws_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms/btcusdt@trade/btcusdt@kline_1m/btcusdt@aggTrade"

# --------------------------------------------------------------------------------------
# CREATE Websocket (Async)
# --------------------------------------------------------------------------------------

        # Retry mechanism
        retries = 0
        while retries < MAX_RETRIES:
            try:
                logger.info(f"Connecting to {s_ws_url} (Attempt {retries + 1})")
                async with websockets.connect(s_ws_url) as websocket:
                    # print("CHK: async with await")
                    await process_websocket(websocket, s_sym_u, p_thresh, p_data, p_snap, p_exp,p_endpoint, s_gen,p_persist_to)
            except websockets.ConnectionClosedError as e:
                logger.warning(f"WebSocket connection closed: {e}")
                retries += 1
                if retries < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)  # Retry delay
                else:
                    logger.error("Max retries reached")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break

    except Exception as e:
        logger.error(f"Error in wss_stream: {e}")
        raise e

# =========================================================================================
#   RUN: Websocket
# =========================================================================================

# Function to process incoming WebSocket data
async def process_websocket(websocket, s_sym_u, p_thresh, p_data, p_snap, p_exp,p_endpoint, s_gen, p_persist_to):

# --------------------------------------------------------------------------------------
# SPECIFY Batches & Exports
# --------------------------------------------------------------------------------------
    try:
        # print("chk: try")
        start_time = datetime.now()
        export_interval = timedelta(minutes=p_exp)  # 5-minute export
        next_export_time = start_time + export_interval

        global dq_data_buffer 
        dq_data_buffer = deque()
        tz_ny = pytz.timezone("America/New_York")

        global df_snapshot
        df_snapshot = pd.DataFrame()
        global df_big
        global df_liq    
# --------------------------------------------------------------------------------------
# STREAM Websocket
# --------------------------------------------------------------------------------------
        while True:
            # print("chk: while true")            
            # -----------------------------------------------------------
            # Timestamps and Dates 
            # -----------------------------------------------------------
            # json_data = orjson.loads(raw_data)  # Parse JSON
            ts21 = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f %Z%z")
            date_str, microseconds = ts21.rsplit('.', 1)
            microseconds = int(microseconds)  # Convert to int
            datetime_utc = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")  # Convert to datetime
            datetime_utc = datetime_utc.replace(microsecond=microseconds)  # Add microseconds
            tz_ny = pytz.timezone("America/New_York")
            datetime_ny = datetime_utc.astimezone(tz_ny)
            tz_frankfurt = pytz.timezone("Europe/Berlin")  # CET is used during standard time
            datetime_frankfurt = datetime_utc.astimezone(tz_frankfurt)
            # print("datetime_utc: "+str(datetime_utc))
            
            # -----------------------------------------------------------
            # RECEIVE Websocket
            # -----------------------------------------------------------
            raw_data = await websocket.recv()  # Receive data
            val_usd = 0
            s_quantity = d_quantity[p_data]
            if p_data in ['big','liq']:
                s_quantity = d_quantity[p_data]
                json_data = json.loads(raw_data) if p_data=='big' else json.loads(raw_data)['o']
                val_usd = (float(json_data['p']) * float(json_data[s_quantity]))
                # print("val_usd: "+str(val_usd))
                if val_usd>d_thresh[s_sym_u]:
                    dq_data_buffer.append({datetime_ny.strftime("%Y-%m-%d %H:%M:%S.%f|%Z%z"):raw_data})
                    # print(dq_data_buffer)
            else:
                # print("chk: dq_data_buffer.append")
                dq_data_buffer.append({datetime_ny.strftime("%Y-%m-%d %H:%M:%S.%f|%Z%z"):raw_data})        
                # print("dq_data_buffer LEN: "+str(len(dq_data_buffer)))

            print(f"Counter: {len(dq_data_buffer)}", end='\r', flush=True)  # Overwrites the current line         
            
            batch_size = p_snap
            batch_counter = 0 
            

            # -----------------------------------------------------------
            # EXPORT Batch
            # -----------------------------------------------------------
            
            # If batch size is reached, process the batch
            if p_data in ["l2","fr"]:
                if (len(dq_data_buffer) >= batch_size):
                    current_time = datetime.now()
                    temp_buffer = list(dq_data_buffer)
                    dq_data_buffer.clear()  # Clear the main buffer
                    # print("chk: dq_data_buffer")

                    if p_data=="l2":
                        # print("chk: p_data==l2")
                        # print("chk: p_endpoint=="+p_endpoint)
                        if p_endpoint=='a':
                            lodf_snapshot = [create_df_l2_02(json_str, ts, s_sym_u) for d in temp_buffer for ts,json_str in d.items()]
                            df_snapshot = pd.concat(lodf_snapshot, ignore_index=True)
                        elif p_endpoint=='b':
                            # print("chk: p_endpoint==b")
                            lodf_snapshot = [create_df_l2b_01(json_str, ts, s_sym_u) for d in temp_buffer for ts,json_str in d.items()]
                        elif p_endpoint=='c':
                            print("chk: p_endpoint==c")
                            lodf_snapshot = [create_df_l2c_01(json_str, ts, s_sym_u) for d in temp_buffer for ts,json_str in d.items()]  
                            print(lodf_snapshot)                          
                            # print("lodf_snapshot LEN:"+str(len(lodf_snapshot)))                                                         
                            # df_snapshot = pd.concat(lodf_snapshot, ignore_index=True)
                            # print("df_snapshot: ")
                            # print(df_snapshot.tail()) 

                    elif p_data=="fr":
                        df_snapshot = create_df_fr(json_data, current_time, s_sym_u)
                        # print(df_snapshot.tail())   

                    # filename_feather= f"data/{d_persist_to[p_persist_to]}/{p_data}_{s_sym_u}_{s_gen}_{current_time.strftime('%Y%m%d_%H%M')}.feather"
                    # filename = f"data/{d_persist_to[p_persist_to]}/{p_data}_{s_sym_u}_{s_gen}_{current_time.strftime('%Y%m%d_%H%M')}.csv"

                    filename = f"data/{d_persist_to[p_persist_to]}/{p_data}_{s_sym_u}_{s_gen}_{current_time.strftime('%Y%m%d_%H%M')}"     
                    # print("filename: "+str(filename))                                       
                    feather.write_feather(df_snapshot, filename+".feather")            
                    # df_snapshot.to_csv(filename+".csv", index=False)
                    print("export: "+str(filename)+" | length: "+str(len(df_snapshot)))            

                    next_export_time = current_time + export_interval  # Update the next export time

            elif ( p_data in ["big","liq"] ):# and (val_usd > p_thresh):
                if current_time >= next_export_time:

                    if len(dq_data_buffer)>0:
                        temp_buffer = list(dq_data_buffer)
                        dq_data_buffer.clear()  # Clear the main buffer
                        func = d_functions[p_data]
                        lodf_snapshot = [func(json_str, ts, s_sym_u) for d in temp_buffer for ts,json_str in d.items()]   
                        df_snapshot = pd.concat(lodf_snapshot, ignore_index=True)      
                        # print(df_snapshot.tail())         

                        filename_feather= f"{d_persist_to[p_persist_to]}/{p_data}_{s_sym_u}_{s_gen}_{current_time.strftime('%Y%m%d_%H%M')}.feather"            
                        feather.write_feather(df_snapshot, filename_feather)            
                        print("export: "+str(filename_feather)+" | length: "+str(len(df_snapshot)))            
                        # filename = f"{d_persist_to[p_persist_to]}/{p_data}_{s_sym_u}_{s_gen}_{current_time.strftime('%Y%m%d_%H%M')}.csv"
                        # export_to_csv(lodf_snapshot , filename)  # Export to CSV

                        next_export_time = current_time + export_interval  # Update the next export time

                    else:
                        next_export_time = current_time + export_interval

    except Exception as e:
        logger.error(f"Error in process_websocket: {e}")
        raise e

# =========================================================================================
#   MAIN
# =========================================================================================

# Start the event loop and call the main function
if __name__ == "__main__":
    i_exch = "bin"
    i_data = "l2"
    i_gen = "G003"
    i_ver = "00205"
    i_snap = 120
    i_exp = 120
    i_sym = "ethusdt"
    i_thresh = 500000
    i_persist_to="test"

    parser = argparse.ArgumentParser()

    parser.add_argument("--a_exch", type=str, default=i_exch)
    parser.add_argument("--a_data", type=str, default=i_data)
    parser.add_argument("--a_sym", type=str, default=i_sym)
    parser.add_argument("--a_thresh", type=int, default=i_thresh)    
    parser.add_argument("--a_snap", type=int, default=i_snap)
    parser.add_argument("--a_exp", type=int, default=i_exp)
    parser.add_argument("--a_endpoint", type=str, default="a")
    parser.add_argument("--a_gen", type=str, default=i_gen)
    parser.add_argument("--a_ver", type=str, default=i_ver)
    parser.add_argument("--a_persist_to", type=str, default=i_persist_to)

    args = parser.parse_args()

    asyncio.run(
        wss_stream(
            p_exch=args.a_exch,
            p_data=args.a_data,
            p_sym=args.a_sym,
            p_thresh=args.a_thresh,
            p_snap=args.a_snap,
            p_exp=args.a_exp,
            p_endpoint=args.a_endpoint,            
            p_gen=args.a_gen,
            p_ver=args.a_ver,
            p_persist_to=args.a_persist_to
        )
    )

    # python3 s_crypto_002.py --a_exch=bin --a_data=l2
    #  --a_sym=solusdt --a_'psnap=1 --a_exp=60 --a_endpoint=a --a_gen=G001 --a_ver=02
    # python3 s_crypto_002.py --a_exch=bin --a_data=l2 --a_sym=ethusdt --a_snap=1 --a_exp=60 --a_endpoint=a --a_gen=G001 --a_ver=02
    # python3 sc_crypto_00202.py --a_exch=bin --a_data=l2 --a_sym=solusdt --a_snap=3600 --a_exp=60 --a_endpoint=a --a_gen=G003 --a_ver=00202    
    # python3 sc_crypto_00202.py --a_ebxch=bin --a_data=l2 --a_sym=ethusdt --a_snap=3600 --a_exp=60 --a_endpoint=a --a_gen=G003 --a_ver=00202    
    # python3 sc_crypto_00202.py --a_exch=bin --a_data=l2 --a_sym=btcusdt --a_snap=3600 --a_exp=60 --a_endpoint=a --a_gen=G003 --a_ver=00202       
    # python3 sc_crypto_00204.py --a_exch=bin --a_data=big --a_sym=btcusdt --a_snap=2 --a_exp=60 --a_endpoint=a --a_gen=G003 --a_ver=00204 
    # python3 sc_crypto_00206.py --a_exch=bin --a_data=big --a_sym=btcusdt --a_thresh=500000 --a_snap=10 --a_exp=60 --a_endpoint=a --a_gen=G003 --a_ver=00206           
    # python3 sc_crypto_00207.py --a_exch=bin --a_data=liq --a_sym=btcusdt --a_thresh=3000 --a_snap=1 --a_exp=60 --a_endpoint=a --a_gen=G003 --a_ver=00207
    # python3 sc_crypto_00302.py --a_exch=bin --a_data=liq --a_sym=btcusdt --a_thresh=3000 --a_snap=1 --a_exp=1 --a_endpoint=a --a_gen=G003 --a_ver=00302 --a_persist_to=output
    # python3 d_crypto_00501.py --a_exch=bin --a_data=l2 --a_sym=btcusdt --a_thresh=3000 --a_snap=10 --a_exp=1 --a_endpoint=a --a_gen=G002 --a_ver=00401 --a_persist_to=raw   
    # python3 d_crypto_00501.py --a_exch=bin --a_data=l2 --a_sym=btcusdt --a_thresh=3000 --a_snap=10 --a_exp=1 --a_endpoint=b --a_gen=G001 --a_ver=00501 --a_persist_to=raw        
    # python3 d_crypto_00502.py --a_exch=bin --a_data=l2 --a_sym=btcusdt --a_thresh=3000 --a_snap=3600 --a_exp=1 --a_endpoint=b --a_gen=G002 --a_ver=00502 --a_persist_to=raw
    # python3 d_crypto_00503.py --a_exch=bin --a_data=l2 --a_sym=btcusdt --a_thresh=3000 --a_snap=3600 --a_exp=1 --a_endpoint=b --a_gen=G004 --a_ver=00503 --a_persist_to=raw                