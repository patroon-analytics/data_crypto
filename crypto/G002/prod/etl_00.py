# *******************************************************************************************************
# 
# 
#   LOAD Initial Snapshot and SCD_History tables from the API 5000 download file
#  
# 
# *******************************************************************************************************

import os
import pandas as pd
import pyarrow
import pytz
from datetime import datetime, timedelta
import pyarrow.feather as feather
import numpy as np
pd.set_option('display.max_columns', None)
import matplotlib.pyplot as plt
from pandasql import sqldf
import sqlite3
import argparse
import shutil


from utils import *

global yprint
yprint = True

            
# #######################################################################################################
#   
#   SETUP db
#   
# #######################################################################################################

def db_init(p_db_ver):
    pass

# #######################################################################################################
#   
#   INTIAL Snapshpt & History
#   
# #######################################################################################################

    
def f_ETL_Init_01(p_snap_init_file,conn):

# ============================================================================================
#   EXTRACT  
# ============================================================================================

    ### READ-IN
    l2_snapshot_01 = pd.read_feather(p_snap_init_file)
    xprint("***ETL_Init_01*** Read in file: "+str(p_snap_init_file))    
    xprint("***ETL_Init_01*** Read in # rows: "+str(len(l2_snapshot_01)))

# ============================================================================================
#   TRANSFORM
# ============================================================================================

    ### Lower-case
    l2_snapshot_01.columns = map(str.lower, l2_snapshot_01.columns)
    xprint("***ETL_Init_01*** l2_snapshot_01")
    # xdisplay(l2_snapshot_01.head(2))
    ### SPLIT into Bid-Ask
    l2_snapshot_02a = l2_snapshot_01[(l2_snapshot_01['side'].str.upper() == 'ASK')]
    xdisplay(l2_snapshot_02a.head(2))
    l2_snapshot_02b = l2_snapshot_01[(l2_snapshot_01['side'].str.upper() == 'BID')]    
    xdisplay(l2_snapshot_02b.head(2))

    ### SCD Columns
    l2_history_00 = l2_snapshot_01.copy()
    l2_history_00['scd_from_date'] = l2_history_00['timestamp']
    
    # l2_history_00['scd_to_date'] = '9999-12-31 23:59'
    l2_history_00['scd_to_date'] = get_max_scd_to_date()
    ### SPLIT into Bid-Ask    
    l2_history_00a = l2_history_00[(l2_history_00['side'].str.upper() == 'ASK')]
    l2_history_00b = l2_history_00[(l2_history_00['side'].str.upper() == 'BID')]    

# ============================================================================================
#   LOAD Initial data 
# ============================================================================================

    # conn = sqlite3.connect(p_db_ver+'.db')

    l2_snapshot_02a.to_sql('l2_snapshot_ask', conn, if_exists='replace', index=False)
    l2_snapshot_02b.to_sql('l2_snapshot_bid', conn, if_exists='replace', index=False)    
    l2_history_00a.to_sql('l2_history_ask', conn, if_exists='replace', index=False)
    l2_history_00b.to_sql('l2_history_bid', conn, if_exists='replace', index=False)    

    conn.close()

# #######################################################################################################
#   
#   MAIN
#   
# #######################################################################################################

# ============================================================================================
#   DEFINE Main
# ============================================================================================

def main(p_ticker,p_snap_init_file_dir,p_db_ver,p_yprint, p_reset):

    s_db = p_ticker+"_"+p_db_ver+'.db'

    # global yprint
    # yprint = p_yprint
    xprint("yprint: "+str(yprint))

    if p_reset == False:

    # # ------------------------------------------------------------------------------------
    # # CREATE Connection
    # # ------------------------------------------------------------------------------------        
        # Initialize SQLite database connection
        # conn = sqlite3.connect('snapshot_database.db')
        conn = sqlite3.connect(s_db)

    # # ------------------------------------------------------------------------------------
    # # DEFINE Schema
    # # ------------------------------------------------------------------------------------        

        # Create the SCD table with additional columns for SCD tracking
        for side in ['ask','bid']:
            history_schema = f"""
            CREATE TABLE IF NOT EXISTS l2_history_{side} (
                ticker TEXT,
                side TEXT,
                price FLOAT,
                timestamp TEXT,
                lastupdateid TEXT,
                quantity TEXT,
                scd_from_date TEXT,
                scd_to_date TEXT
            )
            """
            conn.execute(history_schema)

    # # ------------------------------------------------------------------------------------
    # # LOAD
    # # ------------------------------------------------------------------------------------        

        d_inti_snapshots = {"BTCUSDT" : "l2s_BTCUSDT_G002_00502_20240511_1315.feather",
                            "ETHUSDT" : "l2s_ETHUSDT_G002_00502_20240511_1319.feather",
                            "SOLUSDT" : "l2s_SOLUSDT_G002_00502_20240511_1319.feather"
            }

        p_snap_init_file = p_snap_init_file_dir+"001_raw/"+d_inti_snapshots[p_ticker]


        # Load initial snapshot
        f_ETL_Init_01(p_snap_init_file,conn)

    # # ------------------------------------------------------------------------------------
    # # CLEAN-UP & CLOSE
    # # ------------------------------------------------------------------------------------        

        shutil.move(os.path.join(p_snap_init_file_dir+"001_raw/", d_inti_snapshots[p_ticker]), 
                    os.path.join(p_snap_init_file_dir+"011_processed/", d_inti_snapshots[p_ticker]))

        # Close the database connection
        conn.close()

    # # ------------------------------------------------------------------------------------
    # # RESET
    # # ------------------------------------------------------------------------------------        

    else:
        ## MOVE Files back to raw folder
        for filename in os.listdir(p_snap_init_file_dir+"011_processed"):
            shutil.move(os.path.join(p_snap_init_file_dir+"011_processed/", filename), 
                        os.path.join(p_snap_init_file_dir+"001_raw/", filename))
            
        ## DELETE db
        # Check if the file exists
        if os.path.exists(s_db):
            # Delete the file
            os.remove(s_db)
            print(f"{s_db} deleted successfully")
        else:
            print(f"{s_db} does not exist")

# ============================================================================================
#   CALL Main
# ============================================================================================

if __name__ == "__main__":

    n_ticker = "BTCUSDT"
    # n_snap_init_file_dir = "../data/001_raw/"
    n_snap_init_file_dir = "../test/data/"
    n_db_ver = "l2_G002_D002"
    n_yprint = True    
    n_reset = False

    parser = argparse.ArgumentParser()

    parser.add_argument("--a_ticker", type=str, default=n_ticker)    
    parser.add_argument("--a_snap_init_file_dir", type=str, default=n_snap_init_file_dir)
    parser.add_argument("--a_db_ver", type=str, default=n_db_ver)
    parser.add_argument("--a_yprint", type=bool, default=n_yprint)        
    parser.add_argument("--a_reset", type=bool, default=n_reset)            

    args = parser.parse_args()

    main(p_ticker=args.a_ticker,
        p_snap_init_file_dir=args.a_snap_init_file_dir,
        p_db_ver=args.a_db_ver,
        p_yprint=args.a_yprint,
        p_reset=args.a_reset)

# python3 etl_00.py --a_ticker=BTCUSDT --a_db_ver=l2_G002_D001 --a_reset=True

