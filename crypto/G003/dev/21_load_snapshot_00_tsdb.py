# *******************************************************************************************************
# 
#   00: Init TSDB implementation
# 
# *******************************************************************************************************

import os
import pandas as pd
import pyarrow.feather as feather
import psycopg2
import argparse
import shutil
from psycopg2 import sql
from utils import *
from dotenv import load_dotenv
from io import StringIO

# Load environment variables from .env file
load_dotenv()

global yprint
yprint = True

def db_init(p_db_ver):
    pass

# #######################################################################################################
#   
#   INTIAL Snapshpt & History
#   
# #######################################################################################################

def f_ETL_Init_01(p_snap_init_file, conn):

    # ============================================================================================
    #   EXTRACT  
    # ============================================================================================

    ### READ-IN
    l2_snapshot_01 = pd.read_feather(p_snap_init_file)
    xprint("***ETL_Init_01*** Read in file: " + str(p_snap_init_file))    
    xprint("***ETL_Init_01*** Read in # rows: " + str(len(l2_snapshot_01)))

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
    xdisplay("l2_history_00a: ")
    xdisplay(l2_history_00a.head())    

    # ============================================================================================
    #   LOAD Initial data 
    # ============================================================================================

    copy_from_dataframe(conn, l2_snapshot_02a, 'l2_snapshot_ask')
    copy_from_dataframe(conn, l2_snapshot_02b, 'l2_snapshot_bid')
    copy_from_dataframe(conn, l2_history_00a, 'l2_history_ask')
    copy_from_dataframe(conn, l2_history_00b, 'l2_history_bid')

# #######################################################################################################
#   
#   MAIN
#   
# #######################################################################################################

# ============================================================================================
#   DEFINE Main
# ============================================================================================

def main(p_ticker, p_snap_init_file_dir, p_db_ver, p_yprint, p_reset):

    # PostgreSQL connection details from environment variables
    # DB_USER = os.getenv('DB_USERNAME')
    # DB_PASSWORD = os.getenv('DB_PASSWORD')
    # DB_HOST = os.getenv('DB_HOST')
    # DB_PORT = os.getenv('DB_PORT')
    # DB_NAME = os.getenv('DB_NAME')

    # PostgreSQL connection details
    # psql "postgres://tsdbadmin:<PASSWORD>@<HOST>:<PORT>/tsdb?sslmode=require"
    # CONNECTION = "postgres://username:password@host:port/dbname"
    CONNECTION = "postgres://tsdbadmin:>X>#h2lWqXESlyGd}mj2NPDlB@esd7mq3z84.dts890uzaz.tsdb.cloud.timescale.com:37281/tsdb?sslmode=require"

    # PostgreSQL connection details from environment variables
    # DB_USER = os.getenv('DB_USERNAME')
    # DB_PASSWORD = os.getenv('DB_PASSWORD')
    # DB_HOST = os.getenv('DB_HOST')
    # DB_PORT = os.getenv('DB_PORT')
    # DB_NAME = os.getenv('DB_NAME')
    # CONNECTION = f"postgres://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    conn = psycopg2.connect(CONNECTION)
    conn.autocommit = True
    cursor = conn.cursor()

    # global yprint
    # yprint = p_yprint
    xprint("yprint: " + str(yprint))

    if not p_reset:

        # # ------------------------------------------------------------------------------------
        # # DEFINE Schema
        # # ------------------------------------------------------------------------------------

        for side in ['ask', 'bid']:
            snapshot_schema = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                side TEXT,
                price TEXT,
                quantity TEXT,                                      
                timestamp TIMESTAMPTZ,
                lastupdateid TEXT
            )
            """).format(table=sql.Identifier(f'l2_snapshot_{side}'))
            cursor.execute(snapshot_schema)
                        
            history_schema = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                ticker TEXT,
                side TEXT,
                price TEXT,
                quantity TEXT,                                     
                timestamp TIMESTAMPTZ,
                lastupdateid TEXT,
                scd_from_date TIMESTAMPTZ,
                scd_to_date TIMESTAMPTZ
            )
            """).format(table=sql.Identifier(f'l2_history_{side}'))
            cursor.execute(history_schema)

        d_inti_snapshots = {
            "BTCUSDT": "l2s_BTCUSDT_G002_00502_20240511_1315.feather",
            # "ETHUSDT": "l2s_ETHUSDT_G002_00502_20240511_1319.feather",
            # "SOLUSDT": "l2s_SOLUSDT_G002_00502_20240511_1319.feather"
        }

        p_snap_init_file = p_snap_init_file_dir + "001_raw/" + d_inti_snapshots[p_ticker]

        # Load initial snapshot
        f_ETL_Init_01(p_snap_init_file, conn)

        # ------------------------------------------------------------------------------------
        # CLEAN-UP & CLOSE
        # ------------------------------------------------------------------------------------

        shutil.move(os.path.join(p_snap_init_file_dir + "001_raw/", d_inti_snapshots[p_ticker]), 
                    os.path.join(p_snap_init_file_dir + "011_processed/", d_inti_snapshots[p_ticker]))

        cursor.close()
        conn.close()

    else:
        ## MOVE Files back to raw folder
        for filename in os.listdir(p_snap_init_file_dir + "011_processed"):
            shutil.move(os.path.join(p_snap_init_file_dir + "011_processed/", filename), 
                        os.path.join(p_snap_init_file_dir + "001_raw/", filename))
            xprint("Moved: "+str(filename))
            
        ## DELETE tables
        with psycopg2.connect(CONNECTION) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                for side in ['ask', 'bid']:
                    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(table=sql.Identifier(f'l2_history_{side}')))
                    xprint(f"Table 'l2_history_{side}' deleted successfully.")
                    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(table=sql.Identifier(f'l2_snapshot_{side}')))
                    xprint(f"Table 'l2_snapshot_{side}' deleted successfully.")

# ============================================================================================
#   CALL Main
# ============================================================================================

if __name__ == "__main__":

    n_ticker = "BTCUSDT"
    # n_snap_init_file_dir = "../data/001_raw/"
    n_snap_init_file_dir = "../test/data/"
    n_db_ver = "l2_G003_D001"
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

# python3 etl_00_tsdb.py --a_ticker=BTCUSDT --a_db_ver=l2_G003_D001 --a_reset=True
