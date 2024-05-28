
# *******************************************************************************************************
# 
#   # Base Code: etl_09_scd_fix.py
# 
#   00: SCD
#   02: Split into Sides
#   03: ChatGTP checks 
#   04: Error handling & logging
#   05: Align To_SCD timestamp to ts_est
#   06: Log Updates
#   07: Sort input files
#   08: scd_to date
#   09: scd_to fix
#   
#   21: tsdb & logging
# 
# *******************************************************************************************************


import os
import pandas as pd
import pyarrow
import pytz
from datetime import datetime, timedelta
import pyarrow.feather as feather
import numpy as np
import matplotlib.pyplot as plt
from pandasql import sqldf
import sqlite3
import argparse
import shutil
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

from utils import *
from utils_tsdb import *

import logging


import warnings 
  
# Settings the warnings to be ignored 
warnings.filterwarnings('ignore') 
  

# Configure logging
# logging.basicConfig(filename='logs/30_load_update_20.log', level=logging.INFO, 
#                     format='%(asctime)s %(levelname)s:%(message)s')

# ???????????????????????????????????????????????????????????????????????
# import sys
# old_stdout = sys.stdout

# log_file = open("logs/30_load_update_20_v11.log","w")

# sys.stdout = log_file
# ???????????????????????????????????????????????????????????????????????

# global yprint
# yprint=False

# #######################################################################################################
# UPDATES
# #######################################################################################################

l_col_order_00 = ['ticker', 'side','price','px_00','quantity','qx_00','timestamp','ts_e_est','lastupdateid','scd_from_date','scd_to_date']

def g_E01_df(p_ticker, p_dir_updates_root, conn,p_db_ver,l_col_order_01=l_col_order_00):

    p_dir_updates = p_dir_updates_root + "001_raw/"

    log_info(conn, p_db_ver, f"***g_E01_df*** GET list of files")
    filenames = [f for f in os.listdir(p_dir_updates) if f.startswith('l2_' + p_ticker) and f.endswith('.feather')]
    filenames.sort()

    log_info(conn, p_db_ver, f"***g_E01_df*** ITER PROCESS each file")
    for filename in filenames:
        try:
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            log_info(conn, p_db_ver, f"***g_E01*** READING-IN: {filename} @ {now_ts}")
            # print(os.path.join(p_dir_updates, filename))
            l2_update_02 = pd.read_feather(os.path.join(p_dir_updates, filename))
            xprint("len(l2_update_02) pre"+str(len(l2_update_02)))
            l2_update_02 = l2_update_02.drop_duplicates() 
            log_info(conn, p_db_ver, f"Number of rows input: {str(len(l2_update_02))}")
            ### Lower-case
            l2_update_02.columns = map(str.lower, l2_update_02.columns)
            l2_update_02['ts_p'] = l2_update_02['timestamp'].str.split('|').str[0]
            l2_update_02['ts_p_03'] = pd.to_datetime(l2_update_02['ts_p'], format="%Y-%m-%d %H:%M:%S.%f").dt.round("s")
            # l2_update_02[['ts_e_est', 'ts_e_cet']] = l2_update_02['timestampevent'].apply(f_convert_ts_to_dt)
            l2_update_02[['ts_e_est']] = l2_update_02['timestampevent'].apply(f_convert_ts_to_dt_02)            
            l2_update_02 = l2_update_02.sort_values(by=['price', 'lastupdateid'])
            ### CREATE Numeric px & qx
            l2_update_02['px_00'] = l2_update_02['price']
            l2_update_02['qx_00'] = l2_update_02['quantity']
            ### SCD Columns
            l2_update_02['scd_from_date'] = l2_update_02['ts_e_est']            
            l2_update_02['scd_to_date'] = get_max_scd_to_date()
            ### ORDER Columns
            
            l2_update_02 = l2_update_02[l_col_order_01]
            # print("***generator output***")
            # print(l2_update_02.head())
            # shutil.move(os.path.join(p_dir_updates_root + "001_raw/", filename),
            #             os.path.join(p_dir_updates_root + "011_processed/", filename)),
            log_info(conn, p_db_ver, f"***g_E01_df*** MOVE {filename} file to processed folder")

            yield l2_update_02

        except Exception as e:
            log_error(conn, p_db_ver, f"***g_E01_df*** Error processing file {filename}: {str(e)}")
            continue

# ============================================================================================
# LOAD
# ============================================================================================

def f_L01_update(conn,cursor,p_db_ver, df_updates, p_side,l_col_order_01=l_col_order_00):
    max_scd_to_date = get_max_scd_to_date()
    try:
        log_info(conn, p_db_ver, f"***f_L01_update*** DEDUPE & SORT List of lastupdateid")
        l_lastid_upd_i = sorted(df_updates['lastupdateid'].unique())
        log_info(conn, p_db_ver, f"***f_L01_update*** ITER PROCESSING all the lastupdateid's")
        for iter, lastupdateid in enumerate(l_lastid_upd_i):
            xprint("# ================================================================================")
            xprint("# iter: "+str(iter))
            xprint("# lastupdateid in l_lastid_upd_i: "+str(lastupdateid))
            xprint("# ================================================================================")            
            update_i = df_updates[df_updates['lastupdateid'] == lastupdateid].copy()
            xprint("update_i ===>")
            xprint(update_i)
            # xprint(update_i[(update_i['price']=='61227.27000000')])

        # --------------------------------------------------------------------------------------------
        #   LOAD Update temp table 
        # --------------------------------------------------------------------------------------------

            # bulk_insert_update_i(conn,p_db_ver, cursor, update_i)

            # Step 0: Use execute_values for bulk insert
            try:
                update_i_records = update_i.to_dict('records')
                xprint("update_i_records ==>")
                xprint(update_i_records)
                # columns = update_i.columns.tolist()
                columns = l_col_order_01
                # xprint("update_i.columns.tolist() ==>")
                # xprint(columns)

                # Step 1: Create a temporary table
                # create_tmp_table_query = sql.SQL("""
                #     CREATE TEMP TABLE tmp_update_i AS
                #     SELECT * FROM update_i WHERE 1=0;
                # """)
                create_tmp_table_query = f"""
                    CREATE TEMP TABLE IF NOT EXISTS tmp_update_i (
                        ticker TEXT,
                        side TEXT,
                        price TEXT,
                        px_00 NUMERIC(1000, 15),
                        quantity TEXT,
                        qx_00 NUMERIC(1000, 15),                  
                        timestamp TEXT,
                        ts_e_est TIMESTAMPTZ,
                        lastupdateid TEXT,
                        scd_from_date TIMESTAMPTZ,
                        scd_to_date TIMESTAMPTZ,
                        PRIMARY KEY (price, side, timestamp)
                    );
                """
                cursor.execute(create_tmp_table_query)
                conn.commit()

                xprint(">>>>> tmp_update_i: CREATE")
                xprint(pd.read_sql_query("select * from tmp_update_i",conn))                
                # Step 2: Truncate the temporary table to ensure it's clean
                truncate_tmp_table_query = "TRUNCATE TABLE tmp_update_i;"
                cursor.execute(truncate_tmp_table_query)
                conn.commit()
                xprint(">>>>> tmp_update_i: TRUNC")                
                xprint(pd.read_sql_query("select * from tmp_update_i",conn))

                # Step 3: Insert data into the temporary table using execute_values
                xprint("Step 3: Insert data ===>")
                # xprint(columns)
                # xprint(format(sql.SQL(', ').join(map(sql.Identifier, columns))))
                # sql_tmp_insert = sql.SQL("""
                #     INSERT INTO tmp_update_i ({})
                #     VALUES %s
                # """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))
                # (ticker, side ,price,px_00,quantity,qx_00,timestamp,ts_e_est,lastupdateid,scd_from_date,scd_to_date)
                sql_tmp_insert = sql.SQL("""
                    INSERT INTO tmp_update_i ({})
                    VALUES %s
                """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))

                execute_values(cursor, sql_tmp_insert, [tuple(record.values()) for record in update_i_records])
                conn.commit()
                xprint(">>>>> tmp_update_i: INSERT")      
                if yprint: 
                    df_print=pd.read_sql_query("select * from tmp_update_i",conn)
                    xprint(df_print[df_print['price']=='61227.27000000'])

            except Exception as e:
                conn.rollback()
                log_message(conn, 'ERROR', p_db_ver, f"***f_L01_update >> bulk_insert_update_i*** Error during bulk insert into tmp_update_i: {str(e)}")

        # --------------------------------------------------------------------------------------------
        #   UPDATE the Snapshot
        # --------------------------------------------------------------------------------------------
            # lprint(">>>>>>>>here<<<<<<<<<<",conn,p_db_ver)
            sql_snap_update = f"""
                INSERT INTO l2_snapshot_{p_side} (ticker, side, price,px_00, quantity, qx_00, timestamp, ts_e_est, lastupdateid)
                SELECT 
                    COALESCE(s.ticker, u.ticker),
                    COALESCE(s.side, u.side),
                    COALESCE(s.price, u.price),
                    COALESCE(s.px_00, u.px_00),
                    COALESCE(u.quantity, s.quantity),
                    COALESCE(u.qx_00, s.qx_00),
                    COALESCE(u.timestamp, s.timestamp) as timestamp,
                    COALESCE(u.ts_e_est, s.ts_e_est) as ts_e_est,                    
                    COALESCE(u.lastupdateid, s.lastupdateid)
                FROM l2_snapshot_{p_side} s
                FULL JOIN tmp_update_i u ON s.price = u.price AND s.side = u.side
                ON CONFLICT (price, side) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    px_00 = EXCLUDED.px_00,
                    quantity = EXCLUDED.quantity,
                    qx_00 = EXCLUDED.qx_00,
                    timestamp = EXCLUDED.timestamp,
                    ts_e_est = EXCLUDED.ts_e_est,
                    lastupdateid = EXCLUDED.lastupdateid;                
                ;
            """
            # cursor = conn.cursor()
            # cursor.execute(sql_snap_update)
            # conn.commit()

        # --------------------------------------------------------------------------------------------
        #   UPDATE the SCD History
        # --------------------------------------------------------------------------------------------

# Here --->>> !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            xprint(f">>>>> l2_history_{p_side}: PRE UPDATE")
            if yprint==True: 
                df_print = pd.read_sql_query(f"select * from l2_history_{p_side}",conn)
                xprint(df_print[df_print['price']=='61227.27000000'])                

            update_scd_query = f"""
                UPDATE l2_history_{p_side}
                SET scd_to_date = (SELECT ts_e_est FROM tmp_update_i WHERE l2_history_{p_side}.price = tmp_update_i.price AND l2_history_{p_side}.side = tmp_update_i.side)
                WHERE price IN (SELECT price FROM tmp_update_i)
                AND scd_to_date = '{max_scd_to_date}'
                AND (lastupdateid < (SELECT lastupdateid FROM tmp_update_i WHERE l2_history_{p_side}.price = tmp_update_i.price AND l2_history_{p_side}.side = tmp_update_i.side))
                AND (quantity != (SELECT quantity FROM tmp_update_i WHERE l2_history_{p_side}.price = tmp_update_i.price AND l2_history_{p_side}.side = tmp_update_i.side))
            """
            cursor.execute(update_scd_query)
            xprint(f">>>>> l2_history_{p_side}: POST UPDATE")
            if yprint==True: 
                df_print = pd.read_sql_query(f"select * from l2_history_{p_side}",conn)
                xprint(df_print[df_print['price']=='61227.27000000'])                

            # Error updating l2_snapshot_ask and l2_history_ask: duplicate key value violates unique constraint "l2_snapshot_ask_pkey"\nDETAIL: Key (price, side, "timestamp")=(60556.01000000, ASK, 2024-05-11 09:15:50.905435|EDT-0400) already exists.\n

        # --------------------------------------------------------------------------------------------
        #   INSERT into the SCD History
        # --------------------------------------------------------------------------------------------

                # INSERT INTO l2_history_{p_side} (ticker, side, price, timestamp, lastupdateid, quantity, scd_from_date, scd_to_date)
            insert_into_scd_query = f"""
                INSERT INTO l2_history_{p_side} (ticker, side, price, px_00, quantity, qx_00, timestamp, ts_e_est, lastupdateid, scd_from_date, scd_to_date)
                SELECT 
                    u.ticker,
                    u.side,
                    u.price,
                    u.px_00,
                    u.quantity,
                    u.qx_00,
                    u.timestamp,
                    u.ts_e_est,
                    u.lastupdateid,
                    u.scd_from_date,
                    '{max_scd_to_date}' as scd_to_date
                FROM tmp_update_i u
                LEFT JOIN l2_history_{p_side} s 
                    ON  u.price = s.price 
                    AND u.side = s.side
                    AND s.scd_to_date = '{max_scd_to_date}'
                WHERE  (s.lastupdateid IS NULL)
                   OR  (s.lastupdateid < u.lastupdateid AND s.quantity != u.quantity)
            """
            cursor.execute(insert_into_scd_query)

            xprint(f">>>>> l2_history_{p_side}: POST insert")
            if yprint==True: 
                df_print = pd.read_sql_query(f"select * from l2_history_{p_side}",conn)
                xprint(df_print[df_print['price']=='61227.27000000'])                

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!<<<---To Here

        # --------------------------------------------------------------------------------------------
        #   COMMIT Changes to db
        # --------------------------------------------------------------------------------------------
        
            rows_updated = cursor.rowcount
            rows_inserted = cursor.rowcount

            conn.commit()

            log_info(conn, p_db_ver, f"***f_L01_update*** Processed lastupdateid {lastupdateid}: {rows_inserted} rows inserted, {rows_updated} rows updated")

    except Exception as e:
        log_error(conn, p_db_ver, f"***f_L01_update*** Error updating l2_snapshot_{p_side} and l2_history_{p_side}: {str(e)}")

# #######################################################################################################
# MAIN
# #######################################################################################################

def main(p_ticker, p_dir_updates_root, p_db_ver):

    CONNECTION = "postgres://tsdbadmin:phnkvdq0tfttytfn@esd7mq3z84.dts890uzaz.tsdb.cloud.timescale.com:37281/tsdb?sslmode=require"
    conn = psycopg2.connect(CONNECTION)
    conn.autocommit = True
    cursor = conn.cursor()

    log_info(conn, p_db_ver, "***main*** Starting the update process")

    try:
        for iter, update_df in enumerate(g_E01_df(p_ticker=p_ticker, p_dir_updates_root=p_dir_updates_root, conn=conn,p_db_ver=p_db_ver)):
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            log_info(conn, p_db_ver, f"***f_L01*** PROCESSING #{iter} @ {now_ts}")
            f_L01_update(conn,cursor,p_db_ver, update_df[update_df['side'].str.upper() == 'ASK'], 'ask')
            f_L01_update(conn,cursor,p_db_ver, update_df[update_df['side'].str.upper() == 'BID'], 'bid')
        log_info(conn, p_db_ver, f"***f_L01*** PROCESSING Update Completed")
    except Exception as e:
        log_error(conn,p_db_ver, f"***main*** Error during the update process: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":

    n_ticker = "BTCUSDT"
    n_dir_updates_root = "../test/data/"
    n_db_ver = os.path.basename(__file__)
    n_yprint = False

    parser = argparse.ArgumentParser()

    parser.add_argument("--a_ticker", type=str, default=n_ticker)

    args = parser.parse_args()

    main(p_ticker=args.a_ticker,
         p_dir_updates_root=n_dir_updates_root,
         p_db_ver=n_db_ver)

# ???????????????????????????????????????????????????????????????????????
# sys.stdout = old_stdout
# log_file.close()
# ???????????????????????????????????????????????????????????????????????