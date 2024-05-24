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
from io import StringIO  # Import StringIO from the io module


global yprint
yprint=False

def xprint(x):
    if yprint==True:
         print(x) 

def xdisplay(x):
    if yprint==True:
        try:
            display(x)
        except:
            xprint(x)

def f_convert_ts_to_dt(timestamp):
    ts_e_est = datetime.fromtimestamp(timestamp / 1000, pytz.timezone("US/Eastern")).strftime('%Y-%m-%d %H:%M:%S.%f')
    ts_e_cet = datetime.fromtimestamp(timestamp / 1000, pytz.timezone("Europe/Berlin")).strftime('%Y-%m-%d %H:%M:%S.%f')
    return pd.Series({'ts_e_est': ts_e_est, 'ts_e_cet': ts_e_cet})


# def f_exec_query(conn, query, params=()):
#     return pd.read_sql_query(query, conn, params=params)

def get_max_scd_to_date():
    max_date = datetime(9999, 12, 31, 23, 59, 59, 999999)
    return max_date.strftime('%Y-%m-%d %H:%M:%S.%f')

# ==========================================================================================
#   Logging
# ==========================================================================================

import logging
from datetime import datetime

def lprint(message):
    if yprint:
        print(message)
    log_message(conn, 'INFO', p_db_ver ,message)

def setup_logging_table(conn):
    logging_table_schema = """
    CREATE TABLE IF NOT EXISTS logs (
        timestamp TEXT,
        level TEXT,
        p_db_ver TEXT,
        message TEXT
    )
    """
    conn.execute(logging_table_schema)
    conn.commit()

def log_message(conn, level, p_db_ver, message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    log_entry = (timestamp, level, p_db_ver,message)
    insert_log_query = """
    INSERT INTO logs (timestamp, level, p_db_ver, message)
    VALUES (?, ?, ?, ?)
    """
    conn.execute(insert_log_query, log_entry)
    conn.commit()

def log_info(conn, p_db_ver, message):
    log_message(conn, "INFO", p_db_ver, message)

def log_error(conn, p_db_ver, message):
    log_message(conn, "ERROR", p_db_ver, message)

def print_table_info(conn, table_name):
    try:
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """
        cursor = conn.cursor()
        cursor.execute(query)
        columns = cursor.fetchall()
        
        print(f"Table: {table_name}")
        for column in columns:
            print(f"  {column[0]}: {column[1]}")
    except Exception as e:
        log_message(conn, 'ERROR', f"Error retrieving table info for {table_name}: {str(e)}")
    finally:
        cursor.close()

# ------------------------------------------------------------------------------------
#  Init Load
# ------------------------------------------------------------------------------------

def log_message_01_Init_Load(conn, log_level, message):
    cursor = conn.cursor()
    log_query = """
    INSERT INTO logs (log_level, log_message)
    VALUES (%s, %s);
    """
    cursor.execute(log_query, (log_level, message))
    conn.commit()
    cursor.close()

# ==========================================================================================
#   ETL
# ==========================================================================================

def copy_from_dataframe(conn, df, table):
    """
    Save the dataframe to the database using COPY FROM with StringIO
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    with conn.cursor() as cur:
        cur.copy_from(buffer, table, sep=',', null='')