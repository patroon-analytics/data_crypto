import psycopg2
from psycopg2 import sql
from utils import *
from utils_tsdb import *
import argparse
import shutil
import re
import logging

# Configure logging
logging.basicConfig(filename='logs/create_db_01.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# ============================================================================================
#   DEFINE Main
# ============================================================================================

def create_connection():
    # conn = psycopg2.connect(
    #     dbname='your_database',
    #     user='your_user',
    #     password='your_password',
    #     host='your_host',
    #     port='your_port'
    # )

    CONNECTION = "postgres://tsdbadmin:phnkvdq0tfttytfn@esd7mq3z84.dts890uzaz.tsdb.cloud.timescale.com:37281/tsdb?sslmode=require"
    
    conn = psycopg2.connect(CONNECTION)
    conn.autocommit = True

    return conn

# ============================================================================================
#   CONFIGURE Tables
# ============================================================================================

def create_tables_and_hypertables(conn,p_db_ver,):
    commands = [
        """
        CREATE TABLE IF NOT EXISTS l2_snapshot_ask (
            ticker TEXT,
            side TEXT,
            price TEXT,
            px_00 NUMERIC(1000, 15),
            quantity TEXT,
            qx_00 NUMERIC(1000, 15),                  
            timestamp TEXT,
            ts_e_est TIMESTAMPTZ,
            lastupdateid TEXT,
            PRIMARY KEY (price, side,timestamp)            
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS l2_snapshot_bid (
            ticker TEXT,
            side TEXT,
            price TEXT,
            px_00 NUMERIC(1000, 15),
            quantity TEXT,
            qx_00 NUMERIC(1000, 15),                  
            timestamp TEXT,
            ts_e_est TIMESTAMPTZ,
            lastupdateid TEXT,
            PRIMARY KEY (price, side,timestamp)            
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS l2_history_ask (
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
            PRIMARY KEY (price, side, scd_from_date)            
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS l2_history_bid (
            ticker TEXT,
            side VARCHAR(10),
            price TEXT,
            px_00 NUMERIC(1000, 15),
            quantity TEXT,
            qx_00 NUMERIC(1000, 15),                  
            timestamp TEXT,
            ts_e_est TIMESTAMPTZ,
            lastupdateid TEXT,
            scd_from_date TIMESTAMPTZ,
            scd_to_date TIMESTAMPTZ,
            PRIMARY KEY (price, side, scd_from_date)            
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS logs (
                timestamp TEXT,
                level TEXT,
                p_db_ver TEXT,
                message TEXT
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS update_i (
            ticker TEXT,
            side VARCHAR(10),
            price TEXT,
            px_00 NUMERIC(1000, 15),
            quantity TEXT,
            qx_00 NUMERIC(1000, 15),                  
            timestamp TEXT,
            lastupdateid TEXT,
            timestampevent TIMESTAMPTZ,
            ts_p TIMESTAMPTZ,            
            ts_p_03 TIMESTAMPTZ,
            ts_e_est TIMESTAMPTZ,                        
            PRIMARY KEY (price, side, timestamp)            
        );
        """
    ]

    hypertable_commands = [
        "SELECT create_hypertable('l2_snapshot_ask', 'timestamp', if_not_exists => TRUE);",
        "SELECT create_hypertable('l2_snapshot_bid', 'timestamp', if_not_exists => TRUE);",
        "SELECT create_hypertable('l2_history_ask', 'scd_from_date', if_not_exists => TRUE);",
        "SELECT create_hypertable('l2_history_bid', 'scd_from_date', if_not_exists => TRUE);",
        "SELECT create_hypertable('update_i', 'timestamp', if_not_exists => TRUE);"
    ]

    try:
        cursor = conn.cursor()
        for command in commands:
            try:
                print("***CREATING Table*** "+str(re.search(r'l2\w+', command).group()))
            except:
                print("***CREATING Table*** logs")
                
            cursor.execute(command)
        for hypertable_command in hypertable_commands:
            # xprint("***CREATING Hypertable*** "++str(re.search(r"'l2\w+", hypertable_command).group()))            
            cursor.execute(hypertable_command)
        conn.commit()
        
        # Log and print table information
        tables = ['l2_snapshot_ask', 'l2_snapshot_bid', 'l2_history_ask', 'l2_history_bid','update_i']
        for table in tables:
            print_table_info(conn, table,p_db_ver)

        cursor.close()
        xprint("***CREATING Table END***")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# ============================================================================================
#   MAIN
# ============================================================================================

def main(p_snap_init_file_dir, p_db_ver, p_yprint, p_reset):

    if not p_reset:   
        conn = create_connection()
        create_tables_and_hypertables(conn,p_db_ver)
    else:
        ## MOVE Files back to raw folder
        for filename in os.listdir(p_snap_init_file_dir + "011_processed"):
            shutil.move(os.path.join(p_snap_init_file_dir + "011_processed/", filename), 
                        os.path.join(p_snap_init_file_dir + "001_raw/", filename))
            xprint("Moved: "+str(filename))
            
        ## DELETE tables
        # with psycopg2.connect(CONNECTION) as conn:
            # conn.autocommit = True
        with create_connection() as conn:
            with conn.cursor() as cursor:
                for side in ['ask', 'bid']:
                    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(table=sql.Identifier(f'l2_history_{side}')))
                    print(f"Table 'l2_history_{side}' deleted successfully.")
                    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(table=sql.Identifier(f'l2_snapshot_{side}')))
                    print(f"Table 'l2_snapshot_{side}' deleted successfully.")

                truncate_tmp_table_query = "TRUNCATE TABLE logs;"
                cursor.execute(truncate_tmp_table_query)
                conn.commit()

if __name__ == '__main__':

    n_snap_init_file_dir = "../test/data/"
    n_db_ver = os.path.basename(__file__)
    n_yprint = True    
    n_reset = False

    parser = argparse.ArgumentParser()

    parser.add_argument("--a_snap_init_file_dir", type=str, default=n_snap_init_file_dir)
    # parser.add_argument("--a_db_ver", type=str, default=n_db_ver)
    parser.add_argument("--a_yprint", type=bool, default=n_yprint)        
    parser.add_argument("--a_reset", type=bool, default=n_reset)            

    args = parser.parse_args()

    main(p_snap_init_file_dir=args.a_snap_init_file_dir,
        p_db_ver=n_db_ver,
        p_yprint=args.a_yprint,
        p_reset=args.a_reset)
