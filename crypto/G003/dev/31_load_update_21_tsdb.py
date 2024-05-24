
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
from datetime import datetime
import shutil

from utils import f_convert_ts_to_dt, f_exec_query, xprint, xdisplay, setup_logging_table, log_message, log_info, log_error, get_max_scd_to_date

def g_E01_df(p_ticker, p_dir_updates_root, conn):

    p_dir_updates = p_dir_updates_root + "001_raw/"

    filenames = [f for f in os.listdir(p_dir_updates) if f.startswith('l2_' + p_ticker) and f.endswith('.feather')]
    filenames.sort()

    for filename in filenames:
        try:
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            xprint("***g_E01*** READING-IN: " + str(filename) + " @ " + now_ts)
            l2_update_02 = pd.read_feather(os.path.join(p_dir_updates, filename))

            l2_update_02 = l2_update_02.drop_duplicates()
            xprint("len-post: " + str(len(l2_update_02)))
            l2_update_02.columns = map(str.lower, l2_update_02.columns)
            l2_update_02['ts_p'] = l2_update_02['timestamp'].str.split('|').str[0]
            l2_update_02['ts_p_03'] = pd.to_datetime(l2_update_02['ts_p'], format="%Y-%m-%d %H:%M:%S.%f").dt.round("s")
            l2_update_02[['ts_e_est', 'ts_e_cet']] = l2_update_02['timestampevent'].apply(f_convert_ts_to_dt)
            l2_update_02 = l2_update_02.sort_values(by=['price', 'lastupdateid'])

            shutil.move(os.path.join(p_dir_updates_root + "001_raw/", filename),
                        os.path.join(p_dir_updates_root + "011_processed/", filename))

            yield l2_update_02
        except Exception as e:
            log_error(conn, f"Error processing file {filename}: {str(e)}")
            continue

def f_L01_update(conn, df_updates, p_side):
    try:
        l_lastid_upd_i = sorted(df_updates['lastupdateid'].unique())

        for lastupdateid in l_lastid_upd_i:
            xprint("------------------------------------------------------")
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            update_i = df_updates[df_updates['lastupdateid'] == lastupdateid].copy()
            xprint("lastupdateid = " + str(lastupdateid) + " @ " + str(current_timestamp))
            xprint("# rows in the update: " + str(len(update_i)))

            # Use execute_values for bulk insert
            update_i_records = update_i.to_dict('records')
            columns = update_i.columns.tolist()
            sql = f"""
                INSERT INTO update_i ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (lastupdateid) DO NOTHING
            """
            execute_values(conn.cursor(), sql, update_i_records)

            # Update the snapshot table
            sql_query = f"""
                INSERT INTO l2_snapshot_{p_side} (ticker, side, price, timestamp, lastupdateid, quantity)
                SELECT 
                    COALESCE(s.ticker, u.ticker),
                    COALESCE(s.side, u.side),
                    COALESCE(s.price, u.price),
                    COALESCE(u.ts_e_est, s.timestamp),
                    COALESCE(u.lastupdateid, s.lastupdateid),
                    COALESCE(u.quantity, s.quantity)
                FROM l2_snapshot_{p_side} s
                LEFT JOIN update_i u ON s.price = u.price AND s.side = u.side
                UNION ALL
                SELECT 
                    u.ticker,
                    u.side,
                    u.price,
                    u.ts_e_est,
                    u.lastupdateid,
                    u.quantity
                FROM update_i u
                LEFT JOIN l2_snapshot_{p_side} s ON s.price = u.price AND s.side = u.side
                WHERE s.price IS NULL
                ORDER BY price
                ON CONFLICT (price, side) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    timestamp = EXCLUDED.timestamp,
                    lastupdateid = EXCLUDED.lastupdateid,
                    quantity = EXCLUDED.quantity
            """
            cursor = conn.cursor()
            cursor.execute(sql_query)
            conn.commit()

            # Update the SCD history
            max_scd_to_date = get_max_scd_to_date()
            update_scd_query = f"""
                UPDATE l2_history_{p_side}
                SET scd_to_date = (SELECT ts_e_est FROM update_i WHERE l2_history_{p_side}.price = update_i.price AND l2_history_{p_side}.side = update_i.side)
                WHERE price IN (SELECT price FROM update_i)
                AND scd_to_date = '{max_scd_to_date}'
                AND (lastupdateid < (SELECT lastupdateid FROM update_i WHERE l2_history_{p_side}.price = update_i.price AND l2_history_{p_side}.side = update_i.side))
                AND (quantity != (SELECT quantity FROM update_i WHERE l2_history_{p_side}.price = update_i.price AND l2_history_{p_side}.side = update_i.side))
            """
            cursor.execute(update_scd_query)

            insert_into_scd_query = f"""
                INSERT INTO l2_history_{p_side} (ticker, side, price, timestamp, lastupdateid, quantity, scd_from_date, scd_to_date)
                SELECT 
                    u.ticker,
                    u.side,
                    u.price,
                    u.ts_e_est,
                    u.lastupdateid,
                    u.quantity,
                    u.ts_e_est as scd_from_date,
                    '{max_scd_to_date}' as scd_to_date
                FROM update_i u
                LEFT JOIN l2_history_{p_side} s 
                    ON  u.price = s.price 
                    AND u.side = s.side
                    AND s.scd_to_date = '{max_scd_to_date}'
                WHERE  (s.lastupdateid IS NULL)
                   OR  (s.lastupdateid < u.lastupdateid AND s.quantity != u.quantity)
            """
            cursor.execute(insert_into_scd_query)
            conn.commit()

            rows_updated = cursor.rowcount
            rows_inserted = cursor.rowcount

            log_info(conn, f"Processed lastupdateid {lastupdateid}: {rows_inserted} rows inserted, {rows_updated} rows updated")

    except Exception as e:
        log_error(conn, f"Error updating l2_snapshot_{p_side} and l2_history_{p_side}: {str(e)}")

def main(p_ticker, p_dir_updates_root, p_db_ver, p_yprint):

    global yprint
    yprint = p_yprint

    conn = create_connection()
    setup_logging_table(conn)
    log_info(conn, "Starting the update process")

    try:
        for update_df in g_E01_df(p_ticker=p_ticker, p_dir_updates_root=p_dir_updates_root, conn=conn):
            now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            xprint("***f_L01*** PROCESSING @ " + now_ts)
            f_L01_update(conn, update_df[update_df['side'].str.upper() == 'ASK'], 'ask')
            f_L01_update(conn, update_df[update_df['side'].str.upper() == 'BID'], 'bid')
        log_info(conn, "Update process completed successfully")
    except Exception as e:
        log_error(conn, f"Error during the update process: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":

    n_ticker = "BTCUSDT"
    n_dir_updates_root = "../test/data/"
    n_db_ver = "l2_G002_D002"
    n_yprint = False

    parser = argparse.ArgumentParser()

    parser.add_argument("--a_ticker", type=str, default=n_ticker)
    parser.add_argument("--a_dir_updates_root", type=str, default=n_dir_updates_root)
    parser.add_argument("--a_db_ver", type=str, default=n_db_ver)
    parser.add_argument("--a_yprint", type=bool, default=n_yprint)

    args = parser.parse_args()

    main(p_ticker=args.a_ticker,
         p_dir_updates_root=args.a_dir_updates_root,
         p_db_ver=args.a_db_ver,
         p_yprint=args.a_yprint)
