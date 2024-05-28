import logging
import psycopg2
from utils import *
from psycopg2.extras import execute_values


# ============================================================================================
#   CONNECTION
# ============================================================================================

def create_connection():
    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_dbuser",
        password="your_dbpassword",
        host="your_dbhost",
        port="your_dbport"
    )
    return conn

# ============================================================================================
#   CONFIGURE Tables
# ============================================================================================

def setup_tables(conn):
    commands = [
        """
        CREATE TABLE IF NOT EXISTS l2_snapshot_ask (
            ticker VARCHAR(10),
            side VARCHAR(3),
            price FLOAT,
            timestamp TIMESTAMPTZ,
            lastupdateid BIGINT,
            quantity FLOAT,
            PRIMARY KEY (price, side,timestamp)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS l2_snapshot_bid (
            ticker VARCHAR(10),
            side VARCHAR(3),
            price FLOAT,
            timestamp TIMESTAMPTZ,
            lastupdateid BIGINT,
            quantity FLOAT,
            PRIMARY KEY (price, side,timestamp)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS l2_history_ask (
            ticker VARCHAR(10),
            side VARCHAR(3),
            price FLOAT,
            timestamp TIMESTAMPTZ,
            lastupdateid BIGINT,
            quantity FLOAT,
            scd_from_date TIMESTAMPTZ,
            scd_to_date TIMESTAMPTZ,
            PRIMARY KEY (price, side, scd_from_date)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS l2_history_bid (
            ticker VARCHAR(10),
            side VARCHAR(3),
            price FLOAT,
            timestamp TIMESTAMPTZ,
            lastupdateid BIGINT,
            quantity FLOAT,
            scd_from_date TIMESTAMPTZ,
            scd_to_date TIMESTAMPTZ,
            PRIMARY KEY (price, side, scd_from_date)
        );
        """,
        """
        SELECT create_hypertable('l2_history_ask', 'scd_from_date', if_not_exists => TRUE);
        """,
        """
        SELECT create_hypertable('l2_history_bid', 'scd_from_date', if_not_exists => TRUE);
        """
    ]
    cursor = conn.cursor()
    for command in commands:
        cursor.execute(command)
    conn.commit()
    cursor.close()

# ============================================================================================
#   EXEC Querry
# ============================================================================================

def f_exec_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

# ============================================================================================
#   Logging
# ============================================================================================

def print_table_info(conn, table_name,p_db_ver):
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = cursor.fetchall()
    
    print(f"Table: {table_name}")
    logging.info(f"Code: {p_db_ver}")
    logging.info(f"Table: {table_name}")
    for column in columns:
        print(f"  {column[0]}: {column[1]}")
        logging.info(f"  {column[0]}: {column[1]}")
    cursor.close()


# ============================================================================================
#   EMPTY Table
# ============================================================================================

def truncate_table(conn,p_db_ver, table_name,func):
    log_message(conn, 'INFO', f"***{func}*** All rows truncated from {table_name} successfully.")    
    try:
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE {table}").format(table=sql.Identifier(table_name))
        cursor.execute(truncate_query)
        conn.commit()
        log_message(conn, 'INFO', p_db_ver, f"***{func}*** All rows truncated from {table_name} successfully.")        
    except Exception as e:
        log_message(conn, 'ERROR', p_db_ver, f"***{func}*** Error truncating table {table_name}: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()    

# ============================================================================================
#   BULK Insert Table
# ============================================================================================

def bulk_insert_update_i(conn,p_db_ver, cursor, update_i):
    pass
    # try:
    #     update_i_records = update_i.to_dict('records')
    #     columns = update_i.columns.tolist()

    #     # Step 1: Create a temporary table
    #     create_tmp_table_query = sql.SQL("""
    #         CREATE TEMP TABLE tmp_update_i AS
    #         SELECT * FROM update_i WHERE 1=0;
    #     """)
    #     cursor.execute(create_tmp_table_query)
    #     conn.commit()

    #     # Step 2: Insert data into the temporary table using execute_values
    #     sql_tmp_insert = sql.SQL("""
    #         INSERT INTO tmp_update_i ({})
    #         VALUES %s
    #     """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))

    #     execute_values(cursor, sql_tmp_insert, [tuple(record.values()) for record in update_i_records])
    #     conn.commit()
    # except Exception as e:
    #     conn.rollback()
    #     log_message(conn, 'ERROR', p_db_ver, f"***f_L01_update >> bulk_insert_update_i*** Error during bulk insert into tmp_update_i: {str(e)}")

