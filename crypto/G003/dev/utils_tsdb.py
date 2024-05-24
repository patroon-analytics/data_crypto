import logging
import psycopg2

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