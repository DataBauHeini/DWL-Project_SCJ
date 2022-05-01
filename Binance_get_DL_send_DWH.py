import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from decouple import config
from sqlalchemy import create_engine

# Credentials Data Lake
API_KEY = config('API_KEY')
API_SECRET = config('API_SECRET')
HOST = config('HOST')
DBNAME = config('DBNAME')
USER = config('USER')
PASSWORD = config('PASSWORD')
DATABASE_URL=config('DATABASE_URL')
PORT=config('PORT')

# Credentials Datawarehouse
DWH_HOST = config('DWH_HOST')
DWH_DBNAME = config('DWH_DBNAME')
DWH_USER = config('DWH_USER')
DWH_PASSWORD = config('DWH_PASSWORD')
DWH_DATABASE_URL = config('DWH_DATABASE_URL')

def get_load_data(cur, binance_trading_pair, method):
    # define variables for fetching data
    table_name_str = "'TABLE_"+binance_trading_pair+"_1d_data'"
    table_name = 'TABLE_'+binance_trading_pair+'_1d_data'
    df = pd.DataFrame()

    while True:
        # column names of the table in the datalake
        try:
            cur.execute('select COLUMN_NAME from information_schema.columns where table_name='+table_name_str)
            column_names = [row[0] for row in cur]

            column_date = column_names.index('timestamp')
            column_open = column_names.index('open')
            column_high = column_names.index('high')
            column_low = column_names.index('low')
            column_close = column_names.index('close')
            column_volume = column_names.index('volume')
            column_asset = column_names.index('trading_pair')

        except Exception as e:
            print("Column indexes could not be defined")
            print(e)
            break

        # fetch data in the datalake
        try:
            # cur.execute('select * from "'+table_name+'";')
            cur.execute('select * from "' + table_name + '" where timestamp = (select max(timestamp) from "' + table_name +'");')
            result = cur.fetchall()
        except psycopg2.Error as e:
            print('Error: select *')
            print(e)
            break

        # build dataframe
        try:
            for row in result:
                df = df.append(pd.DataFrame([{
                    'date': row[column_date],
                'asset': row[column_asset],
                'open': row[column_open],
                'high': row[column_high],
                'low': row[column_low],
                'close': row[column_close],
                'volume': row[column_volume]}]))
        except Exception as e:
            print('Dataframe could not be built')
            print(e)

        # check if data of yesterday is available and correct it if necessary
        yesterday = datetime.strftime(datetime.today() - timedelta(1), "%Y-%m-%d")
        date_df = df['date'].values[0]

        if date_df != yesterday:
            df['date'] = datetime.strptime(yesterday, "%Y-%m-%d").date()
        else:
            pass

            # define loading variables
        DWH_TABLE_NAME = 'crypto'
        if_ex_val = method

        # load data
        try:
            conn_string = DWH_DATABASE_URL
            engine = create_engine(conn_string)
            df.to_sql(DWH_TABLE_NAME, conn_string, if_exists=if_ex_val, index=False)
            print(table_name+" loaded")
        except Exception as e:
            print(e)
            print('Data load failed: '+table_name)
            break
        break

def close_conn_to_dl(cur, conn):
    try:
        cur.close()
        conn.close()
        print("connection closed")
    except psycopg2.Error as e:
        print("Error: Could not close")
        print(e)

def conn_to_dl():
    try:
        conn=psycopg2.connect(database=DBNAME,user='Jeffrey',password=PASSWORD, host=HOST, port=PORT)
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit
    conn.set_session(autocommit=True)
    print("connected")
    return cur, conn

binance_trading_pairs = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "LUNAUSDT", "ADAUSDT",
                         "SOLUSDT", "AVAXUSDT", "DOTUSDT", "DOGEUSDT"}


def main():
    # open connection
    cur, conn = conn_to_dl()

    # process data
    for binance_trading_pair in binance_trading_pairs:
        get_load_data(cur, binance_trading_pair, 'append')

        # close connection
    close_conn_to_dl(cur, conn)


if __name__ == "__main__":
    main()