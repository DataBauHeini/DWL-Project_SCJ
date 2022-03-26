from binance.client import Client
from dateutil import parser
from datetime import datetime
import pandas as pd
import os.path
import math
import time


Name_API_Key = "JeffreyRobinSachsAPIKey"
API_Key = 'mJEmINUxI135jU4J0mAf95tR4aRPA3YXih4wM5grNYHxoMGof7scbTlAuUla1rFq'
Secret_Key = 'sTSDq1vZB6Wujqr67iwSUDiRxNgYE8POGVg6j4wbs6SOqutmJzb81YDNV87cFCjZ'


# Fixed variables
timesteps = {'1d': 1440}
client = Client(api_key = API_Key, api_secret = Secret_Key)


# Functions
def fetch_timing(trading_pair, step_size, data, source):
    if len(data) > 0:
        before = parser.parse(data['timestamp'].iloc[-1])
    elif source == 'binance':
        before = datetime.strptime('01 Jan 2017', '%d %b %Y')
    if source == 'binance':
        after = pd.to_datetime(client.get_klines(symbol=trading_pair, interval=step_size)[-1][0], unit='ms')
    return before, after


def retrieve_data(trading_pair, step_size, save=False):
    docname = '%s-1d-data.csv' % trading_pair
    if os.path.isfile(docname):
        dataframe = pd.read_csv(docname)
    else:
        dataframe = pd.DataFrame()

    before_time, after_time = fetch_timing(trading_pair, step_size, dataframe, source='binance')
    difference_minutes = (after_time - before_time).total_seconds() / 60
    available_data = math.floor(difference_minutes / timesteps[step_size])
    if before_time == datetime.strptime('01 Jan 2017', '%d %b %Y'):
        print('Getting all data for', step_size, trading_pair)
    else:
        print('Getting additional data for', step_size, trading_pair)

    candlesticks = client.get_historical_klines(trading_pair, step_size, before_time.strftime('%d %b %Y'),
                                                after_time.strftime("%d %b %Y"))

    full_info = pd.DataFrame(candlesticks, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                                    'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    full_info['timestamp'] = pd.to_datetime(full_info['timestamp'], unit='ms')

    if len(dataframe) > 0:
        additional_dataframe = pd.DataFrame(full_info)
        dataframe = dataframe.append(additional_dataframe)
    else:
        dataframe = full_info

    dataframe.set_index('timestamp', inplace=True)
    if save:
        dataframe.to_csv(docname)
    print('Done')
    return dataframe


# Get all Top10 trading pairs (without stable coins - based on Binance)
binance_trading_pairs = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "LUNAUSDT", "ADAUSDT", "SOLUSDT", "AVAXUSDT",
                   "DOTUSDT", "DOGEUSDT"]
for trading_pair in binance_trading_pairs:
    retrieve_data(trading_pair, '1d', save = True)

import psycopg2
from sqlalchemy import create_engine

try:
    conn = psycopg2.connect(
        "host=jeffreydb.c4eax0zhcvkf.us-east-1.rds.amazonaws.com dbname=datalake1 user=Jeffrey password=JeffreyDB")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# Auto commit is very important
conn.set_session(autocommit=True)

conn_string = 'postgresql://Jeffrey:JeffreyDB@jeffreydb.c4eax0zhcvkf.us-east-1.rds.amazonaws.com/datalake1'

import glob
path = '/Users/jeffreysachs/Desktop/HSLU/Semester 3/DWL1/Project'
files = glob.glob(path + "/*.csv")

for filename in files:
    df = pd.read_csv(filename, index_col=None)
    db = create_engine(conn_string)
    conn = db.connect()
    start_time = time.time()
    df.to_sql('TABLE_'+trading_pair+'-1d-data', con=conn, if_exists='replace', index=False)

print('Export successful')
