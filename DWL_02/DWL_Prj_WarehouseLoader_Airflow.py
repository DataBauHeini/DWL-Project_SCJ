# import necessary packages
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# defining necessary functions for Airflow
def start():
    logging.info('Starting the DAG "Loading and transforming data from lake to warehouse ...')


def twi_etl_done():
    logging.info('Twitter ETL process successfully done.\nStarting with Yahoo finance ...')


def yh_etl_done():
    logging.info('Yahoo ETL process successfully done.\nStarting with Binance ...')


def bi_etl_done():
    logging.info('Binance ETL process successfully done.\nStarting with Reddit ...')


def end():
    logging.info('Transfer from data lake to data warehouse successfully finished.')


def fetch_trans_twitter_lake(ti, **kwargs):
    asset = kwargs.get('templates_dict').get('asset_tw', None)
    # define variables for fetching data
    table_name_str = "'table_tw_"+asset+"'"
    table_name = 'table_tw_'+asset

    sql_clmnName_stmt = "select COLUMN_NAME from information_schema.columns where table_name="+table_name_str
    sql_exe_stmt = "SELECT * FROM "+table_name+" WHERE date = (SELECT MAX(date) FROM "+table_name+");"
    pg_twi_lake_hook = PostgresHook(
        postgres_conn_id='yahoo_postgresDB_lake'
    )
    pg_twi_lake_conn = pg_twi_lake_hook.get_conn()
    twi_cur = pg_twi_lake_conn.cursor()

    df = pd.DataFrame()

    while True:
        # column names of the table in the data lake
        try:
            twi_cur.execute(sql_clmnName_stmt)
            column_names = [row[0] for row in twi_cur]

            col_ind_date = column_names.index('date')
            col_ind_tc = column_names.index('tweet_count')
            col_ind_asset = column_names.index('label')
        except Exception as e:
            logging.info("Column indexes could no be defined")
            logging.info(e)
            break

        # fetch data in data lake
        try:
            twi_cur.execute(sql_exe_stmt)
            result = twi_cur.fetchall()
        except Exception as e:
            logging.info("Error: select *")
            logging.info(e)
            break

        # build dataframe
        try:
            for row in result:
                df = df.append(pd.DataFrame([{'date': row[col_ind_date],
                                              'asset': row[col_ind_asset],
                                              'count': row[col_ind_tc],
                                              'source': 'twitter'}]))
        except Exception as e:
            logging.info("Dataframe could not be built")
            logging.info(e)
            break

        # change date format
        date_df = df['date'].values[0]
        timestamp = ((date_df - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
        df['date'] = datetime.utcfromtimestamp(timestamp)

        # close connection
        try:
            twi_cur.close()
            pg_twi_lake_conn.close()
            logging.info("connection closed")
        except Exception as e:
            logging.info("Error: Could not close")
            logging.info(e)
            break

        # push information part
        try:
            # first serialize the df to JSON, otherwise it is not possible to push a dataframe
            df_seri = df.to_json()
            logging.info(f'Variables to push - the_df: {df_seri} ...')
            ti.xcom_push(key=f'the_twitter_df_{asset}', value=df_seri)
            logging.info('Variables pushed.')
        except Exception as e:
            logging.info('Information can not be pushed to XCOM')
            logging.info(e)


def load_twi_wh(ti, **kwargs):
    asset = kwargs.get('templates_dict').get('asset_tw', None)
    # define loading variables
    table_name = 'table_tw_'+asset
    dwhtblname = 'social_media'
    if_ex_val = 'append'

    # pulling df
    try:
        logging.info('Pulling infos ...')
        df = ti.xcom_pull(task_ids=f'get_twitter_data_{asset}', key=f'the_twitter_df_{asset}')
        logging.info("Received asset: '%s'" % asset)
    except Exception as e:
        logging.info('Information can not be pulled from XCOM')
        logging.info(e)

    # convert JSON to Pandas_df
    df = pd.read_json(df)

    # get the DW credentials out of variables
    dwhuname = Variable.get('dwhuname_cr_1')
    dwhpwd = Variable.get('dwhpwd_cr_1')
    dwhhost = Variable.get('dwhhost_cr_1')
    port = Variable.get('port_cr_1')
    dwhdbname = Variable.get('dwhdbname_cr_1')

    try:
        conn_string = 'postgresql://'+dwhuname+':'+dwhpwd+'@'+dwhhost+':'+port+'/'+dwhdbname
        # engine = create_engine(conn_string)
        df.to_sql(dwhtblname, conn_string, if_exists=if_ex_val, index=False)
        logging.info(table_name + " loaded")
    except Exception as e:
        logging.info(e)
        logging.info("Data load failed: " + table_name)


def fetch_trans_yahoo_lake(ti, **kwargs):
    asset2 = kwargs.get('templates_dict').get('asset_yf', None)
    # define variables for fetching data
    table_name_str = "'table_yf_" + asset2 + "'"
    table_name = 'table_yf_' + asset2

    sql_clmnName_stmt2 = "select COLUMN_NAME from information_schema.columns where table_name=" + table_name_str
    sql_exe_stmt2 = "SELECT * FROM " + table_name + " WHERE date = (SELECT MAX(date) FROM " + table_name + ");"
    pg_yh_lake_hook = PostgresHook(
        postgres_conn_id='yahoo_postgresDB_lake'
    )
    pg_yh_lake_conn = pg_yh_lake_hook.get_conn()
    yh_cur = pg_yh_lake_conn.cursor()

    df2 = pd.DataFrame()

    while True:
        # column names of the table in the data lake
        try:
            yh_cur.execute(sql_clmnName_stmt2)
            column_names = [row[0] for row in yh_cur]

            col_ind_date = column_names.index('date')
            col_ind_open = column_names.index('Open')
            col_ind_high = column_names.index('High')
            col_ind_low = column_names.index('Low')
            col_ind_close = column_names.index('Close')
            col_ind_vol = column_names.index('Volume')
            col_ind_asset = column_names.index('Ins_label')
            logging.info(f'Col_indexes successfully built:\n'
                         f'- col_ind_date: {col_ind_date}\n'
                         f'- col_ind_open: {col_ind_open}\n'
                         f'- col_ind_high: {col_ind_high}\n'
                         f'- col_ind_low: {col_ind_low}\n'
                         f'- col_ind_close: {col_ind_close}\n'
                         f'- col_ind_vol: {col_ind_vol}\n'
                         f'- col_ind_asset: {col_ind_asset} ...')
        except Exception as e:
            logging.info("Column indexes could no be defined")
            logging.info(e)
            break

        # fetch data in data lake
        try:
            yh_cur.execute(sql_exe_stmt2)
            result = yh_cur.fetchall()
            logging.info("Fetch data in DL successful ...")
        except Exception as e:
            logging.info("Error: select *")
            logging.info(e)
            break

        # build dataframe
        try:
            for row in result:
                df2 = df2.append(pd.DataFrame([{'date': row[col_ind_date],
                                              'asset': row[col_ind_asset],
                                              'open': row[col_ind_open],
                                              'high': row[col_ind_high],
                                              'low': row[col_ind_low],
                                              'close': row[col_ind_close],
                                              'volume': row[col_ind_vol]}]))
            logging.info(f"Data frame successfully built:\n {df2.head()}")
        except Exception as e:
            logging.info("Dataframe could not be built")
            logging.info(e)

        # check if data of yesterday is available and correct it if necessary
        yesterday = datetime.strftime(datetime.today() - timedelta(1), "%Y-%m-%d")
        date_df2 = df2['date'].values[0]
        logging.info(f'Resulting date_df2: {date_df2} ...')
        date_df2 = np.datetime_as_string(date_df2, unit='D')

        if date_df2 != yesterday:
            logging.info("date_df2 != yesterday")
            df2['date'] = datetime.strptime(yesterday, "%Y-%m-%d").date()
        else:
            logging.info("date_df2 == yesterday")
            pass

        # close connection
        try:
            yh_cur.close()
            pg_yh_lake_conn.close()
            logging.info("connection closed")
        except Exception as e:
            logging.info("Error: Could not close")
            logging.info(e)
            break

        # push information part
        try:
            # first serialize the df to JSON, otherwise it is not possible to push a dataframe
            df2_seri = df2.to_json()
            logging.info(f'Variables to push - the_df: {df2_seri} ...')
            ti.xcom_push(key=f'the_yahoo_df_{asset2}', value=df2_seri)
            logging.info('Variables pushed.')
        except Exception as e:
            logging.info('Information can not be pushed to XCOM')
            logging.info(e)


def load_yh_wh(ti, **kwargs):
    asset2 = kwargs.get('templates_dict').get('asset_yf', None)
    # define loading variables
    table_name = 'table_yf_'+asset2
    dwhtblname = 'finance'
    if_ex_val = 'append'

    # pulling df
    try:
        logging.info('Pulling infos ...')
        df2 = ti.xcom_pull(task_ids=f'get_yahoo_data_{asset2}', key=f'the_yahoo_df_{asset2}')
        logging.info("Received asset: '%s'" % asset2)
    except Exception as e:
        logging.info('Information can not be pulled from XCOM')
        logging.info(e)

    # convert JSON to Pandas_df
    df2 = pd.read_json(df2)

    # get the DW credentials out of variables
    dwhuname = Variable.get('dwhuname_cr_1')
    dwhpwd = Variable.get('dwhpwd_cr_1')
    dwhhost = Variable.get('dwhhost_cr_1')
    port = Variable.get('port_cr_1')
    dwhdbname = Variable.get('dwhdbname_cr_1')

    # load data
    try:
        conn_string = 'postgresql://' + dwhuname + ':' + dwhpwd + '@' + dwhhost + ':' + port + '/' + dwhdbname
        # engine = create_engine(conn_string)
        df2.to_sql(dwhtblname, conn_string, if_exists=if_ex_val, index=False)
        logging.info(table_name + " loaded")
    except Exception as e:
        logging.info(e)
        logging.info("Data load failed: " + table_name)


def fetch_trans_binance_lake(ti, **kwargs):
    asset3 = kwargs.get('templates_dict').get('asset_bi', None)
    # define variables for fetching data
    table_name_str = "'TABLE_"+asset3+"_1d_data'"
    table_name = 'TABLE_'+asset3+'_1d_data'

    sql_clmnName_stmt3 = "select COLUMN_NAME from information_schema.columns where table_name=" + table_name_str
    sql_exe_stmt3 = 'select * from "' + table_name + '" where timestamp = (select max(timestamp) from "' + table_name +'");'
    pg_bi_lake_hook = PostgresHook(
        postgres_conn_id='binance_postgresDB_lake'
    )
    pg_bi_lake_conn = pg_bi_lake_hook.get_conn()
    bi_cur = pg_bi_lake_conn.cursor()

    df3 = pd.DataFrame()

    while True:
        # column names of the table in the datalake
        try:
            bi_cur.execute(sql_clmnName_stmt3)
            column_names = [row[0] for row in bi_cur]

            column_date = column_names.index('timestamp')
            column_open = column_names.index('open')
            column_high = column_names.index('high')
            column_low = column_names.index('low')
            column_close = column_names.index('close')
            column_volume = column_names.index('volume')
            column_asset = column_names.index('trading_pair')

        except Exception as e:
            logging.info("Column indexes could not be defined")
            logging.info(e)
            break

        # fetch data in the datalake
        try:
            # cur.execute('select * from "'+table_name+'";')
            bi_cur.execute(sql_exe_stmt3)
            result = bi_cur.fetchall()
        except Exception as e:
            logging.info('Error: select *')
            logging.info(e)
            break

        # build dataframe
        try:
            # dict of asset names
            asset_dict = {"BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "BNBUSDT": "binance", "XRPUSDT": "ripple",
                          "LUNAUSDT": "terra", "ADAUSDT": "cardano", "SOLUSDT": "solana", "AVAXUSDT": "avalanche",
                          "DOTUSDT": "polkadot", "DOGEUSDT": "dogecoin"}

            for row in result:
                df3 = df3.append(pd.DataFrame([{'date': row[column_date],
                                              'asset': asset_dict[row[column_asset]],   # renaming workaround
                                              'open': row[column_open],
                                              'high': row[column_high],
                                              'low': row[column_low],
                                              'close': row[column_close],
                                              'volume': row[column_volume]}]))
        except Exception as e:
            logging.info('Dataframe could not be built')
            logging.info(e)

        # check if data of yesterday is available and correct it if necessary
        yesterday = datetime.strftime(datetime.today() - timedelta(1), "%Y-%m-%d")
        date_df3 = df3['date'].values[0]

        if date_df3 != yesterday:
            df3['date'] = datetime.strptime(yesterday, "%Y-%m-%d").date()
        else:
            pass

        # close connection
        try:
            bi_cur.close()
            pg_bi_lake_conn.close()
            logging.info("connection closed")
        except Exception as e:
            logging.info("Error: Could not close")
            logging.info(e)
            break

        # push information part
        try:
            # first serialize the df to JSON, otherwise it is not possible to push a dataframe
            df3_seri = df3.to_json()
            logging.info(f'Variables to push - the_df: {df3_seri} ...')
            ti.xcom_push(key=f'the_binance_df_{asset3}', value=df3_seri)
            logging.info('Variables pushed.')
        except Exception as e:
            logging.info('Information can not be pushed to XCOM')
            logging.info(e)


def load_bi_wh(ti, **kwargs):
    asset3 = kwargs.get('templates_dict').get('asset_bi', None)
    # define loading variables
    table_name = 'table_tw_'+asset3
    dwhtblname = 'crypto'
    if_ex_val = 'append'

    # pulling df3
    try:
        logging.info('Pulling infos ...')
        df3 = ti.xcom_pull(task_ids=f'get_binance_data_{asset3}', key=f'the_binance_df_{asset3}')
        logging.info("Received asset: '%s'" % asset3)
    except Exception as e:
        logging.info('Information can not be pulled from XCOM')
        logging.info(e)

    # convert JSON to Pandas_df
    df3 = pd.read_json(df3)

    # get the DW credentials out of variables
    dwhuname = Variable.get('dwhuname_cr_1')
    dwhpwd = Variable.get('dwhpwd_cr_1')
    dwhhost = Variable.get('dwhhost_cr_1')
    port = Variable.get('port_cr_1')
    dwhdbname = Variable.get('dwhdbname_cr_1')

    # load data
    try:
        conn_string = 'postgresql://' + dwhuname + ':' + dwhpwd + '@' + dwhhost + ':' + port + '/' + dwhdbname
        # engine = create_engine(conn_string)
        df3.to_sql(dwhtblname, conn_string, if_exists=if_ex_val, index=False)
        logging.info(table_name + " loaded")
    except Exception as e:
        logging.info(e)
        logging.info("Data load failed: " + table_name)


# specify the general DAG
dag = DAG(dag_id='DWL_Prj_DWH_Loader',
    start_date=datetime(2022, 5, 14),
    # schedule_interval=timedelta(days=1)
    schedule_interval='@daily'
)

# specify tasks which are running only once
greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag
)

twi_ETL_done_task = PythonOperator(
    task_id='twi_ETL_done',
    python_callable=twi_etl_done,
    trigger_rule='all_done',
    dag=dag
)

yh_ETL_done_task = PythonOperator(
    task_id='yh_ETL_done',
    python_callable=yh_etl_done,
    trigger_rule='all_done',
    dag=dag
)

bi_ETL_done_task = PythonOperator(
    task_id='binance_ETL_done',
    python_callable=bi_etl_done,
    trigger_rule='all_done',
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=end,
    dag=dag
)

# Lists to adjust the No. and topics of tasks
assets = ['bitcoin', 'ethereum', 'binance', 'ripple', 'terra', 'cardano', 'solana', 'avalanche', 'polkadot',
          'dogecoin', 'msci_world', 'euro_stoxx', 'smi', 'nasdaq', 'gold', 'silver']
assets_yh = ['gold', 'silver', 'msci_world', 'euro_stoxx', 'smi', 'nasdaq']
assets_binance = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "LUNAUSDT", "ADAUSDT", "SOLUSDT", "AVAXUSDT",
                  "DOTUSDT", "DOGEUSDT"]

with dag:
    # with this for loop, the defined tasks here are executed parallel for each asset in the list before
    for asset in assets:
        # 1. Get the twitter data from PostgreSQL datalake
        task_fetch_trans_tw = [
            PythonOperator(
            task_id=f'get_twitter_data_{asset}',
            python_callable=fetch_trans_twitter_lake,
            do_xcom_push=True,
            provide_context=True,
            templates_dict={
                'asset_tw': asset}
            )
        ]

        # 2. only to be sure the operator can have a short break and not confuses itself ;)
        sleep_task_tw = BashOperator(
            task_id=f'sleep_1s_{asset}_twitter',
            bash_command='sleep 1',
            trigger_rule='all_done')

        # 3. Load the twitter data to PostgreSQL datawarehouse
        task_load_tw = [
            PythonOperator(
            task_id=f'load_twitter_data_{asset}',
            python_callable=load_twi_wh,
            do_xcom_push=True,
                provide_context=True,
                templates_dict={
                    'asset_tw': asset}
            )
        ]

        greet_task >> task_fetch_trans_tw >> sleep_task_tw >> task_load_tw >> twi_ETL_done_task

    for asset_yf in assets_yh:
        # 1. Get the yahoo finance data from PostgreSQL datalake
        task_fetch_trans_yf = [
            PythonOperator(
                task_id=f'get_yahoo_data_{asset_yf}',
                python_callable=fetch_trans_yahoo_lake,
                do_xcom_push=True,
                provide_context=True,
                templates_dict={
                    'asset_yf': asset_yf}
            )
        ]

        # 2. only to be sure the operator can have a short break and not confuses itself ;)
        sleep_task_yf = BashOperator(
            task_id=f'sleep_1s_{asset_yf}_yahoo',
            bash_command='sleep 1')

        # 3. Load the twitter data to PostgreSQL datawarehouse
        task_load_yf = [
            PythonOperator(
                task_id=f'load_yahoo_data_{asset_yf}',
                python_callable=load_yh_wh,
                do_xcom_push=True,
                provide_context=True,
                templates_dict={
                    'asset_yf': asset_yf}
            )
        ]

        twi_ETL_done_task >> task_fetch_trans_yf >> sleep_task_yf >> task_load_yf >> yh_ETL_done_task

    for asset_bi in assets_binance:
        # 1. Get the binance finance data from PostgreSQL datalake
        task_fetch_trans_bi = [
            PythonOperator(
                task_id=f'get_binance_data_{asset_bi}',
                python_callable=fetch_trans_binance_lake,
                do_xcom_push=True,
                provide_context=True,
                templates_dict={
                    'asset_bi': asset_bi}
            )
        ]

        # 2. only to be sure the operator can have a short break and not confuses itself ;)
        sleep_task_bi = BashOperator(
            task_id=f'sleep_1s_{asset_bi}_binance',
            bash_command='sleep 1')

        # 3. Load the twitter data to PostgreSQL datawarehouse
        task_load_bi = [
            PythonOperator(
                task_id=f'load_binance_data_{asset_bi}',
                python_callable=load_bi_wh,
                do_xcom_push=True,
                provide_context=True,
                templates_dict={
                    'asset_bi': asset_bi}
            )
        ]

        yh_ETL_done_task >> task_fetch_trans_bi >> sleep_task_bi >> task_load_bi >> bi_ETL_done_task >> end_task