import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from decouple import config
from datetime import datetime


# Credentials Datawarehouse
dwhuname = config('DWHUNAME')
dwhpwd = config('DWHPWD')
dwhhost = config('DWHHOST')
dwhdbname = config('DWHDBNAME')
port = config('PORT')


def conn_to_datasource():
    try:
        conn = psycopg2.connect("host=" + dwhhost + " dbname=" + dwhdbname + " user=" + dwhuname + " password="
                                + dwhpwd)
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres DWH ...")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database ...")
        print(e)

    # Auto commit
    conn.set_session(autocommit=True)
    print("connected ...")
    return cur, conn


def close_conn_to_datasource(cur, conn):
    try:
        cur.close()
        conn.close()
        print("Connection closed.")
    except psycopg2.Error as e:
        print("Error: Could not close ...")
        print(e)


def load_data(table, asset, cursor):
    tbl = table
    sql_load_stmt = "SELECT date, asset, close FROM " + tbl + " WHERE asset = '" +\
                    asset + "' AND date BETWEEN '2021-01-01' AND '" + datetime.today().strftime("%Y-%m-%d") + "';"

    # fetch data in data lake
    try:
        cursor.execute(sql_load_stmt)
        result = cursor.fetchall()
    except Exception as e:
        print(f"Error with sql-statement \n"
                     f"'{sql_load_stmt}' ...")
        print(e)

    # build dataframe
    df = pd.DataFrame()
    try:
        for row in result:
            df = pd.concat([df, pd.DataFrame([{'date': row[0],
                                          'asset': row[1],
                                          'price': row[2]
                                          }])])
        df.sort_values(by='date', inplace=True)
        df = df.reset_index(drop=True)
    except Exception as e:
        print("Dataframe could not be built ...")
        print(e)

    return df


def calc_fomo_date(asset, cursor, fomo_runs):
    tbl = 'social_media'
    # sql_load_stmt = "SELECT * FROM " + tbl + " WHERE asset = '" + asset + "';"
    sql_load_stmt = "SELECT * FROM " + tbl + " WHERE asset = '" +\
                    asset + "' AND date BETWEEN '2021-01-01' AND '" + datetime.today().strftime("%Y-%m-%d") + "';"

    # fetch data in data lake
    try:
        cursor.execute(sql_load_stmt)
        result = cursor.fetchall()
    except Exception as e:
        print(f"Error with sql-statement \n"
              f"'{sql_load_stmt}' ...")
        print(e)

    # build dataframe
    df = pd.DataFrame()
    try:
        for row in result:
            df = pd.concat([df, pd.DataFrame([{'date': row[0],
                                               'asset': row[1],
                                               'count': row[2],
                                               'source': row[3]
                                               }])])
        df.sort_values(by='date', inplace=True)
        df = df.reset_index(drop=True)
    except Exception as e:
        print("Dataframe could not be built ...")
        print(e)

    # build results with top 10 dates for each year
    df['date'] = pd.to_datetime(df['date'])
    years = pd.unique(df['date'].dt.year)
    sm_res = pd.DataFrame()
    for y in years:
        sm_res = pd.concat([sm_res, df.loc[(df['source'] == 'twitter') & (df['date'].dt.year == y),
                                          :]['count'].nlargest(fomo_runs)])
    sm_2res = []
    for i in sm_res.index:
        sm_2res.append(datetime.date(df['date'][i]))

    return sm_2res


def strat_performance(dataframe, cursor, table='crypto', budget=1000, strategy='monthly', recovery=1, asset='',
                      fomo_runs=4):
    spent = 0
    cnt_asset = 0
    df = dataframe
    df_results = pd.DataFrame(columns=['date', 'asset', 'price', 'spent', 'saldo', 'n_asset'])

    if strategy == 'monthly':
        for ind in df.index:
            if df['date'][ind].day == 25:
                spent += budget
                cnt_asset += budget / df['price'][ind]
                df_results = pd.concat([df_results, df.iloc[[ind]]])
                df_results['spent'].iloc[df_results['date'] == df['date'][ind]] = spent
                df_results['n_asset'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset
                df_results['saldo'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset * df['price'][ind]

    if strategy == 'quarterly':
        q_budget = budget
        actl_month = 0
        new_mth = False
        bought = False

        for ind in df.index:
            idx = df['date'][ind]
            # buy not in the first two month of the tested period
            if ind not in [0, 1]:
                # if the recovery over the last two days is like expected -> buy order is ok
                okey = df['price'][ind] >= (1+recovery/100) * df['price'][ind-1] <= (1-recovery/300) * df['price'][ind-2]
            if idx.month != actl_month:
                # every new month, increase the budget and reset the bought-status
                q_budget += budget
                actl_month = idx.month
                new_mth = True
                bought = False
            # next line a list of days of a month which can be skipped
            lst_rng = list(range(8,32))
            # next four lines are only there to skip unnecessary month
            if idx.month == 1 and idx.year == 2021 and new_mth:
                new_mth = False
            elif idx.month in [2, 4, 5, 7, 8, 10, 11] and new_mth:
                new_mth = False
            elif new_mth and bought == False:
                if table == 'crypto' and idx.day in lst_rng:
                    new_mth = False
                elif table == 'crypto' and idx.day not in lst_rng:
                    if okey and bought == False:
                        spent += q_budget
                        cnt_asset += q_budget / df['price'][ind]
                        df_results = pd.concat([df_results, df.iloc[[ind]]])
                        df_results['spent'].iloc[df_results['date'] == df['date'][ind]] = spent
                        df_results['n_asset'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset
                        df_results['saldo'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset * df['price'][ind]
                        bought = True
                    # at the last day of the week, buy if not bought before
                    elif idx.day == 7 and bought == False:
                        spent += q_budget
                        cnt_asset += q_budget / df['price'][ind]
                        df_results = pd.concat([df_results, df.iloc[[ind]]])
                        df_results['spent'].iloc[df_results['date'] == df['date'][ind]] = spent
                        df_results['n_asset'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset
                        df_results['saldo'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset * df['price'][ind]
                        bought = True
                elif table == 'finance':
                    # if the day is between Monday and Thursday and the buy-signal ok, then buy
                    if datetime.weekday(idx) < 4 and okey and bought == False:
                        spent += q_budget
                        cnt_asset += q_budget / df['price'][ind]
                        df_results = pd.concat([df_results, df.iloc[[ind]]])
                        df_results['spent'].iloc[df_results['date'] == df['date'][ind]] = spent
                        df_results['n_asset'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset
                        df_results['saldo'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset * df['price'][ind]
                        bought = True
                    # if its Friday and still not bought -> buy
                    elif datetime.weekday(idx) == 4 and bought == False:
                        spent += q_budget
                        cnt_asset += q_budget / df['price'][ind]
                        df_results = pd.concat([df_results, df.iloc[[ind]]])
                        df_results['spent'].iloc[df_results['date'] == df['date'][ind]] = spent
                        df_results['n_asset'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset
                        df_results['saldo'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset * df['price'][ind]
                        bought = True
            if bought:
                q_budget = 0

    if strategy == 'fomo':
        # get a list of dates where the asset has its top peaks related to no. mentioning
        datelist = calc_fomo_date(asset, cursor, fomo_runs)
        f_budget = budget
        actl_month = 0
        bought = False

        for ind in df.index:
            idx = df['date'][ind]
            if idx.month != actl_month:
                f_budget += budget
                actl_month = idx.month
                bought = False
            if df['date'][ind] in datelist and bought == False:
                spent += f_budget
                cnt_asset += f_budget / df['price'][ind]
                df_results = pd.concat([df_results, df.iloc[[ind]]])
                df_results['spent'].iloc[df_results['date'] == df['date'][ind]] = spent
                df_results['n_asset'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset
                df_results['saldo'].iloc[df_results['date'] == df['date'][ind]] = cnt_asset * df['price'][ind]
                bought = True
            if bought:
                f_budget = 0

    return df_results


def load_to_dwh(df, dwhtblname):
    # load data
    try:
        conn_string = 'postgresql://' + dwhuname + ':' + dwhpwd + '@' + dwhhost + ':' + port + '/' + dwhdbname
        engine = create_engine(conn_string)
        df.to_sql(dwhtblname, conn_string, if_exists='replace', index=False)
        print("Results loaded to DWH.")
    except Exception as e:
        print("Data load failed: " + dwhtblname)
        print(e)


# open connection
cur, conn = conn_to_datasource()
df_res = pd.DataFrame(columns=['date', 'asset', 'price', 'spent', 'saldo', 'n_asset', 'strategy', 'profit'])

assets_crypto = ['bitcoin', 'ethereum', 'binance', 'ripple', 'cardano', 'solana', 'avalanche', 'polkadot',
                 'dogecoin']
assets_yf = ['gold', 'silver', 'msci_world', 'euro_stoxx', 'smi', 'nasdaq']

for asset in assets_crypto:
    table = 'crypto'
    df_data = load_data(table, asset, cur)
    for strat in ['monthly', 'quarterly', 'fomo']:
        df_step = strat_performance(df_data, strategy=strat, recovery=0.75, asset=asset, cursor=cur, fomo_runs=3)
        df_step['strategy'] = strat
        df_step['profit'] = (df_step['saldo'] / df_step['spent'] - 1) * 100
        df_res = pd.concat([df_res, df_step])

for asset in assets_yf:
    table = 'finance'
    df_data = load_data(table, asset, cur)
    for strat in ['monthly', 'quarterly', 'fomo']:
        df_step = strat_performance(df_data, strategy=strat, recovery=0.75, asset=asset, cursor=cur, fomo_runs=3)
        df_step['strategy'] = strat
        df_step['profit'] = (df_step['saldo'] / df_step['spent'] - 1) * 100
        df_res = pd.concat([df_res, df_step])

# load the results to the DWH in a separate table
cur.execute('''CREATE TABLE IF NOT EXISTS investment_results (
                date date,
                asset text,
                price float,
                spent int,
                saldo float,
                n_asset float,
                strategy text,
                profit float);
                ''')
load_to_dwh(df=df_res, dwhtblname='investment_results')
close_conn_to_datasource(cur, conn)
