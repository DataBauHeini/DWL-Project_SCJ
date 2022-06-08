import boto3
from decouple import config  # ATTENTION, after running this line & change the *.env, rerun python (to clear cache)
import pandas as pd
from io import BytesIO
from datetime import datetime


# loaded data
del_li_names = []

del_list = ['avax_Subreddit_2022-04-04.csv', 'doge_Subreddit_2022-04-05.csv', 'luna_Subreddit_2022-04-03.csv',
            'nasdaq_Subreddit_2022-04-05.csv', 'polkadot_Subreddit_2022-04-04.csv', 'ripple_Subreddit_2022-04-03.csv',
            'terra_Subreddit_2022-04-03.csv', 'binance_Subreddit_2022-04-03.csv', 'solana_Subreddit_2022-04-04.csv',
            'gold_Subreddit_2022-04-05.csv', 'cryptocurrency_Subreddit_2022-04-03.csv',
            'bitcoin_Subreddit_2022-04-03.csv', 'ethereum_Subreddit_2022-04-03.csv', 'cardano_Subreddit_2022-04-03.csv',
            'bitcoin_Subreddit_2022-05-18.csv', 'ethereum_Subreddit_2022-05-18.csv', 'binance_Subreddit_2022-05-18.csv',
            'ripple_Subreddit_2022-05-18.csv', 'luna_Subreddit_2022-05-18.csv', 'cardano_Subreddit_2022-05-18.csv',
            'solana_Subreddit_2022-05-18.csv', 'avax_Subreddit_2022-05-18.csv', 'polkadot_Subreddit_2022-05-18.csv',
            'doge_Subreddit_2022-05-18.csv', 'nasdaq_Subreddit_2022-05-18.csv', 'gold_Subreddit_2022-05-18.csv',
            'solana_Subreddit_2022-06-01.csv', 'ripple_Subreddit_2022-06-01.csv', 'gold_Subreddit_2022-06-01.csv',
            'ethereum_Subreddit_2022-06-01.csv', 'avax_Subreddit_2022-06-01.csv', 'cardano_Subreddit_2022-06-01.csv',
            'cryptocurrency_Subreddit_2022-06-01.csv', 'binance_Subreddit_2022-06-01.csv',
            'bitcoin_Subreddit_2022-06-01.csv', 'doge_Subreddit_2022-06-01.csv', 'polkadot_Subreddit_2022-06-01.csv',
            'luna_Subreddit_2022-06-01.csv']


# load credentials
# AWS
ACCESS_KEY = config('aws_access_key_id')
SECRET_KEY = config('aws_secret_access_key')
SESSION_TOKEN = config('aws_session_token')

# specifying S3 credentials
s3 = boto3.resource(
    service_name='s3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN
)

# Credentials Datawarehouse
dwhuname = config('DWHUNAME')
dwhpwd = config('DWHPWD')
dwhhost = config('DWHHOST')
dwhdbname = config('DWHDBNAME')

bucket_name = 'teambucketscj'

my_bucket = s3.Bucket(bucket_name)


# get the list of files
def file_list(s3_bucket):
    bucket_list = []
    name_list = []
    for file in s3_bucket.objects.all():
        file_name = file.key
        if file_name.find(".csv") != -1:
            bucket_list.append(file.key)
            name_list.append(file_name.partition('_')[0])
    # length_bucket_list = print(len(bucket_list))
    print(bucket_list)
    # return only the values, which are not loaded
    bucket_list = list(set(bucket_list).symmetric_difference(set(del_list)))
    name_list = list(set(name_list).symmetric_difference(set(del_li_names)))
    return bucket_list, name_list


# create a df out of the csv's
def df_from_csv(list_of_bucket, list_of_names):
    df_list = []  # Initializing empty list of dataframes
    for i, file in enumerate(list_of_bucket):
        obj = s3.Object(bucket_name, file)
        print(f'Get data from {file} ...')
        data = obj.get()['Body'].read()
        print(f'{file} received.')
        locals()[list_of_names[i]] = pd.read_csv(BytesIO(data), delimiter=",", index_col=False,
                                                 usecols=['id', 'created_utc'], low_memory=False)
        print(f'df {file} created ...')
        df_list.append(locals()[list_of_names[i]])

    print('all files added to the List ...')
    return df_list


# count and add everything to a df
def cnt_addtodf(list_of_df, list_of_names):
    nam = list_of_names
    print("creating a new df for the results ...")
    df_result = pd.DataFrame()
    for x, df in enumerate(list_of_df):
        print(f'working on {nam[x]} ...')
        for i in range(len(df['created_utc'])):
            df.loc[i, 'date'] = datetime.utcfromtimestamp(df['created_utc'].loc[i]).date()
        print(f'converted date from {nam[x]} ...')
        df_step = pd.DataFrame(df.groupby('date').size())
        print(f'did the group by and count for {nam[x]},\nrenaming the columns ...')
        df_step.loc[:, 'asset'] = nam[x]
        df_step = df_step.rename({0:'count'}, axis=1)
        print(f'renaming done.\nAdd name of the source now ...')
        df_step = df_step.assign(source='reddit')
        print(f'added column with source-name for {nam[x]}\nConcatenating the df now ...')
        frames = [df_result, df_step]
        df_result = pd.concat(frames)
        print(f'Concatenating for {nam[x]} successfully done ...')

    # df_result['plattform'] = 'reddit'
    df_result.reset_index(inplace=True)
    df_result = df_result.rename(columns={'index':'date'})
    df_result = df_result[['date', 'asset', 'count', 'source']]
    return df_result


def load_to_dwh(result):
    # load data to data warehouse
    # define loading variables
    dwhtblname = 'social_media'
    if_ex_val = 'append'

    try:
        conn_string = 'postgresql://' + dwhuname + ':' + dwhpwd + '@' + dwhhost + ':5432/' + dwhdbname
        # engine = create_engine(conn_string)
        result.to_sql(dwhtblname, conn_string, if_exists=if_ex_val, index=False)
        print("Reddit data loaded.")
    except Exception as e:
        print(e)
        print("Data load failed!")


# def main():
#     try:
        listli, li_names = file_list(my_bucket)
        # check and correct lists before running the next steps!!!!!
        dataframes = df_from_csv(listli, li_names)
        res = cnt_addtodf(dataframes, li_names)
        load_to_dwh(res)
        print("Everything went fine!")
    # except Exception as e:
    #     print("something went wrong...")
    #     print(e)

# if __name__ == "__main__":
#     main()


