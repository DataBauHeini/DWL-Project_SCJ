import logging
from datetime import datetime, timedelta
from pmaw import PushshiftAPI
import pandas as pd
import boto3

from airflow import DAG
from airflow.models.xcom import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable


# define all necessary functions
def start():
    logging.info('Starting the DAG "reddit counter airflow"...')


def end():
    logging.info('Counting on reddit subs successfully finished.')


def _comment_cntr(ti, **kwargs):
    sub = kwargs.get('templates_dict').get('sub', None)     # get subreddit name
    days = 3
    comm_filter = ['id', 'created']                         # attributes of comments to get
    limit = None                                            # No. of comments
    logging.info('Starting comment scraper ...')
    api = PushshiftAPI()

    # if limit == None:                                     # only for testing
        # limit = 100
    logging.info(f'Limit set to {limit}.')

    for i in range((days), 0, -1):                          # reversed range
        # setting parameters for searching comments
        d = datetime.today().day
        m = datetime.today().month
        y = datetime.today().year
        day = datetime(y, m, d, 0, 0)
        before = int((day - timedelta(days=i)).timestamp())
        after = int((day - timedelta(days=i + 1)).timestamp())
        logging.info(f'Looking for comments in Subreddit {sub} at {datetime.utcfromtimestamp(after)} ...')
        comments = api.search_comments(subreddit=sub,
                                       before=before,
                                       after=after,
                                       limit=limit,
                                       filter=comm_filter,
                                       # mem_safe=True,
                                       # safe_exit=True,
                                       )
        logging.info(f'Retrieved {len(comments)} comments in Sub {sub} from Pushshift.')

        # if it is the first run of the loop, create a dataframe
        if i == days:                                     # because the inverse range was used before
            col_names = ['date', 'sub', 'count']
            df = pd.DataFrame(columns=col_names)

        df.loc[len(df.index)] = [datetime.utcfromtimestamp(after), sub, len(comments)]
        logging.info(f'Added row: (date: {datetime.utcfromtimestamp(after)}, sub: {sub}, count: {len(comments)}.')

    # push information part
    # first serialize the df to JSON, otherwise it is not possible to push a dataframe
    df_seri = df.to_json()
    logging.info(f'Variables to push - the_sub: {sub}; the_df: {df_seri}; day_delta: {days} ...')
    ti.xcom_push(key='the_sub', value=sub)
    ti.xcom_push(key=f'the_df_{sub}', value=df_seri)
    ti.xcom_push(key='day_delta', value=days)
    logging.info('Variables pushed.')


def _upload_s3(**kwargs):
    logging.info('Starting upload to S3 ...')

    # get the subreddit name out of the DAG
    sub = kwargs.get('templates_dict').get('sub', None)

    # pull information part
    logging.info('Pulling infos ...')
    ti = kwargs['ti']
    sub = ti.xcom_pull(task_ids=f'cnt_task_{sub}', key='the_sub')
    df = ti.xcom_pull(task_ids=f'cnt_task_{sub}', key=f'the_df_{sub}')
    days = ti.xcom_pull(task_ids=f'cnt_task_{sub}', key='day_delta')
    logging.info("Received sub: '%s'" % sub)

    # convert JSON to Pandas_df
    df2 = pd.read_json(df)

    # convert to csv (the file is safed in a temporary environment inside the Airflow/Docker-environment)
    logging.info('Converting to CSV ...')
    ts = datetime.now().date()
    file_name = str(f'{sub}_Subreddit_{days}d-before_{ts}.csv')
    df2.to_csv(file_name)
    logging.info(f'Saved {sub} as CSV-File: {file_name}.')

    # upload to S3-Bucket
    s3 = boto3.resource(
        service_name='s3',
        aws_access_key_id=Variable.get('ACCESS_KEY'),
        aws_secret_access_key=Variable.get('SECRET_KEY'),
        aws_session_token=Variable.get('SESSION_TOKEN')
    )
    bucket = Variable.get('bucket')
    response = s3.Bucket(bucket).upload_file(file_name, file_name)
    if response == None:
        logging.info(f'Upload for {file_name} successful ...')
    else:
        logging.info(f'Upload for {file_name} not successful: {response} ...')
    logging.info('Uploading to S3 finished.')


# specify the general DAG
dag = DAG(
    dag_id='reddit.cntr',
    start_date=datetime(2022, 4, 1),
    schedule_interval=timedelta(days=3))

# specify tasks which are running only once
greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=end,
    dag=dag
)

# comparing to Reddit_HistoricalData, the list is shortened, because we focus only on the main assets in this stage
assets_list = ['bitcoin', 'btc', 'eth', 'ethereum', 'binance', 'bnb', 'nasdaq', 'gold']

with dag:
    # with this for loop, the defined tasks here are executed parallel for each asset in the list before
    for subreddit in assets_list:
        cnt_task = [
            PythonOperator(
                task_id=f'cnt_task_{subreddit}',
                python_callable=_comment_cntr,
                provide_context=True,
                templates_dict={
                    'sub': subreddit})
        ]

        # only to be sure the operator can have a short break and not confuses itself ;)
        sleep_task = BashOperator(
            task_id=f'sleep_1s_{subreddit}',
            bash_command='sleep 1')

        convert_n_upload_task = [PythonOperator(
            task_id=f'conv_upl_task_{subreddit}',
            python_callable=_upload_s3,
            provide_context=True,
                templates_dict={
                    'sub': subreddit})
        ]

        greet_task >> cnt_task >> sleep_task >> convert_n_upload_task >> end_task
