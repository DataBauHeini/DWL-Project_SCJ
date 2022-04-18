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


def start():
    logging.info('Starting the DAG "reddit counter airflow"...')

def end():
    logging.info('Counting on reddit subs successfully finished.')


def _comment_cntr(ti, **kwargs):
    # sub = 'bitcoin'
    sub = kwargs.get('templates_dict').get('sub', None)
    days = 3
    comm_filter = ['subreddit_id', 'approved_at_utc', 'author_is_blocked', 'comment_type', 'link_title',
                   'mod_reason_by',
                   'banned_by', 'ups', 'num_reports', 'author_flair_type', 'total_awards_received', 'link_author',
                   'likes',
                   'user_reports', 'saved', 'id', 'banned_at_utc', 'mod_reason_title', 'gilded', 'archived',
                   'collapsed_reason_code', 'no_follow', 'num_comments', 'can_mod_post', 'send_replies', 'parent_id',
                   'score', 'author_fullname', 'over_18', 'report_reasons', 'removal_reason', 'approved_by',
                   'controversiality',
                   'body', 'edited', 'top_awarded_type', 'downs', 'author_flair_css_class', 'is_submitter', 'collapsed',
                   'author_flair_richtext', 'author_patreon_flair', 'body_html', 'gildings', 'collapsed_reason',
                   'distinguished', 'associated_award', 'stickied', 'author_premium', 'can_gild', 'link_id',
                   'unrepliable_reason', 'author_flair_text_color', 'score_hidden', 'permalink', 'subreddit_type',
                   'link_permalink', 'name', 'author_flair_template_id', 'subreddit_name_prefixed', 'author_flair_text',
                   'treatment_tags', 'created', 'created_utc', 'awarders', 'all_awardings', 'locked',
                   'author_flair_background_color', 'collapsed_because_crowd_control', 'mod_reports', 'quarantine',
                   'mod_note', 'link_url', '_fetched']
    limit = None
    logging.info('Starting comment scraper ...')
    api = PushshiftAPI()

    if limit == None:
        limit = 100
    logging.info(f'Limit set to {limit}.')

    for i in range((days), 0, -1):
        d = datetime.today().day
        m = datetime.today().month
        day = datetime(2022, m, d, 0, 0)
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

        if i == (days):  # because using the inverse range before
            col_names = ['date', 'sub', 'count']
            df = pd.DataFrame(columns=col_names)
            df.loc[len(df.index)] = [datetime.utcfromtimestamp(after), sub, len(comments)]
        else:
            df.loc[len(df.index)] = [datetime.utcfromtimestamp(after), sub, len(comments)]
        logging.info(f'Added row: (date: {datetime.utcfromtimestamp(after)}, sub: {sub}, count: {len(comments)}.')

    # push part
    # first serialize the df to JSON
    df_seri = df.to_json()
    logging.info(f'Variables to push - the_sub: {sub}; the_df: {df_seri}; day_delta: {days} ...')
    ti.xcom_push(key='the_sub', value=sub)
    ti.xcom_push(key=f'the_df_{sub}', value=df_seri)  # df is not seriable
    ti.xcom_push(key='day_delta', value=days)
    logging.info('Variables pushed.')


def upload_s3(**kwargs):
    logging.info('Starting upload to S3 ...')

    sub = kwargs.get('templates_dict').get('sub', None)

    # pull part
    logging.info('Pulling infos ...')
    ti = kwargs['ti']
    sub = ti.xcom_pull(task_ids=f'cnt_task_{sub}', key='the_sub')
    df = ti.xcom_pull(task_ids=f'cnt_task_{sub}', key=f'the_df_{sub}')
    days = ti.xcom_pull(task_ids=f'cnt_task_{sub}', key='day_delta')
    logging.info("Received sub: '%s'" % sub)

    # convert JSON to Pandas-df
    df2 = pd.read_json(df)

    # convert to csv
    logging.info('Converting to CSV ...')
    ts = datetime.now().date()
    file_name = str(f'{sub}_Subreddit_{days}d-before_{ts}.csv')
    df2.to_csv(file_name)
    print(f'Saved {sub} as CSV-File: {file_name}.')

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


dag = DAG(
    dag_id='reddit.cntr',
    start_date=datetime.now(),
    schedule_interval="@once")

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

assets_list = ['bitcoin', 'btc', 'eth', 'ethereum', 'binance', 'bnb', 'nasdaq', 'gold']

with dag:
    for subreddit in assets_list:
        cnt_task = [
            PythonOperator(
                task_id=f'cnt_task_{subreddit}',
                python_callable=_comment_cntr,
                provide_context=True,
                templates_dict={
                    'sub': subreddit})
        ]

        sleep_task = BashOperator(
            task_id=f'sleep_1s_{subreddit}',
            bash_command='sleep 1')

        convert_n_upload_task = [PythonOperator(
            task_id=f'conv_upl_task_{subreddit}',
            python_callable=upload_s3,
            provide_context=True,
                templates_dict={
                    'sub': subreddit})
        ]

        greet_task >> cnt_task >> sleep_task >> convert_n_upload_task >> end_task




