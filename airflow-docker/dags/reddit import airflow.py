import logging

import botocore.exceptions
# from decouple import config  # ATTENTION, after running this line & change the *.env, rerun python (clear cache)
import praw
import pandas
from datetime import datetime, timedelta
import json
from pmaw import PushshiftAPI
import boto3
import os

from airflow import DAG
from airflow.models.xcom import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

def start():
    logging.info('Starting the DAG ...')

def comment_scraper(ti):
    sub = 'bitcoin'
    before = int(datetime.now().timestamp())
    after = int((datetime.now() - timedelta(days=3)).timestamp())
    filter = ['subreddit_id', 'approved_at_utc', 'author_is_blocked', 'comment_type', 'link_title', 'mod_reason_by',
          'banned_by', 'ups', 'num_reports', 'author_flair_type', 'total_awards_received', 'link_author', 'likes',
          'user_reports', 'saved', 'id', 'banned_at_utc', 'mod_reason_title', 'gilded', 'archived',
          'collapsed_reason_code', 'no_follow', 'num_comments', 'can_mod_post', 'send_replies', 'parent_id',
          'score', 'author_fullname', 'over_18', 'report_reasons', 'removal_reason', 'approved_by', 'controversiality',
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
    logging.info(f'Looking for comments in Subreddit {sub} between {datetime.utcfromtimestamp(after)} and '
                 f'{datetime.utcfromtimestamp(before)} ...')
    comments = api.search_comments(subreddit=sub,
                                   # before=before,
                                   # after=after,
                                   limit=limit,
                                   filter=filter,
                                   # mem_safe=True,
                                   # safe_exit=True,
                                   )
    logging.info(f'Retrieved {len(comments)} comments in Sub {sub} from Pushshift.')

    logging.info(os.listdir())
    logging.info('Starting comment to *.csv converter ...')
    ts = datetime.now().date()
    file_name = str(f'{sub}_Subreddit_{ts}.csv')
    logging.info(f'Saving it as {file_name} ...')
    logging.info(f'Working directory: {Variable.get("scrape_dag_csv_path")}')
    csv_path = Variable.get('scrape_dag_csv_path')
    pandas.DataFrame(comments).to_csv(csv_path+file_name)
    logging.info(f'Saved {sub} as CSV-File: {file_name}.')

    # push part
    logging.info(f'Variables to push: sub - {sub}; file_name - {file_name} ...')
    ti.xcom_push(key="the_sub", value=sub)
    ti.xcom_push(key="the_file_name", value=file_name)
    logging.info('Comment to *.csv converter finished.')

def upload_s3(**kwargs):
    logging.info('Starting upload to S3 ...')
    # pull part
    ti = kwargs['ti']
    sub = ti.xcom_pull(task_ids='commet_scrape', key='the_sub')
    file = ti.xcom_pull(task_ids='comment_scrape', key='the_file_name')
    logging.info("Received sub: '%s'" % sub)
    logging.info("Received file_name: '%s'" % file)

    bucket = Variable.get('bucket')
    response = s3.Bucket(bucket).upload_file(file, file)
    if response == None:
        logging.info(f'Upload for {file} successful ...')
        os.remove(file)
    else:
        logging.info(f'Upload for {file} not successful: {response}...')
    logging.info('Uploading to S3 finished ...')


dag = DAG(
    dag_id='reddit.scraper',
    start_date = datetime.now(),
    schedule_interval="@once")

greet_task = PythonOperator(
   task_id="start_task",
   python_callable=start,
   dag=dag
)

scrape_task = PythonOperator(
    task_id='scrape_task',
    python_callable=comment_scraper,
    provide_context=True,
    dag=dag)

upload_s3_task = PythonOperator(
    task_id='upload_s3_task',
    python_callable=upload_s3,
    provide_context=True,
    dag=dag
)



greet_task >> scrape_task >> upload_s3_task