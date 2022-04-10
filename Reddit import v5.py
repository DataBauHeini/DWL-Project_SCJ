from decouple import config  # ATTENTION, after running this line & change the *.env, rerun python (clear cache)
import praw
import pandas as pd
from datetime import datetime
import json
from pmaw import PushshiftAPI
import boto3
import os

# load credentials
CLIENT_ID = config('R_CLIENT_ID')
CLIENT_SECRET = config('R_CLIENT_SECRET')
PASSWORD = config('R_PASSWORD')
USER_AGENT = config('R_USER_AGENT')
USERNAME = config('R_USER_NAME')
ACCESS_KEY = config('AWS_ACCESS_KEY')
SECRET_KEY = config('AWS_SECRET_KEY')
SESSION_TOKEN = config('AWS_SESSION_TOKEN')

# specifying S3 credentials
s3 = boto3.resource(
    service_name='s3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN
)
# specifying reddit credentials
reddit = praw.Reddit(
    client_id = CLIENT_ID,
    client_secret = CLIENT_SECRET,
    password = PASSWORD,
    user_agent = USER_AGENT,
    username = USERNAME,
)

# choose only the common attributes of a comment
fields = ['subreddit_id', 'approved_at_utc', 'author_is_blocked', 'comment_type', 'link_title', 'mod_reason_by',
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

# until when we have to scrape
first_day = datetime(2021, 1, 1, 0, 0).timestamp()
last_day = datetime.now().timestamp()

# list of assets to look for subreddits (excl. smi, nothing found by hand in those special cases)
# assets_list = ['bitcoin', 'btc', 'eth', 'ethereum', 'binance', 'bnb', 'ripple', 'xrp', 'terra', 'luna', 'coin',
#                'cryptocur', 'cardano', 'ada', 'solana', 'sol', 'avalanche', 'avax', 'polkadot', 'dot', 'dogecoin',
#                'doge', 'msci world', 'stoxx50', 'nasdaq', 'gold', 'silver', 'swiss market index']
assets_list = ['bitcoin', 'ethereum', 'binance', 'ripple', 'terra', 'luna', 'coin',
               'cryptocur', 'cardano', 'solana', 'avalanche', 'avax', 'polkadot', 'dogecoin',
               'doge', 'msci world', 'stoxx50', 'nasdaq', 'gold', 'silver', 'swiss market index']

# search for certain subreddits
reddit_sub_lst = list()
for asset in assets_list:
    cnt = 0
    for subreddit in reddit.subreddits.search_by_name(f'{asset}'):
        cnt += 1
        reddit_sub_lst.append(subreddit.display_name)
    print(f'Added {cnt} Subreddit-Names for {asset} ...')

# get comments
api = PushshiftAPI()
limit = 100000
bucket_name = 'simonssecondbucket'

# reddit_sub_lst2 = ['bitcoin', 'ethereum', 'binance']

cnt = 0
cntr = 0
for sub in reddit_sub_lst:
    cnt += 1
    print(f'Looking for comments in Subreddit {sub} ...')
    comments = api.search_comments(subreddit=sub,
                                   limit=limit,
                                   before=1648399322,
                                   after=int(first_day),
                                   filter=fields,
                                   mem_safe=True,
                                   safe_exit=True)
    print(f'Retrieved {len(comments)} comments in Sub {sub} from Pushshift ...')
    cntr += len(comments)
    file_name = str(f'{sub}_Subreddit.csv ...')
    pd.DataFrame(comments).to_csv(file_name)
    print(f'Saved {sub} as CSV-File: {file_name} ...')
    response = s3.Bucket(bucket_name).upload_file(file_name, file_name)
    if response == None:
        print(f'Upload for {file_name} successful ...')
        os.remove(file_name)
    else:
        print(f'Upload for {file_name} not successful: {response}...')
print(f'Retrieve and Upload finished: {cnt} Files uploaded with total {cntr} Comments.')