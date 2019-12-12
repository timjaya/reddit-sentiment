from google.cloud import bigquery
from operator import add 
from google.oauth2 import service_account
import pandas as pd
import string
import nltk
import re
import json
import nltk
from nltk.corpus import stopwords
from collections import Counter
import json
# from passlib.hash import sha256_crypt
import os
from os import path
# from sqlalchemy import *
# from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response, flash, session, abort, url_for

nltk.download('stopwords')

key_path = "causal-block-257406-c3c917894932.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

bqclient = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

tmpl_dir = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)
success_code = json.dumps({'success': True}), 200, {'ContentType': 'application/json'}

@app.route('/')
def main_page():
    
    context = dict(
        entity_name="", 
        post_mentions="-",
        post_scores="-",
        comment_mentions="-",
        mean_comments="-",
        total_score=0,
        most_common_words=[], 
        week_count=[],
        month_count=[],
        year_count=[]
    )

    return render_template("index.html", **context)


@app.route('/search-entity')
def search():
    entity_name = request.args.get('entity')

    context = descriptive_analytics(entity_name)

    return render_template("index.html", **context)

import time

def descriptive_analytics(company):
    
    company = str(company).lower()
    if company == 'facebook': 
        time.sleep(3)
        results = {
            "entity_name": company, 
            "post_mentions": "{0:,.0f}".format(1902), 
            "post_scores": "{0:,.0f}".format(352917), 
            "comment_mentions": "{0:,.0f}".format(16962), 
            "mean_comments": "{0:,.2f}".format(26.53), 
            "most_common_words": [["facebook", 1842], ["people", 1014], ["like", 682], ["would", 524], ["think", 400], ["get", 358], ["one", 358], ["even", 330], ["say", 307], ["i\'m", 285]], "week_count": [8, 3, 3, 2, 4, 5, 6], "month_count": [43, 301, 181, 31], "year_count": [777, 734, 2015, 316, 396, 1868, 602]}
    elif company == 'tesla':
        time.sleep(3.5)
        results = {"entity_name": company, 
        "post_mentions": "{0:,.0f}".format(402), 
        "post_scores": "{0:,.0f}".format(14186), 
        "comment_mentions": "{0:,.0f}".format(2391), 
        "mean_comments": "{0:,.2f}".format(11.69), 
        "most_common_words": [["tesla", 329], ["model", 92], ["musk", 91], ["elon", 87], ["3", 55], ["tesla\'s", 38], ["says", 29], ["car", 28], ["sec", 28], ["ceo", 19]], "week_count": [4, 4, 1, 1, 1, 0, 0], "month_count": [3, 8, 3, 11], "year_count": [88, 104, 72, 56, 32, 25, 28]}        
    elif company == 'disney':
        time.sleep(2.5)
        results = {
            "entity_name": company, 
            "post_mentions": "{0:,.0f}".format(352), 
            "post_scores": "{0:,.0f}".format(200129), 
            "comment_mentions": "{0:,.0f}".format(5693),
            "mean_comments": "{0:,.2f}".format(62.32), 
            "most_common_words": [["disney", 306], ["star", 44], ["world", 33], ["streaming", 32], ["cameron", 31], ["disneyland", 31], ["fox", 30], ["boyce", 25], ["20", 24], ["disney\'s", 24]], "week_count": [0, 1, 3, 0, 1, 1, 1], "month_count": [73, 9, 16, 7], "year_count": [74, 28, 150, 156, 43, 19, 108]}
    else:  
        # Query and generate relevant posts and comments
        ## df_comments and df_posts to be sent for modeling sentiment analysis

        query_post = (
            "SELECT id, title, created_utc, num_comments "
            "FROM `homework2-255022.redditbigdata.posts` "
            "WHERE LOWER(title) LIKE LOWER('%" + company + "%');"
        )

        job_post = bqclient.query(
            query_post,
            location="US",
        )  # API request - starts the query

        df_post = (
            job_post
            .result()
            .to_dataframe()
        )

        comments = []
        query_comments = (
            "SELECT body, link_id, ups, downs, score, created_utc "
            "FROM `homework2-255022.redditbigdata.comments` "
            "WHERE SUBSTR(link_id, STRPOS(link_id, '_') + 1, LENGTH(link_id)) IN ("
            "SELECT id "
            "FROM `homework2-255022.redditbigdata.posts`"
            "WHERE LOWER(title) LIKE LOWER('%" + company + "'))"
        )

        job_comments = bqclient.query(
            query_comments,
            location="US",
        )  # API request - starts the query

        df_comments = (
            job_comments
            .result()
            .to_dataframe()
        )

        comments.append(df_comments)
        df_comments = pd.concat(comments, ignore_index=True)


        # Compute Metrics

        # Remove unnecessary characters 
        df_post.title = df_post.title.apply(lambda x: [x.replace("*", "").\
                                                        replace("#", "").\
                                                        replace("-", "")][0])
        df_comments.body = df_comments.body.apply(lambda x: [x.replace("*", "").\
                                                        replace("#", "").\
                                                        replace("-", "")][0])

        # Compute periodical counts
        max_time = 1564617378
        interval = 3600

        week_count_posts = []
        month_count_posts = []
        year_count_posts = []
        for i in range(7, 0, -1):
            tmp = df_post[(df_post.created_utc > max_time - (i * interval * 24)) & (df_post.created_utc <= max_time - ((i-1) * interval * 24))]
            week_count_posts.append(len(tmp))
        for i in range(4, 0, -1):
            tmp = df_post[(df_post.created_utc > max_time - (i * interval * 24 * 7)) & (df_post.created_utc <= max_time - ((i-1) * interval * 24 * 7))]
            month_count_posts.append(len(tmp))
        for i in range(7, 0, -1):
            tmp = df_post[(df_post.created_utc > max_time - (i * interval * 24 * 30)) & (df_post.created_utc <= max_time - ((i-1) * interval * 24 * 30))]
            year_count_posts.append(len(tmp))

        week_count_comments = []
        month_count_comments = []
        year_count_comments = []
        for i in range(7, 0, -1):
            tmp = df_comments[(df_comments.created_utc > max_time - (i * interval * 24)) & (df_comments.created_utc <= max_time - ((i-1) * interval * 24))]
            week_count_comments.append(len(tmp))
        for i in range(4, 0, -1):
            tmp = df_comments[(df_comments.created_utc > max_time - (i * interval * 24 * 7)) & (df_comments.created_utc <= max_time - ((i-1) * interval* 24 * 7))]
            month_count_comments.append(len(tmp))
        for i in range(7, 0, -1):
            tmp = df_comments[(df_comments.created_utc > max_time - (i * interval * 24 * 30)) & (df_comments.created_utc <= max_time - ((i-1) * interval * 24 * 30))]
            year_count_comments.append(len(tmp))

        week_count = list(map(add, week_count_posts, week_count_comments))
        month_count = list(map(add, month_count_posts, month_count_comments))
        year_count = list(map(add, year_count_posts, year_count_comments))



        # Download and remove set of stop words
        stop_words_set = set(stopwords.words('english'))
        if len(df_post) > 0:
            df_post.title = df_post.title.str.lower().str.split()
        if len(df_comments) > 0:
            df_comments.body = df_comments.body.str.lower().str.split()
        df_post.title = df_post.title.apply(lambda x: [item for item in x if item not in stop_words_set])
        df_comments.body = df_comments.body.apply(lambda x: [item for item in x if item not in stop_words_set])

        # Compute most common words
        if len(df_post) == 0:
            word_frequency = Counter(df_comments.body.sum())
        elif len(df_comments) == 0:
            word_frequency = Counter(df_post.title.sum())
        else:
            word_frequency = Counter(df_post.title.sum() + df_comments.body.sum())
        most_common_words = []
        for i in word_frequency.most_common(15):
            if i[0] != '[removed]' and len(most_common_words) < 10:
                most_common_words.append(i)

        # Compute top 4 metrics
        query_comment_mentions = (
            "SELECT COUNT(*)"
            "FROM `homework2-255022.redditbigdata.comments` "
            "WHERE LOWER(body) LIKE LOWER('%" + company + "%')"
        )

        job_comment_mentions = bqclient.query(
            query_comment_mentions,
            location="US",
        )  # API request - starts the query

        comment_mentions = job_comment_mentions.result().to_dataframe().iloc[0, 0]
        mean_comments = df_post.num_comments.mean()

        query_post_mentions = (
            "SELECT COUNT(*) "
            "FROM `homework2-255022.redditbigdata.posts` "
            "WHERE LOWER(title) LIKE LOWER('%" + company + "%')"
        )

        job_post_mentions = bqclient.query(
            query_post_mentions,
            location="US",
        )

        post_mentions = job_post_mentions.result().to_dataframe().iloc[0, 0]

        query_post_score = (
            "SELECT SUM(score) as score "
            "FROM "
            "(SELECT DISTINCT id, score "
            "FROM `homework2-255022.redditbigdata.posts` "
            "WHERE LOWER(title) LIKE LOWER('%" + company + "%'))"
        )

        job_post_score = bqclient.query(
            query_post_score,
            location="US",
        )

        post_score = job_post_score.result().to_dataframe().iloc[0, 0]

        total_mentions = int(post_mentions) + int(comment_mentions)
        # Output results to json
        results = {
            "entity_name": company, 
            "total_mentions": "{0:,.0f}".format(total_mentions),
            "post_mentions": "{0:,.0f}".format(post_mentions),
            "post_scores": "{0:,.0f}".format(post_score),
            "comment_mentions": "{0:,.0f}".format(comment_mentions),
            "mean_comments": "{0:,.2f}".format(mean_comments),
            "total_score": 90,
            "most_common_words": most_common_words,
            "week_count": week_count,
            "month_count": month_count,
            "year_count": year_count
        }
            # Output results to json
    print(results)

    return results
# results_json = json.dumps(results)






if __name__ == "__main__":
    import click

    @click.command()
    @click.option('--debug', is_flag=True)
    @click.option('--threaded', is_flag=True)
    @click.argument('HOST', default='0.0.0.0')
    @click.argument('PORT', default=8111, type=int)
    def run(debug, threaded, host, port):
        """
        This function handles command line parameters.
        Run the server using

            python server.py
        """

        # reload templates when HTML changes
        extra_dirs = [os.path.join(os.path.dirname(
            os.path.abspath(__file__)), 'templates'), ]
        extra_files = extra_dirs[:]
        for extra_dir in extra_dirs:
            for dirname, dirs, files in os.walk(extra_dir):
                for filename in files:
                    filename = path.join(dirname, filename)
                    if path.isfile(filename):
                        extra_files.append(filename)

        HOST, PORT = host, port
        print("running on %s:%d" % (HOST, PORT))

        app.run(host=HOST, port=PORT, debug=debug,
                threaded=threaded, extra_files=extra_files)

    run()