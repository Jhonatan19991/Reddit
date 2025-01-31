import os
import sys

work_dir = os.getenv("WORK_DIR")
sys.path.append(work_dir)

import pandas as pd
import json 
import time
from kafka_connection import kafka_producer
from dotenv import load_dotenv

import praw

if __name__ == "__main__":

    load_dotenv()
    CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
    CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
    USER_AGENT = os.getenv("REDDIT_USER_AGENT")


    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT
    )

    producer = kafka_producer()

    subreddit = reddit.subreddit("all")

    for post in subreddit.stream.submissions():
        payload = {
            "id": post.id,
            "title": post.title,
            "author": str(post.author),
            "subreddit": post.subreddit.display_name,
            "upvotes": post.score,
            "created_utc": post.created_utc,
            "text": post.selftext
        }
        producer.send("redditChannel", value="payload")
        time.sleep(0.1)
    
    producer.close()
    



