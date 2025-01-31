from sqlalchemy import Column, Integer, String, DATE, TEXT, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base
from datetime import datetime

BASE = declarative_base()
MAX_STRING_SIZE = 255
class RedditPosts(BASE):
    __tablename__ = 'RedditPosts'

    id = Column(String(MAX_STRING_SIZE), primary_key=True)
    title = Column(TEXT, nullable=False)
    author = Column(String(MAX_STRING_SIZE), nullable=False)
    subreddit = Column(String(100), nullable=False)
    upvotes =  Column(Integer, nullable=False)
    created_utc = Column(TIMESTAMP, nullable=False)
    text = Column(TEXT, nullable=False)
    processed_at = Column(TIMESTAMP, default=datetime.now)


