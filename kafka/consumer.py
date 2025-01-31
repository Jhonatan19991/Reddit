import sys
import os
from dotenv import load_dotenv

work_dir = os.getenv("WORK_DIR")
sys.path.append(work_dir)

# database connections
from src.models.models import RedditPosts
from src.database.dbconnection import getconnection
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


# pandas
import pandas as pd

# Spark Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Dont run in production server
if __name__ == '__main__':
    load_dotenv()
    dialect = os.getenv('PGDIALECT')
    user = os.getenv('PGUSER')
    passwd = os.getenv('PGPASSWD')
    host = os.getenv('PGHOST')
    port = os.getenv('PGPORT')
    db = os.getenv('PGDB')

    engine = getconnection()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        if inspect(engine).has_table('RedditPosts'):
            RedditPosts.__table__.drop(engine)
        RedditPosts.__table__.create(engine)
        print("Table created successfully.")
    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")
    
    spark = SparkSession.builder.appName("RedditProcessor").config("spark.sql.shuffle.partitions", "2").getOrCreate()

    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redditChannel") \
    .load()

    processed_df = df.select(
        from_json(col("value").cast("string"), 
        """
            id STRING,
            title STRING,
            author STRING,
            subreddit STRING,
            upvotes INT,
            created_utc DOUBLE,
            text STRING
        """).alias("data")
    ).select("data.*")

    # Filter just posts about AI
    filtered_df = processed_df.filter(
        lower(col("title")).like("%artificial intelligence%") | 
        lower(col("text")).like("%machine learning%")
    )

    url = f"{dialect}://{user}:{passwd}@{host}:{port}/{db}"

    def write_to_postgres(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "RedditPosts") \
            .option("user", user) \
            .option("password", passwd) \
            .mode("append") \
            .save()
        
    # Save into postgresql   
    filtered_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start() \
    .awaitTermination()

        