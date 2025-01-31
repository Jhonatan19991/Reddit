from dotenv import load_dotenv
import os

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

def getconnection():
    load_dotenv()
    dialect = os.getenv('PGDIALECT')
    user = os.getenv('PGUSER')
    passwd = os.getenv('PGPASSWD')
    host = os.getenv('PGHOST')
    port = os.getenv('PGPORT')
    db = os.getenv('PGDB')
    
    url = f"{dialect}://{user}:{passwd}@{host}:{port}/{db}"
    
    try:
        if not database_exists(url):
            create_database(url)
            print(f"Database created succesfully {db}")

        engine = create_engine(url)
        print(f'Conected successfully to database {db}!')
        return engine
    except SQLAlchemyError as e:
        print(f'Error: {e}')
        return None