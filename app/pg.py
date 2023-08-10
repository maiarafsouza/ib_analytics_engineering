#%%
import os
import logging
import pandas as pd
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

#%%
logging.basicConfig(level=logging.INFO, filename='./logs/app.log', filemode='a', format='%(asctime)s : %(module)s - %(funcName)s - %(lineno)d : %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#%%
logging.info('Loading env variables')
load_dotenv()
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

#%%

class Database(ABC):
    def __init__(*args, **kwargs):
        pass

    @abstractmethod
    def _get_connection(*args, **kwargs):
        pass
#%%
class DatabasePostgre(Database):

    def __init__(self, dialect, driver, db=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT):
        self.dialect = dialect
        self.driver = driver
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = self._get_engine()
        self.connection = self._get_connection()

    def _get_connection_string(self):
        if self.dialect == 'postgresql':
            connection_string = f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
            return connection_string
        else:
            return False

    def _get_engine(self):
        try:
            engine = create_engine(self._get_connection_string(), isolation_level="AUTOCOMMIT")
            logging.info("Engine created successfully")
            return engine
        except SQLAlchemyError as e:
            logging.info("Engine not created successfully "+e.__str__())
            return False
    
    def _get_connection(self):
        if self.engine:
            try:
                con = self.engine.raw_connection()
                logging.info("Database connected successfully")
                return con
            except SQLAlchemyError as e:
                logging.info("Database not connected successfully "+e.__str__())
                return False
        else:
            print('No engine available')

# %%
class Table():
    def __init__(self, table_name, db, schema):
        self.name = table_name
        self.db = db
        self.schema = schema

    def insert_into(self, data: pd.DataFrame, **kwargs):
        try:
            logging.info(f'''Inserting 
                         {data.dtypes} 
                         into {self.schema}.{self.name}''')
            data.to_sql(name=self.name, con=self.db.engine, schema=self.schema, if_exists='replace', index=False, **kwargs)
        except SQLAlchemyError as e:
            logging.info(e.__str__())
            return False

    def read(self):
        try:
            sql = f"select * from {self.schema}.{self.name};"
            logging.info(f"Reading {self.schema}.{self.name}")
            df = pd.read_sql(sql=sql, con=self.db.connection)
            return df
        except SQLAlchemyError as e:
            logging.info(e.__str__())
            return False

