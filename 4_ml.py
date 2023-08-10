#%%
import os
import pandas as pd
from dotenv import load_dotenv
from app.pg import DatabasePostgre, Table
from ydata_profiling import ProfileReport

#%%
load_dotenv()
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

#%%
ib = DatabasePostgre(dialect='postgresql', driver='psycopg2', db=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

coletas_silver = Table('coletas', ib, 'silver')
df_coletas = coletas_silver.read()

setup_silver = Table('setup', ib, 'silver')
df_setup = setup_silver.read()
#%%
type(df_coletas)
#%%
coletas_profile = ProfileReport(df=df_coletas, infer_dtypes=True)
# %%
coletas_profile

# %%
coletas_profile.to_file("coletas_profile.html")