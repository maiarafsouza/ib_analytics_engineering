#%%
import os
import pandas as pd
from app.pg import DatabasePostgre, Table
#%%


#%%
# Database instantiation
ib = DatabasePostgre(dialect='postgresql', driver='psycopg2')
# %%
# Process coletas raw
# Table instantiation
coletas = Table('coletas', ib, 'bronze')
# Get raw
coletas_csv = pd.read_csv('input_data/Coletas.csv', dtype={'MasterKey_RetailerProduct':'str'})
# Load
coletas.insert_into(coletas_csv)
# Validation
df_coletas = coletas.read()
df_coletas

# Process setup raw
# Table instantiation
setup = Table('setup', ib, 'bronze')
# Get raw
setup_csv = pd.read_csv('input_data/ProdutosVarejos.csv', dtype={'MasterKey_RetailerProduct':'str'})
# Load
setup.insert_into(setup_csv)
# Validation
df_setup = setup.read()
df_setup

# %%
ib.connection.close()

