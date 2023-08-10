#%%
import logging
from app.pg import DatabasePostgre, Table
from app.price_outlier import test_outliers, remove_outliers
from app.bronze_to_silver import BasicRowCleaner, ColetasColumnCleaner
from sqlalchemy import DATE
#%%
logging.basicConfig(level=logging.INFO, filename='logs/app.log', filemode='a', format='%(asctime)s : %(module)s - %(funcName)s - %(lineno)d : %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
#%%
SCHEMA_SILVER_SETUP = ['Customer', 'Department', 'Category', 'Brand', 'EAN', 'Product', 'Retailer', 'MasterKey_RetailerProduct']
SCHEMA_SILVER_COLETAS = ['DateIns', 'Screenshot', 'Available', 'Unavailable', 'SuggestedPrice', 'FinalPrice', 'MasterKey_RetailerProduct', 'RandomPrecosNegativos', 'RandomPrecosMissing', 'RandomPrecosDiscrepantes', 'RandomPrecosDiscrepantesFator']
#%%
ib = DatabasePostgre(dialect='postgresql', driver='psycopg2')

#%%
# Clean coletas funcs

def _non_na_count(df):
    return df['FinalPrice'].count()

def _total_rows(df):
    return df.shape[0]

def clean_coletas(df):
    logging.info('Cleaning coletas bronze')
    coletas_row_cleaner = BasicRowCleaner(df, 'coletas', ['DateIns', 'MasterKey_RetailerProduct'])
    df_coletas_clean = coletas_row_cleaner.clean_rows()
    coletas_column_cleaner = ColetasColumnCleaner(df_coletas_clean, 'coletas', ['DateIns', 'MasterKey_RetailerProduct'])
    df_coletas_clean = coletas_column_cleaner.clean_columns()
    return df_coletas_clean

def handle_outliers(df):
    logging.info('Handling outliers')
    df_flagged = test_outliers(df)
    df_b = remove_outliers(df_flagged)
    return df_b

def load_coletas(df, table):
    table.insert_into(df[SCHEMA_SILVER_COLETAS], dtype={'DateIns':DATE})

# %%
# Coletas processing

logging.info('Processing coletas')

coletas_bronze = Table('coletas', ib, 'bronze')
coletas_silver = Table('coletas', ib, 'silver')
df_coletas = coletas_bronze.read()

logging.info(f'''Before processing:
             Total rows: {_total_rows(df_coletas)}
             Non NA rows: {_non_na_count(df_coletas)}''')

df = clean_coletas(df=df_coletas)

df_a = handle_outliers(df)

# SuggestPrice * 10 is unreliable reference method because suggested prices even when configured aren't sane
df_a[(df_a['FinalPrice'] > (df_a['SuggestedPrice']*10)) & (df_a['SuggestedPrice'] > 1)]


df_coletas = df_a

logging.info(f'''After processing:
             Total rows: {_total_rows(df_coletas)}
             Non NA rows: {_non_na_count(df_coletas)}''')

# Coletas loading

load_coletas(df_coletas, coletas_silver)



# %%
# Clean setup funcs

logging.info('Processing setup')

def clean_setup(df):
    setup_row_cleaner = BasicRowCleaner(df, 'setup', ['MasterKey_RetailerProduct'])
    df_setup_clean = setup_row_cleaner.clean_rows()
    return df_setup_clean

def load_setup(df, table):
    table.insert_into(df[SCHEMA_SILVER_SETUP])

# Setup processing
setup_bronze = Table('setup', ib, 'bronze')
setup_silver = Table('setup', ib, 'silver')
df_setup = setup_bronze.read()

# Setup loading
load_setup(clean_setup(df_setup), setup_bronze)
