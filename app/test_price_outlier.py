#%%
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from app.pg import DatabasePostgre, Table
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
#%%
coletas_silver = Table('coletas', ib, 'silver')

#%%
df_coletas = coletas_silver.read()

#%%
df_coletas['FinalPrice'].value_counts()

#%%

def q25(x):
    return x.quantile(0.25)

def q75(x):
    return x.quantile(0.75)

def _median(x):
    return x.median()

def std_dev(x):
    std_dev = np.std(x)
    return std_dev

def make_list(x):
    return list(x)
#%%

iqr = df_coletas.groupby(['MasterKey_RetailerProduct']).agg({'FinalPrice':[q25, q75, _median, std_dev,make_list], 'DateIns':'count'})
# %%
iqr.columns = ['_'.join(col) for col in iqr.columns.values]
#%%
iqr.reset_index(inplace=True)
# %%
df = df_coletas.merge(iqr, on='MasterKey_RetailerProduct')

#%%
    
def detect_outlier_median(value, q25, q75, tolerance, median):
    if pd.isnull(value):
        return 0
    if value > (median * tolerance) and pd.notnull(median):
        return 1
    if value < (median * -tolerance) and pd.notnull(median):
        return -1
    else:
        return 0
    
    
def detect_outlier_iqr(value, q25, q75, tolerance, median):
    if pd.isnull(value):
        return 0
    iqr = q75 - q25
    iqr_tol = iqr * tolerance
    if value > (q75 + iqr_tol):
        return 1
    if value < (q25 - iqr_tol):
        return -1
    else:
        return 0
    
def detect_outlier_std_dev(value, std_dev, tolerance, median):
    if pd.isnull(value):
        return 0
    upper = median + (tolerance*std_dev)
    lower = median - (tolerance*std_dev)
    if value > upper:
        return 1
    if value < lower:
        return -1
    else:
        return 0

#%%
df['FinalPrice'].value_counts()
# %%
df['outlier_iqr_1_5'] = df.apply(lambda x: detect_outlier_iqr(value=x['FinalPrice'], q25=x['FinalPrice_q25'], q75=x['FinalPrice_q75'], tolerance=1.5, median=x['FinalPrice__median']), axis=1)
#%%
df['outlier_iqr_0_5'] = df.apply(lambda x: detect_outlier_iqr(value=x['FinalPrice'], q25=x['FinalPrice_q25'], q75=x['FinalPrice_q75'], tolerance=0.5, median=x['FinalPrice__median']), axis=1)
#%%
df['outlier_median_2_5'] = df.apply(lambda x: detect_outlier_median(value=x['FinalPrice'], q25=x['FinalPrice_q25'], q75=x['FinalPrice_q75'], tolerance=2.5, median=x['FinalPrice__median']), axis=1)
df['outlier_median_2_5'] = df.apply(lambda x: detect_outlier_median(value=x['FinalPrice'], q25=x['FinalPrice_q25'], q75=x['FinalPrice_q75'], tolerance=2.5, median=x['FinalPrice__median']), axis=1)
#%%
df['outlier_stddev_3_0'] = df.apply(lambda x: detect_outlier_std_dev(value=x['FinalPrice'], tolerance=3, median=x['FinalPrice__median'], std_dev=x['FinalPrice_std_dev']), axis=1)
#%%
df['outlier_stddev_2_5'] = df.apply(lambda x: detect_outlier_std_dev(value=x['FinalPrice'], tolerance=2.5, median=x['FinalPrice__median'], std_dev=x['FinalPrice_std_dev']), axis=1)

# %%
df
# %%
test_list = ['outlier_iqr_1_5', 'outlier_iqr_0_5', 'outlier_median_2_5', 'outlier_stddev_2_5', 'outlier_stddev_3_0']
#%%
for i in test_list:

    acc = df[df['FinalPrice'].notnull()].groupby(['RandomPrecosDiscrepantes', i]).agg({'MasterKey_RetailerProduct':'count'})
    acc.reset_index(inplace=True)
    tn = acc[(acc['RandomPrecosDiscrepantes'] == 0) & (acc[i] == 0)]['MasterKey_RetailerProduct'].sum()
    tp = acc[(acc['RandomPrecosDiscrepantes'] != 0) & (acc[i] != 0)]['MasterKey_RetailerProduct'].sum()
    total = acc['MasterKey_RetailerProduct'].sum()
    accuracy = (tn+tp)/total
    print(f"{i} accuracy: {accuracy}")
#%%
# outlier_iqr_1_5 accuracy: 0.9557328471397052
# outlier_iqr_0_5 accuracy: 0.9394591140541128
# outlier_median_2_5 accuracy: 0.9968840382649746
# outlier_stddev_2_5 accuracy: 0.9653225341905131
# outlier_stddev_3_0 accuracy: 0.9495116406489666
