#%%
import pandas as pd
import numpy as np
import logging

# %%
logging.basicConfig(level=logging.INFO, filename='./logs/app.log', filemode='a', format='%(asctime)s : %(module)s - %(funcName)s - %(lineno)d : %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#%%

def median_(x):
    return x.median()

def std_dev(x):
    std_dev = np.std(x)
    return std_dev

def make_list(x):
    return list(x)
#%%
def aggregate_df(df_coletas):
    logging.info(f'Aggregating {df_coletas.columns}')
    df_agg = df_coletas.groupby(['MasterKey_RetailerProduct']).agg({'FinalPrice':[median_, std_dev, make_list], 'DateIns':'count'})
    df_agg.columns = ['_'.join(col) for col in df_agg.columns.values]
    df_agg.reset_index(inplace=True)
    df_a = df_coletas.merge(df_agg, on='MasterKey_RetailerProduct')
    return df_a
#%%
    
def detect_outlier_median(value, tolerance, median):
    if pd.isnull(value):
        return 0
    if value > (median * tolerance) and pd.notnull(median):
        return 1
    if value < (median * -tolerance) and pd.notnull(median):
        return -1
    else:
        return 0


#%%
def flag_outliers(df):
    logging.info(f'Flagging price outliers on {df.columns}')
    
    df['outlier_price'] = df.apply(lambda x: detect_outlier_median(
        value=x['FinalPrice'], 
        tolerance=2.5, 
        median=x['FinalPrice_median_']), axis=1)
    return df

#%%
def test_accuracy(df):
    test_list = ['outlier_price']
    for i in test_list:

        acc = df[df['FinalPrice'].notnull()].groupby(['RandomPrecosDiscrepantes', i]).agg({'MasterKey_RetailerProduct':'count'})
        acc.reset_index(inplace=True)
        tn = acc[(acc['RandomPrecosDiscrepantes'] == 0) & (acc[i] == 0)]['MasterKey_RetailerProduct'].sum()
        tp = acc[(acc['RandomPrecosDiscrepantes'] != 0) & (acc[i] != 0)]['MasterKey_RetailerProduct'].sum()
        total = acc['MasterKey_RetailerProduct'].sum()
        accuracy = (tn+tp)/total
        logging.info(f"{i} accuracy: {accuracy}")

#%%
def remove_outliers(df):
    logging.info(f'Removing price outliers from {df.columns}')
    df['FinalPrice'] = df.apply(lambda x: np.nan if x['outlier_price'] == 1 else x['FinalPrice'], axis=1)
    return df
# %%

def test_outliers(df):

    logging.info(f'Starting outlier test with input {df.columns}')

    df_a = aggregate_df(df)
    logging.info(f'Aggregated df resulted in {df_a.columns}')

    df_b = flag_outliers(df_a)
    logging.info(f'Flagged df resulted in {df_b.columns}')

    logging.info(f'Testing accuracy')
    test_accuracy(df_b)

    logging.info(f'Finishing outlier test with output {df_b.columns}')
    return df_b
