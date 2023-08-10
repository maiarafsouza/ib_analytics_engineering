#%%
import logging
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod

#%%
logging.basicConfig(level=logging.INFO, filename='./logs/app.log', filemode='a', format='%(asctime)s : %(module)s - %(funcName)s - %(lineno)d : %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#%%
class AbstractRowLevelCleaner(ABC):
    def __init__(self, data: pd.DataFrame, table, key_list, *args, **kwargs):
        self.original_data = data.copy(deep=True)
        self.data = data
        self.table = table
        self.keys = key_list
    
    @abstractmethod
    def clean_rows(self):
        pass


class BasicRowCleaner(AbstractRowLevelCleaner):
    def __init__(self, data: pd.DataFrame, table, key_list, *args, **kwargs):
        super().__init__(data, table, key_list, *args, **kwargs)
    
    def eliminate_duplicates(self):
        logging.info(f"Eliminating duplicates from {self.table}")
        self.data.drop_duplicates()
        self.data['concat'] = self.data.astype('str').values.sum(axis=1)
        self.data['length'] = self.data['concat'].apply(len)
        self.data['rank'] = self.data.groupby(self.keys)['length'].rank(method='first', ascending=False)
        self.data = self.data.query("rank == 1")
        return self.data

    def eliminate_null_keys(self):
        logging.info(f"Eliminating null keys from {self.table}")
        for key in self.keys:
            self.data = self.data[self.data[key].notnull()]
        return self.data

    def clean_rows(self):
        logging.info(f"Cleaning rows from {self.table}")
        self.data = self.eliminate_duplicates()
        self.eliminate_null_keys()
        return self.data



class AbstractColumnLevelCleaner(ABC):
    def __init__(self, data: pd.DataFrame, table, key_list, *args, **kwargs):
        self.data = data
        self.table = table
        self.keys = key_list

    
    @abstractmethod
    def clean_columns(self):
        pass

class ColetasColumnCleaner(AbstractColumnLevelCleaner):
    def __init__(self, data: pd.DataFrame, table, key_list, *args, **kwargs):
        super().__init__(data, table, key_list, *args, **kwargs)
    
    def _clean_price_lt_0(self, price_column='FinalPrice'):
        logging.info(f"Cleaning price < 0 from {self.table}")
        self.data[price_column] = self.data[price_column].apply(lambda x: x*-1 if x < 0 else x)
        return self.data
    
    def _clean_price_0(self, price_column='FinalPrice'):
        logging.info(f"Cleaning price = 0 from {self.table}")
        self.data[price_column] = self.data[price_column].apply(lambda x: np.nan if x == 0 else x)
        return self.data
    
    def clean_price_gt_10000(self, price_column='FinalPrice'):
        logging.info(f"Cleaning price > 10.000 from {self.table}")
        self.data[price_column] = self.data[price_column].apply(lambda x: np.nan if x > 10000 else x)
        return self.data

    def clean_columns(self):
        logging.info(f"Cleaning columns from {self.table}")
        self._clean_price_lt_0()
        self._clean_price_0()
        self.clean_price_gt_10000()
        return self.data