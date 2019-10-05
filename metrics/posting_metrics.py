import pandas as pd
import sys
sys.path.append('./')
from data.dataloader import *

class JobListingMetrics(object):
    @staticmethod
    def add_posting_range(df):
        df['time_range'] = df['delete_date'] - df['created']
        return df
    
    @staticmethod
    def get_count(df, col_name, freq = "W"):
        df['created_day'] = df['created'].dt.date
        df['deleted_day'] = df['delete_date'].dt.date
        release = df.groupby(col_name+['created_day'])['created'].count().reset_index().rename(columns={'created':'num_created','created_day':'date'})
        delete = df.groupby(col_name+['deleted_day'])['delete_date'].count().reset_index().rename(columns={'delete_date':'num_deleted','deleted_day':'date'})
        total = pd.merge(release, delete, on=col_name+['date'], how = "outer").fillna(0).sort_values('date')
        total['date'] = pd.to_datetime(total.date)
        total["change"] = total['num_created'] - total['num_deleted']
        total['num_list'] = total.groupby(col_name)['change'].cumsum()
        total.set_index('date',inplace=True)
        res = total.groupby(col_name)['num_list'].resample(freq).pad().reset_index()
        return res

if __name__=='__main__':
    loader = JobListingData()
    df = loader.read_data()
    metrics = JobListingMetrics()
    print(metrics.get_count(df,'company_name'))
    print('Done')


#%%
