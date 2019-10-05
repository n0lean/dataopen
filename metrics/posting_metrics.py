import pandas as pd


class JobListingMetrics(object):
    @staticmethod
    def add_posting_range(df):
        df['time_range'] = df['delete_date'] - df['created']
        return df
