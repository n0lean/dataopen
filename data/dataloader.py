import pandas as pd
import os


class DataFactory(object):
    def __init__(self):
        self.dataset_name = None
        self.dataset_path = {
            'job_listing': './schema/job_listing.csv',
            'uk_employment': './schema/employment_by_occupation.csv',
            'uk_labor': './schema/labor_market_statistics.csv',
            # 'citizen_application': './schema/dataset_4.csv',
            # 'lse': './schema/dataset_5.csv',
            # 'acts': './schema/dataset_6.csv',
        }
        self.dates = []
        self.dtypes = {}

    def read_data(self):
        if self.dataset_name is None:
            raise NotImplementedError
        path = self.dataset_path[self.dataset_name]
        data = pd.read_csv(os.path.abspath(path), parse_dates=self.dates, dtype=self.dtypes)
        return data


class JobListingData(DataFactory):
    def __init__(self):
        super(JobListingData, self).__init__()
        self.dataset_name = 'job_listing'
        self.dates = [
            'created',
            'last_checked',
            'last_updated',
            'delete_date',
            'ticker_start_date',
            'ticker_end_date'
        ]
        self.dtypes = {
            'SOC_occupation_code': 'object'
        }


class UKEmploymentData(DataFactory):
    def __init__(self):
        super(UKEmploymentData, self).__init__()
        self.dataset_name = 'uk_employment'
        self.dtypes = {
            'value': 'int'
        }
        self.dates = ['year']


class UKLabor(DataFactory):
    def __init__(self):
        super(UKLabor, self).__init__()
        self.dataset_name = 'uk_labor'
        self.dates = ['Year']

    def read_data(self):
        path = self.dataset_path[self.dataset_name]
        data = pd.read_csv(os.path.abspath(path), parse_dates=self.dates, dtype=self.dtypes)
        data = data[data['Month'] != 'YEAR']
        return data
