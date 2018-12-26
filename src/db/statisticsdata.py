import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from firebase_admin import firestore
import pandas as pd
import os


class StatisticsData():

    item_cols = ['revenue_ttm', 'quarterly_revenue_growth_yoy', 'diluted_eps_ttm',
                 'quarterly_earnings_growth_yoy', 'net_income_ttm',
                 'operating_margin_ttm', 'profit_margin', 'trailing_annual_dividend_yield',
                 'market_cap', '52_week_high', '52_week_low',
                 'trailing_pe', 'price_per_sales_ttm', 'price_per_book',
                 'total_debt_per_equity', 'return_on_assets_ttm',
                 'return_on_equity_ttm', 'operating_cash_flow_ttm',
                 'levered_free_cash_flow_ttm']

    def __init__(self, input_date=None):
        ## TODO refactor to project config
        ## TODO add data version
        cred = credentials.Certificate('/Users/sunny/Github/dbsviz/credential/dbsweb-secret.json')
        try:
            firebase_admin.initialize_app(cred, {
                'storageBucket': 'dbsweb-f2346.appspot.com',
                'databaseURL': 'https://dbsweb-f2346.firebaseio.com',
                'projectId': 'dbsweb-f2346'
            })
        except:
            pass
        self.db = firestore.client()
        self.bucket = storage.bucket()
        self.path_dir = self._get_path_dir(prefix='data/process/yahoo/stats/', version=input_date)+'/statistics.csv'

    def _get_path_dir(self, prefix, version=None):
        if version is None or version == 'latest':
            max_version = '00000000'
            for blob in self.bucket.list_blobs(prefix=prefix):
                max_version = max(max_version, blob.name.split('/')[-2])
        else:
            max_version = version
        return prefix + max_version

    def _load_data(self):
        # read from firebase storage
        local_tmp_file = 'tmp_metadata'
        blob = self.bucket.blob(self.path_dir)
        blob.download_to_filename(local_tmp_file)
        self.table = pd.read_csv(local_tmp_file, sep='|')
        self._check_and_delete_old_file(local_tmp_file)
        return None

    def _check_and_delete_old_file(self, local_file_name):
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

    def _process_data(self):
        self.table_process = self.table.pivot(index='ticker', columns='item', values='value')
        return None

    def write_to_firebase(self, df, collection):
        # change dataframe to json
        cols = [x for x in StatisticsData.item_cols if x in df.columns]
        dic = df[cols].to_dict(orient='index')
        ## write to db
        for k, v in dic.items():
            self.db.collection(collection).document(k).set(v, merge=True)

    def run(self, collection, tickers=None):
        self._load_data()
        self._process_data()
        if isinstance(tickers,list):
            tickers = [x.lower() for x in tickers]
            self.write_to_firebase(self.table_process[self.table_process.index.isin(tickers)], collection)
        else:
            self.write_to_firebase(self.table_process, collection)

def run_ingestion_job():
    dbwriter = StatisticsData()
    dbwriter.run(collection='stocks')

def run_test():
    dbwriter = StatisticsData()
    dbwriter.run(collection='test', tickers=['cpall','hmpro','ptt'])

if __name__=="__main__":
    run_ingestion_job()
    #run_test()
