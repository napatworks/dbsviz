import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from firebase_admin import firestore
import pandas as pd
import os


class FinancialData():

    item_cols = ['date', 'revenues', 'expenses', 'gross_profit',
                 'operating_profit', 'ebit', 'ebitda', 'net_income', 'cash',
                 'account_receivable', 'inventory', 'current_asset',
                 'property_plant_equipment', 'total_asset', 'short_term_loan',
                 'account_payable', 'current_portion_long_term_debt',
                 'current_liability', 'non_current_liability', 'total_liability',
                 'paid_in_capital', 'total_equity', 'cashflow_operation',
                 'cashflow_investment', 'cashflow_financing', 'depreciation',
                 'pe', 'pbv', 'de',
                 'roa', 'roaa', 'roe', 'roae',
                 'roce', 'npm', 'npma', 'currentratio', 'quickratio',
                 'dividend', 'number_of_shares', 'eps', 'dps', 'eps_cum', 'dps_cum',
                 'close_price']

    def __init__(self):
        # TODO refactor to project config
        # TODO add data version
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
        self.path_dir = 'data/process/siamchart/financial/20181216/'


    def write_to_firebase(self, df, ticker, collection):
        cols = [x for x in FinancialData.item_cols if x in df.columns]
        dic = df[cols].to_dict(orient='index')
        for k, v in dic.items():
            self.db.collection(collection).document(ticker).collection('quarterly').document(k).set(v)
        print('Done for ticker: ', ticker, ', Number of items: ', len(cols), 'Number of quarters: ', len(df))
        print('===========================')

    def get_data(self, ticker, process=True):
        return self._load_data(ticker, process)

    def _load_data(self, ticker, process=True):
        # TODO: Move Dateformatter to config or global variable
        # read from firebase storage (process bucket)
        file_path = self.path_dir + str(ticker).lower() + '.csv'
        blob = self.bucket.blob(file_path)
        blob.download_to_filename('tmp')
        df = pd.read_csv('tmp', sep='|')
        self._check_and_delete_old_file('tmp')
        if process:
            df['date'] = pd.DatetimeIndex(df['date']).date
            df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df.index = df['date']
        return df


    def _check_and_delete_old_file(self, local_file_name):
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)


    def _verify_all_columns_exist(self, columns, verbose):
        cols_left = set(columns).difference(FinancialData.item_cols)
        if len(cols_left) > 0 and verbose:
            print('Columns from processed documents are not in default api key:', cols_left)
        return None


    def _load_and_write(self, ticker, collection):
        ticker = str(ticker).lower()
        df = self._load_data(ticker, process=True)
        self._verify_all_columns_exist(df.columns, verbose=False)
        self.write_to_firebase(df, ticker, collection)


    def run(self, collection):
        for blob in self.bucket.list_blobs(prefix=self.path_dir):
            filename = blob.name.split('/')[-1]
            ticker = filename.split('.')[0]
            self._load_and_write(ticker, collection)


def run_ingestion_job():
    dbwriter = FinancialData()
    dbwriter.run(collection='stocks')


def run_test():
    dbwriter = FinancialData()
    dbwriter._load_and_write(ticker="HMPRO", collection='test')
    dbwriter._load_and_write(ticker="CPALL", collection='test')


if __name__=="__main__":
    # TODO Add test as a parameter
    #run_test()
    run_ingestion_job()