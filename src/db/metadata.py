import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from firebase_admin import firestore
import pandas as pd
import os


class MetaData():

    item_cols = ['ticker', 'company', 'market', 'industry', 'country']

    def __init__(self):
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
        self.path_dir = 'data/process/set_website/metadata/20181202/metadata.csv'

    def _load_data(self):
        # read from firebase storage
        blob = self.bucket.blob(self.path_dir)
        blob.download_to_filename('tmp_metadata')
        self.table = pd.read_csv('tmp_metadata', sep='|')
        self._check_and_delete_old_file('tmp_metadata')

    def _check_and_delete_old_file(self, local_file_name):
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

    def _process_data(self):
        # change columns
        try:
            self.table.rename(columns={'symbol': 'ticker'}, inplace=True)
            self.table['market'] = self.table['market'].apply(lambda x: x.lower())
            self.table['country'] = 'thai'
            self.table.index = self.table['ticker'].apply(lambda x: x.lower())
        except:
            raise

    def write_to_firebase(self, df, collection, tickers=None):
        # change dataframe to json
        cols = [x for x in MetaData.item_cols if x in df.columns]
        dic = df[cols].to_dict(orient='index')
        ## write to db
        for k, v in dic.items():
            self.db.collection(collection).document(k).set(v, merge=True)

    def run(self, collection, tickers=None):
        self._load_data()
        self._process_data()
        if isinstance(tickers,list):
            tickers = [x.lower() for x in tickers]
            self.write_to_firebase(self.table[self.table.index.isin(tickers)], collection)
        else:
            self.write_to_firebase(self.table, collection)

def run_ingestion_job():
    dbwriter = MetaData()
    dbwriter.run(collection='stocks')

def run_test():
    dbwriter = MetaData()
    dbwriter.run(collection='test', tickers=['cpall','hmpro','ptt'])

if __name__=="__main__":
    #run_ingestion_job()
    run_test()
