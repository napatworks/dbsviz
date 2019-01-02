import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
from firebase_admin import firestore
import pandas as pd
import os
import time


MAX_NUMBER_BATCH_WRITE = 500

class PriceData():
    # TODO refactor to parent class : DbWriter
    # TODO refactor to project config
    # TODO refactor app initilization

    item_cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']


    def __init__(self, as_of_date='latest'):
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
        self.path_dir = self.get_file_path(as_of_date=as_of_date)
        self.price_df = None


    def get_file_path(self, as_of_date='latest'):
        prefix = 'data/process/yahoo/price'
        filename = 'price_data.csv'
        if as_of_date != 'latest':
            return '/'.join([prefix, str(as_of_date), filename])
        else:
            as_of_max = max([x.name.split('/')[-2] for x in self.bucket.list_blobs(prefix=prefix)])
            return '/'.join([prefix, str(as_of_max), filename])


    def _get_source_file_path(self, as_of_date, prefix = False):
        if prefix:
            return 'data/process/yahoo/price/'
        else:
            return 'data/process/yahoo/price/' + str(as_of_date) + '/price_data.csv'


    def get_data(self):
        if not self.price_df:
            self.price_df = self._load_data(process=True)
        return self.price_df


    def _load_data(self, process=True):
        # TODO: Move Dateformatter to config or global variable
        # read from firebase storage (process bucket)
        file_path = self.path_dir
        blob = self.bucket.blob(file_path)
        blob.download_to_filename('tmp')
        df = pd.read_csv('tmp', sep='|')
        self._check_and_delete_old_file('tmp')
        if process:
            df['date'] = pd.DatetimeIndex(df['date']).date
            df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df.index = df['date']
            df['ticker'] = self._clean_ticker_name(df['symbol'])
        return df


    def _clean_ticker_name(self, tickers):
        return [x.lower() for x in tickers]


    def _check_and_delete_old_file(self, local_file_name):
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)


    def _verify_all_columns_exist(self, columns, verbose=False):
        cols_left = set(columns).difference(PriceData.item_cols)
        if len(cols_left) > 0 and verbose:
            print('Columns from processed documents are not in default api key:', cols_left)
        return None


    def write_to_firebase(self, df, tickers_list, cols, collection):
        jobs_list = []
        for ticker in tickers_list:
            dfs = df[df.ticker==ticker].copy()
            for col in cols:
                dfs['value'] = dfs[col]
                dic = dfs[['date', 'value']].to_dict(orient='index')
                self.db.collection(collection).document(ticker).collection('daily').document(col).set(dic)
            print('Done for ticker: ', ticker)
            print('=============================================================================')
        #jobs_list.append((self.db.collection(collection).document(ticker).collection('daily').document(col), dic))
        #self._write_batch_jobs_firebase(jobs_list)


    def _write_batch_jobs_firebase(self, jobs_list):
        # TODO move this function to generic utils
        # Currently unused due to Deadline Exceeded error
        #loop over 500 items
        N = min(len(jobs_list), MAX_NUMBER_BATCH_WRITE)
        counter=0
        while N > 0:
            t0 = time.time()
            batch = self.db.batch()
            for i in range(N):
                batch.update(jobs_list[i][0], jobs_list[i][1])
            batch.commit()
            # update jobs list
            jobs_list = jobs_list[N:]
            N = min(len(jobs_list), MAX_NUMBER_BATCH_WRITE)
            counter+=1
            print('=============================================================================')
            print('Done for job batch: ', counter)
            print('Time duration: ', time.time()-t0)
            print('Number of jobs left: ', len(jobs_list))
            print('=============================================================================')


    def run(self, collection, tickers=[]):
        df = self.get_data()
        cols = [x for x in PriceData.item_cols if x in df.columns]
        if not isinstance(tickers,list) or len(tickers)==0:
            tickers_list = df['ticker'].unique()
        else:
            tickers_list = tickers.copy()
        tickers_list = self._clean_ticker_name(tickers_list)
        print('Writing price data for ',len(tickers_list),' number of tickers')
        self.write_to_firebase(df, tickers_list, cols, collection)


def run_ingestion_job():
    dbwriter = PriceData()
    dbwriter.run(collection='stocks')

def run_test():
    dbwriter = PriceData()
    dbwriter.run(collection='test', tickers=['HMPRO', 'CPALL'])

if __name__=="__main__":
    # TODO Add test as a parameter
    #run_test()
    run_ingestion_job()