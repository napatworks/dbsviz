# Firebase
from firebase_admin import credentials
from firebase_admin import storage
import firebase_admin

import pandas as pd
import datetime as dt
from io import StringIO

import os

class YahooStatsScraper:
    #TODO: refactor code to more robust to testing and changing (eg. remove hardcode date=today)
    #TODO: refactor code to decompose writing to firebase job and scraper job
    #TODO: refactor raw scraping, move processing stuff to process
    """
    Reading data from YAHOO Finance overview statistic page
    """
    def __init__(self):
        return None

    def get_yahoostats_url(self, ticker):
        return 'https://finance.yahoo.com/quote/'+ticker.upper()+'.BK/key-statistics'

    def get_stats_table(self, ticker):
        tb = pd.read_html(self.get_yahoostats_url(ticker))
        return pd.concat(tb, ignore_index=True)

def check_and_delete_old_file(local_file_name):
    if os.path.isfile(local_file_name):
        os.remove(local_file_name)

def run_scraping_job(ticker_list = None, output_path = None):
    # TODO change metadata, get_ticker to latest, change to metadata class
    # TODO refactor code to utils

    # Initializing app
    firebase_credential_path = "../../credential/dbsweb-secret.json"
    if (not len(firebase_admin._apps)):
        cred = credentials.Certificate(firebase_credential_path)
        firebase_admin.initialize_app(cred, {
            "storageBucket": "dbsweb-f2346.appspot.com"
        })
    bucket = storage.bucket()

    ## set ticker and
    if ticker_list is None:
        blob = bucket.blob('data/process/set_website/metadata/20181220/metadata.csv')
        meta_table_str = blob.download_as_string().decode('utf-8')
        df_meta = pd.read_csv(StringIO(meta_table_str), sep='|')
        ticker_list = list(df_meta['symbol'].unique())

    # TODO Change dateformatter to config file
    if output_path is None:
        today_str = dt.date.today().strftime(format='%Y%m%d')
        output_path = 'data/raw/yahoo/stats/' + today_str + '/'


    yhss = YahooStatsScraper()
    tmp_filename = "tmp_yahoo_stats.csv"
    for tick in ticker_list:
        ticker = tick.lower()
        print(ticker, ': Loading data from yahoo')
        try:
            df = yhss.get_stats_table(ticker)
            file_path = output_path+ticker+'.csv'
            #write to firebase storage
            df.to_csv(tmp_filename, sep="|", index=False)
            blob = bucket.blob(file_path)
            blob.upload_from_filename(tmp_filename)
            print(ticker, ': Successfully upload file to cloud storage')
        except:
            #TODO: beware of quite error
            print(ticker, 'Error loading ticker')
    check_and_delete_old_file(tmp_filename)

if __name__=='__main__':
    #run_scraping_job(ticker_list=['b-work'])
    run_scraping_job()