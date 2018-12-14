# Scraping tools
from pandas_datareader import data as pdr
import fix_yahoo_finance as yf

# Basic tools
import sys
import datetime as dt
import os
from time import sleep
import json

# Firebase
from firebase_admin import credentials
from firebase_admin import storage
import firebase_admin

# Data processing tools
import numpy as np
import pandas as pd

class Yahoo:
    """
    Reading data from YAHOO Finance
    """
    def __init__(self, firebase_credential_path = "../../credential/dbsweb-secret.json", bucket = "dbsweb-f2346.appspot.com"):
        # Initializing
        if (not len(firebase_admin._apps)):
            cred = credentials.Certificate(firebase_credential_path)
            firebase_admin.initialize_app(cred, {
                   "storageBucket": bucket
               })

    def _read_gcs(self, file_name, data_type, file_type, firebase_dir="data/raw/yahoo/"):

        # Reading files in the firebase storage
        bucket = storage.bucket()
        today_str = dt.date.today().strftime(format="%Y%m%d")
        output_dir = firebase_dir + data_type + "/" + today_str + "/"
        blob = bucket.blob(output_dir + file_name)
        with open(file_name, 'wb') as file_obj:
            blob.download_to_file(file_obj)
        
        # Read with pandas DataFrame
        if file_type == "html":
            df = pd.read_html(file_name)
            self._check_and_delete_old_file(file_name)
            return df
        
        if file_type == "csv":
            df = pd.read_csv(file_name, sep="|")
            self._check_and_delete_old_file(file_name)
            return df

    def _check_and_delete_old_file(self, local_file_name):
        if os.path.isfile(local_file_name):
            os.remove(local_file_name)

    def _upload_file(self, local_file_name, file_name, data_type, firebase_dir="data/raw/yahoo/"):
        bucket = storage.bucket()
        today_str = dt.date.today().strftime(format="%Y%m%d")
        output_dir = firebase_dir + data_type + "/" + today_str + "/"
        blob = bucket.blob(output_dir + file_name)
        blob.upload_from_filename(local_file_name)

    def get_thai_ticker_list(self):
        """
        Getting ticker data from metadata in SET website
        """
        df = self._read_gcs("metadata.csv", "metadata", "csv", firebase_dir="data/process/set_website/")
        return np.array(df["yahoo_symbol"].unique())

    def get_price_data(self, ticker_list, start_date, end_date, progress_log=True):
        """
        Getting price data from Yahoo given ticker_list
        ticker_list: list of ticker
        start_date: start date of the data
        end_date: the last date of the data
        :return: price data, error list
        """
        # FIXED ERROR
        yf.pdr_override()

        # DOWNLOADING DATA
        error_list = []
        data = pdr.get_data_yahoo(
                        ticker_list[0],
                        start=start_date, end=end_date,
                        progress=False
                        ).reset_index()
        data['yahoo_symbol'] = ticker_list[0]
        
        error_list = list(ticker_list[1:])
        for repeat_r in range(10):
            progress_n = 1
            print("round {:d}".format(repeat_r))
            len_error_list = len(error_list)
            for ticker in error_list:
                if progress_log:
                    sys.stdout.write("\rDownloading {0}/{1}".format(str(progress_n), str(len_error_list)))
                    sys.stdout.flush()
                progress_n += 1
                try:
                    sleep(0.4*(repeat_r+1))
                    download_df = pdr.get_data_yahoo(ticker, start=start_date, end=end_date, progress=False).reset_index()
                    download_df['yahoo_symbol'] = ticker
                    data = pd.concat([data, download_df], ignore_index=True)
                    error_list.remove(ticker)
                except:
                    None
            if progress_log:
                print("\n")
            print("Error Number : {:d}".format(len(error_list)))

        print(error_list)

        return data, error_list

    def get_t(self, price_dataframe, ticker_independent_t=True, date_dependent_t=True):
        """
        Getting time t for each ticker
        :param price_dataframe:
        :return:
        """
        # CREATING TICKER t
        if ticker_independent_t:
            # GETTING t FOR EACH TICKER
            price_dataframe = price_dataframe.sort_values(['ticker', 'Date'])
            price_dataframe['ticker_t'] = price_dataframe.groupby(['ticker']).cumcount()

        #
        if date_dependent_t:
            # GETTING COMMON t FOR ALL TICKER
            time_master = pd.DataFrame(price_dataframe['Date'].unique()).sort_values([0])
            time_master.reset_index(drop=True, inplace=True)
            time_master.reset_index(inplace=True)
            time_master.columns = ['time_t', 'Date']
            price_dataframe = pd.merge(price_dataframe, time_master, how='left', on='Date')

        return price_dataframe

    def scrape(self):
        
        # Get params
        ticker_list = self.get_thai_ticker_list()
        start_date = "2000-01-01"
        end_date = str(dt.date.today())
        
        # Scrape price data
        price_df = self.get_price_data(ticker_list, start_date, end_date, progress_log=True)

        # Save to Firebase
        price_df[0].to_csv("tmp_yahoo_price.csv", sep="|", index=False)
        self._upload_file("tmp_yahoo_price.csv", "price_data.csv", "price", firebase_dir="data/raw/yahoo/")
        self._check_and_delete_old_file("tmp_yahoo_price.csv")

        # Error
        error_ticker = {"ticker": price_df[1]}
        with open('tmp_yahoo_error.json', 'w') as outfile:
            json.dump(error_ticker, outfile)
        self._upload_file("tmp_yahoo_error.json", "price_error_list.json", "price", firebase_dir="data/raw/yahoo/")
        self._check_and_delete_old_file("tmp_yahoo_error.json")

def run(firebase_credential_path = "../../credential/dbsweb-secret.json", bucket = "dbsweb-f2346.appspot.com"):
    scraper = Yahoo(firebase_credential_path = firebase_credential_path, bucket = bucket)
    scraper.scrape()

if __name__ == "__main__":
    run()














