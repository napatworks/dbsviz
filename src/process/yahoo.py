# Basic tools
import os
import datetime as dt

# Firebase
from firebase_admin import credentials
from firebase_admin import storage
import firebase_admin

# Data processing tools
import pandas as pd


class Yahoo:
    """
    Processing data from YAHOO Finance
    """

    def __init__(self, firebase_credential_path="../../credential/dbsweb-secret.json",
                 bucket="dbsweb-f2346.appspot.com"):
        # Initializing
        if (not len(firebase_admin._apps)):
            cred = credentials.Certificate(firebase_credential_path)
            firebase_admin.initialize_app(cred, {"storageBucket": bucket})

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

    def _upload_file(self, local_file_name, file_name, data_type, firebase_dir="data/process/yahoo/"):
        bucket = storage.bucket()
        today_str = dt.date.today().strftime(format="%Y%m%d")
        output_dir = firebase_dir + data_type + "/" + today_str + "/"
        blob = bucket.blob(output_dir + file_name)
        blob.upload_from_filename(local_file_name)

    def process_price_data(self, read_file_name="price_data.csv", write_file_name="price_data.csv"):
        # Reading data
        df = self._read_gcs(read_file_name, "price", "csv", firebase_dir="data/raw/yahoo/")

        # Renaming column name
        df.rename({"Date": "date",
                   "Open": "open",
                   "High": "high",
                   "Low": "low",
                   "Close": "close",
                   "Adj Close": "adj_close",
                   "Volume": "volume",
                   "yahoo_symbol": "yahoo_symbol"}, inplace=True, axis=1)

        df["symbol"] = df["yahoo_symbol"].str[:-3]

        df.to_csv("price_data.csv", sep="|", index=False)

        # Uploading files
        self._upload_file(write_file_name, write_file_name, data_type="price", firebase_dir="data/process/yahoo/")
        self._check_and_delete_old_file("price_data.csv")


def run(firebase_credential_path="../../credential/dbsweb-secret.json", bucket="dbsweb-f2346.appspot.com"):
    processor = Yahoo(firebase_credential_path=firebase_credential_path, bucket=bucket)
    processor.process_price_data()


if __name__ == "__main__":
    run()
