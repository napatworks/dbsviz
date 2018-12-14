# Basic tools
import os
import datetime as dt

# Firebase
from firebase_admin import credentials
from firebase_admin import storage
import firebase_admin

# Data processing tools
import numpy as np
import pandas as pd


class Set:
    """
    This class is used for processing data from SET website
    """

    def __init__(self, firebase_credential_path="../../credential/dbsweb-secret.json",
                 bucket="dbsweb-f2346.appspot.com"):

        # Initializing
        if (not len(firebase_admin._apps)):
            cred = credentials.Certificate(firebase_credential_path)
            firebase_admin.initialize_app(cred, {
                "storageBucket": bucket
            })

    def _read_gcs(self, file_name, data_type, file_type, firebase_dir="data/raw/set_website/"):

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

    def _upload_file(self, local_file_name, file_name, data_type, firebase_dir="data/process/set_website/"):
        bucket = storage.bucket()
        today_str = dt.date.today().strftime(format="%Y%m%d")
        output_dir = firebase_dir + data_type + "/" + today_str + "/"
        blob = bucket.blob(output_dir + file_name)
        blob.upload_from_filename(local_file_name)

    def process_metadata(self, read_file_name="metadata.xls", write_file_name="metadata.csv"):
        # Reading data
        df = self._read_gcs(read_file_name, "metadata", "html", firebase_dir="data/raw/set_website/")

        # Processing data
        df = df[0]
        df.columns = df.iloc[1]
        df.drop([0, 1], inplace=True)
        df.reset_index(inplace=True, drop=True)

        column_name = np.array(df.columns)
        # Format column names
        for i in range(len(df.columns)):
            column_name[i] = column_name[i].lower()
            column_name[i] = column_name[i].replace(" ", "_")
            column_name[i] = column_name[i].replace(".", "")
        df.columns = column_name

        # Yahoo Symbol
        df["yahoo_symbol"] = df["symbol"] + ".BK"

        # Flag SET100
        flag_df = self._read_gcs("set100_stocks.csv", "set100_stocks", "csv", firebase_dir="data/raw/set_website/")
        df["set100"] = 0
        df["set100"][df["symbol"].isin(flag_df["Symbol"])] = 1

        # Flag SET50
        flag_df = self._read_gcs("set50_stocks.csv", "set50_stocks", "csv", firebase_dir="data/raw/set_website/")
        df["set50"] = 0
        df["set50"][df["symbol"].isin(flag_df["Symbol"])] = 1

        # Flag SETCLMV
        flag_df = self._read_gcs("setclmv_stocks.csv", "setclmv_stocks", "csv", firebase_dir="data/raw/set_website/")
        df["setclmv"] = 0
        df["setclmv"][df["symbol"].isin(flag_df["Symbol"])] = 1

        # Flag SETHD
        flag_df = self._read_gcs("sethd_stocks.csv", "sethd_stocks", "csv", firebase_dir="data/raw/set_website/")
        df["sethd"] = 0
        df["sethd"][df["symbol"].isin(flag_df["Symbol"])] = 1

        # Flag SETTHSI
        flag_df = self._read_gcs("setthsi_stocks.csv", "setthsi_stocks", "csv", firebase_dir="data/raw/set_website/")
        df["setthsi"] = 0
        df["setthsi"][df["symbol"].isin(flag_df["Symbol"])] = 1

        # Flag sSET
        flag_df = self._read_gcs("sset_stocks.csv", "sset_stocks", "csv", firebase_dir="data/raw/set_website/")
        df["sset"] = 0
        df["sset"][df["symbol"].isin(flag_df["Symbol"])] = 1

        df.to_csv("metadata.csv", sep="|", index=False)

        # Uploading files
        self._upload_file(write_file_name, write_file_name, data_type="metadata",
                          firebase_dir="data/process/set_website/")
        self._check_and_delete_old_file("metadata.csv")


def run(firebase_credential_path="../../credential/dbsweb-secret.json", bucket="dbsweb-f2346.appspot.com"):
    processor = Set(firebase_credential_path=firebase_credential_path, bucket=bucket)
    processor.process_metadata()


if __name__ == "__main__":
    run()
