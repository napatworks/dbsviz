# Basic tools
import os
import datetime as dt
import sys
import copy

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

        df.to_csv(write_file_name, sep="|", index=False)

        # Uploading files
        self._upload_file(write_file_name, write_file_name, data_type="metadata",
                          firebase_dir="data/process/set_website/")
        self._check_and_delete_old_file("metadata.csv")

    def get_thai_ticker_list(self):
        """
        Getting ticker data from metadata in SET website
        """
        df = self._read_gcs("metadata.csv", "metadata", "csv", firebase_dir="data/process/set_website/")
        return np.array(df["symbol"].unique())

    def _factsheet_read_data(self):
        ticker_list = self.get_thai_ticker_list()
        output_data = {}
        progress = 0
        for symbol in ticker_list:
            # Progress
            progress += 1
            sys.stdout.write("\rReading data {0}/{1} ".format(str(progress), str(len(ticker_list))) + symbol + (" " * (10-len(symbol))))
            sys.stdout.flush()
            html_data = self._read_gcs(symbol + ".txt", "factsheet", "html", firebase_dir="data/raw/set_website/")
            data = {}
            data["ticker"] = symbol.upper()
            for i in range(len(html_data)):
                if html_data[i].iloc[0, 0] == "Price (B.)":
                    data["stat0"] = html_data[i]
                    
                if (html_data[i].iloc[0, 0] == "Free Float")&(html_data[i].shape[0]==3)&(html_data[i].shape[1]==6):
                    data["free_float"] = html_data[i].iloc[2, :4]
                    
                if html_data[i].iloc[0, 0] == "Business":
                    data["business"] = html_data[i]
                    
                if html_data[i].iloc[0, 0] == "Foreign Shareholders":
                    data["foreign_share"] = html_data[i]
                    
                if "Top 10 Major Shareholders" in str(html_data[i].iloc[0, 0]):
                    data["top_10_shareholders"] = html_data[i]
                    
                if "Management" in str(html_data[i].iloc[0, 0]):
                    data["management"] = html_data[i]
                    
                if "Value Trade/Day" in str(html_data[i].iloc[0, 0]):
                    data["stat1"] = html_data[i]
            output_data[symbol] = data
        return output_data


    def _factsheet_extract_info(self):
        
        # field_indices = {
        #                     "ticker": 2,
        #                     "stat0": 18,
        #                     'business': 10,
        #                     'free_float': 12,
        #                     'foreign_share': 13,
        #                     'top_10_shareholders': 16,
        #                     'management': 17,
        #                     "stat1": 18
        #                 }

        print("========== start reading html data ==========")
        data = self._factsheet_read_data()
        print("\n========== finish reading html data ==========")
        # Initializing blank variables
        extracted_data = {
                            "ticker": [],
                            'business': [],
                            'free_float': [],
                            'foreign_share': [],
                            'foreign_limit': [],
                            'nvdr_share': []
                        }
        shd_output_data = pd.DataFrame(columns=["rank", "shareholder_name", "no_of_shares", "percent_share", "ticker"])
        mng_output_data = pd.DataFrame(columns=["order", "management_name", "position", "ticker"])
        stats_output_data = pd.DataFrame(columns=['ticker', 'listed_share(million)', 'market_cap', 'current_price', 'bvps',
                                                   'pbv', 'pe', 'turnover_ratio', 'value_trade_per_day', 'beta',
                                                   'percent_price_change_ytd', 'dividend_yield_ytd', 'payout_ratio',
                                                   'dividend_policy', '52week_high', '52week_low',
                                                   'paidup(million)'])

        progress = 0
        print("========== start processing html data ==========")
        ticker_len = len(data.keys())
        for stock in data.keys():
            # print(data[stock].keys())
            progress += 1
            sys.stdout.write("\rProcessing data {0}/{1} ".format(str(progress), str(ticker_len)) + stock + (" " * (10-len(stock))))
            sys.stdout.flush()
            # 1. Extract overview data
            extracted_data["ticker"].append(stock)
            if "business" in data[stock].keys():
                extracted_data["business"].append(data[stock]["business"].iloc[1, 0])
            else:
                extracted_data["business"].append(np.nan)
            if "free_float" in data[stock].keys():
                extracted_data["free_float"].append(round(float(data[stock]["free_float"][0][:-1].replace(",", ""))/100, 4))
            else:
                extracted_data["free_float"].append(np.nan)
            
            if "foreign_share" in data[stock].keys():
                if "-" not in data[stock]["foreign_share"].iloc[0, 1].split('%')[0]:
                    extracted_data["foreign_share"].append(round(float(data[stock]["foreign_share"].iloc[0, 1].split('%')[0].replace(",", ""))/100, 4))
                else:
                    extracted_data["foreign_share"].append(np.nan)
                
                if "-" not in data[stock]["foreign_share"].iloc[0, 3].split('%')[0]:
                    extracted_data["foreign_limit"].append(round(float(data[stock]["foreign_share"].iloc[0, 3].split('%')[0].replace(",", ""))/100, 4))
                else:
                    extracted_data["foreign_limit"].append(np.nan)

                if '%' in data[stock]["foreign_share"].iloc[1, 1]:
                    extracted_data["nvdr_share"].append(round(float(data[stock]["foreign_share"].iloc[1, 1].split('%')[0].replace(",", ""))/100, 4))
                else:
                    extracted_data["nvdr_share"].append(0.0)
            else:
                extracted_data["foreign_share"].append(np.nan)
                extracted_data["foreign_limit"].append(np.nan)
                extracted_data["nvdr_share"].append(0.0)
                
            # 2. Major Shareholders
            if "top_10_shareholders" in data[stock].keys():
                shd_data = copy.copy(data[stock]["top_10_shareholders"])
                shd_data.drop(0, inplace=True)
                shd_data.reset_index(inplace=True, drop=True)
                if len(shd_data.columns) == 4:
                    shd_data.columns = ["rank", "shareholder_name", "no_of_shares", "percent_share"]
                    shd_data["rank"] = shd_data["rank"].str[:-1]
                    shd_data["percent_share"] = shd_data["percent_share"]/100
                    shd_data["ticker"] = stock.upper()
                    shd_output_data = shd_output_data.append(shd_data)
            
            # 3. Management
            if "management" in data[stock].keys():
                mng_data = copy.copy(data[stock]["management"])
                mng_data.drop(0, inplace=True)
                mng_data.reset_index(inplace=True, drop=True)
                if len(mng_data.columns) == 3:
                    mng_data.columns = ["order", "management_name", "position"]
                    mng_data["order"] = mng_data["order"].str[:-1]
                    mng_data["ticker"] = stock.upper()
                    mng_output_data = mng_output_data.append(mng_data)
            
            # 4. Snapshot stats
            if "stat1" in data[stock].keys():
                stats = copy.copy(data[stock]["stat1"].T.drop([0, 1], axis=1))
                stats.columns = stats.iloc[0]
                stats = stats.drop(0).reset_index(drop=True).iloc[0]
                stats = pd.DataFrame(stats).T
                drop_cols = ["nan", np.nan, "Rate of Return"]
                stats.drop([col for col in drop_cols if col in stats.columns], axis=1, inplace=True)
                rename_dict = {"Listed share (M.)": "listed_share(million)",
                 "Market Cap (MB.)": "market_cap",
                 "Price (B./share)": "current_price",
                 "BVPS (B./Share)": "bvps",
                 "P/BV (X)": "pbv",
                 "P/E (X)": "pe",
                 "Turnover Ratio (%)": "turnover_ratio",
                 "Value Trade/Day (MB.)": "value_trade_per_day",
                 "Beta": "beta",
                 "Price Change (%)": "percent_price_change_ytd",
                 "Dividend Yield (%)": "dividend_yield_ytd",
                 "Payout Ratio": "payout_ratio", 
                 "Dividend Policy": "dividend_policy"}
                stats.rename(rename_dict, axis=1, inplace=True)
                stats["ticker"] = stock
                stats["percent_price_change_ytd"][stats["percent_price_change_ytd"]=="-"] = np.nan
                stats["percent_price_change_ytd"] = stats["percent_price_change_ytd"].astype(float)
                stats["percent_price_change_ytd"] = stats["percent_price_change_ytd"]/100
                if ("/" in str(data[stock]["stat0"].iloc[1,1])) & (data[stock]["stat0"].iloc[1,1].split("/")[0] != "-"):
                    stats["52week_high"] = float(data[stock]["stat0"].iloc[1,1].split("/")[0].replace(",", ""))
                    stats["52week_low"] = float(data[stock]["stat0"].iloc[1,1].split("/")[1].replace(",", ""))
                else:
                    stats["52week_high"] = np.nan
                    stats["52week_low"] = np.nan

                if data[stock]["stat0"].iloc[1,4] != "-":
                    stats["paidup(million)"] = float(data[stock]["stat0"].iloc[1,4].replace(",", ""))
                else:
                    stats["paidup(million)"] = np.nan
                
                stats_output_data = stats_output_data.append(stats)
            
        shd_output_data.reset_index(drop=True, inplace=True)
        mng_output_data.reset_index(drop=True, inplace=True)
        stats_output_data.reset_index(drop=True, inplace=True)
        stats_output_data["pe"][stats_output_data["pe"]=="-"] = np.nan
        print("\n========== finish processing html data ==========")
        return pd.DataFrame(extracted_data), shd_output_data, mng_output_data, stats_output_data

    def process_fact_sheet(self):
        stat0, shareholder, management, stat1 = self._factsheet_extract_info()
        
        # overview snapshot
        stat0["as_of_date"] = dt.date.today()
        overview = pd.merge(stat0, stat1, how="outer", on="ticker")
        overview.to_csv("overview.csv", sep="|", index=False)
        self._upload_file("overview.csv", "overview.csv", data_type="factsheet", firebase_dir="data/process/set_website/")

        # shareholder
        shareholder["as_of_date"] = dt.date.today()
        shareholder.to_csv("shareholder.csv", sep="|", index=False)
        self._upload_file("shareholder.csv", "shareholder.csv", data_type="factsheet", firebase_dir="data/process/set_website/")

        # management
        management["as_of_date"] = dt.date.today()
        management.to_csv("management.csv", sep="|", index=False)
        self._upload_file("management.csv", "management.csv", data_type="factsheet", firebase_dir="data/process/set_website/")


def run(firebase_credential_path="../../credential/dbsweb-secret.json", bucket="dbsweb-f2346.appspot.com"):
    processor = Set(firebase_credential_path=firebase_credential_path, bucket=bucket)
    processor.process_metadata()

def run_factsheet(firebase_credential_path="../../credential/dbsweb-secret.json", bucket="dbsweb-f2346.appspot.com"):
    processor = Set(firebase_credential_path=firebase_credential_path, bucket=bucket)
    processor.process_fact_sheet()


if __name__ == "__main__":
    run()
