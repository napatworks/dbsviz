# Scraping tools
from selenium import webdriver
import urllib.request
import requests

# Data management tools
import pandas as pd
import numpy as np

# Basic tools
from time import sleep
import datetime as dt
import os
import copy
import sys

# Firebase
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage


class Set:
    """
    Data reader from SET website
    """

    def __init__(self, chrome_driver_dir, download_dir, firebase_credential_path="../../credential/dbsweb-secret.json",
                 bucket="dbsweb-f2346.appspot.com"):
        self.chrome_driver_dir = chrome_driver_dir
        self.download_dir = download_dir

        # INITIALIZING CHROME
        option = webdriver.ChromeOptions()
        option.add_argument(" — incognito")
        option.add_argument('--headless')
        option.add_argument('--no-sandbox')
        option.add_argument('--disable-dev-shm-usage')
        prefs = {'download.default_directory': self.download_dir}
        option.add_experimental_option('prefs', prefs)
        self.browser = webdriver.Chrome(self.chrome_driver_dir, chrome_options=option)

        self.browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': "/path/to/download/dir"}}
        command_result = self.browser.execute("send_command", params)


        # profile = webdriver.FirefoxProfile()
        # profile.set_preference("browser.download.folderList", 2)
        # profile.set_preference("browser.download.manager.showWhenStarting", False)
        # profile.set_preference("browser.download.dir", '')
        # profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")

        # self.browser = webdriver.Firefox(chrome_driver_dir, firefox_profile=profile)

        if (not len(firebase_admin._apps)):
            cred = credentials.Certificate(firebase_credential_path)
            firebase_admin.initialize_app(cred, {
                "storageBucket": bucket
            })

    def _upload_file(self, local_file_name, file_name, data_type, firebase_dir="data/raw/set_website/"):
        """
        Uploading files to Firebase
        """
        bucket = storage.bucket()
        today_str = dt.date.today().strftime(format="%Y%m%d")
        output_dir = firebase_dir + data_type + "/" + today_str + "/"
        blob = bucket.blob(output_dir + file_name)
        blob.upload_from_filename(local_file_name)

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

    def get_thai_ticker_list(self):
        """
        Getting ticker data from metadata in SET website
        """
        df = self._read_gcs("metadata.csv", "metadata", "csv", firebase_dir="data/process/set_website/")
        return np.array(df["symbol"].unique())

    def _check_and_delete_old_file(self, local_file_name):
        """
        Checking and deleting file in the local path
        """
        if os.path.isfile(self.download_dir + local_file_name):
            os.remove(self.download_dir + local_file_name)

    def get_metadata(self):
        """
        Get company metadata including symbol, company name, location, telephone number, etc.
        """
        self._check_and_delete_old_file(self.download_dir + "listedCompanies_en_US.xls")
#         self.browser.get("https://www.set.or.th/en/company/companylist.html")
#         sleep(1)
#         self.browser.find_element_by_xpath(
# '//a[@href="/dat/eod/listedcompany/static/listedCompanies_en_US.xls"]').click()
#         sleep(1)
        urllib.request.urlretrieve("https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls", self.download_dir + "listedCompanies_en_US.xls")
        self._upload_file(self.download_dir + "listedCompanies_en_US.xls", "metadata.xls", "metadata",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "listedCompanies_en_US.xls")

    def get_delisting(self):
        """
        Getting delisting company list
        """
        self._check_and_delete_old_file(self.download_dir + "delistedSecurities_en_US.xls")
        # self.browser.get("https://www.set.or.th/en/company/companylist.html")
        # sleep(1)
        # self.browser.find_element_by_xpath(
        #     '//a[@href="/dat/eod/listedcompany/static/delistedSecurities_en_US.xls"]').click()
        # sleep(1)
        urllib.request.urlretrieve("https://www.set.or.th/dat/eod/listedcompany/static/delistedSecurities_en_US.xls", self.download_dir + "delistedSecurities_en_US.xls")
        self._upload_file(self.download_dir + "delistedSecurities_en_US.xls", "delisted.xls", "delisted",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "delistedSecurities_en_US.xls")

    # def get_possible_delisting(self):
    #     self.browser.get("https://www.set.or.th/en/company/companylist.html")
    #     sleep(1)
    #     self.browser.find_element_by_xpath('//a[@onclick="ga(\'send\', \'event\', \'PDF\', \'Companies\', \'PossibleDelistingCompanies - EN\');"]').click()
    #     sleep(1)
    #     _upload_file(self.download_dir+"delistedSecurities_en_US.xls", "delisted.xls", "delisted", firebase_dir="data/raw/set_website/")
    #     sleep(1)

    def get_current_set100_stocks(self):
        """
        Getting today price performance of stocks in SET100
        """
        self._check_and_delete_old_file(self.download_dir + "set100_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SET100&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "set100_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir + "set100_stocks.csv", "set100_stocks.csv", "set100_stocks",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "set100_stocks.csv")

    def get_current_set50_stocks(self):
        """
        Getting today price performance of stocks in SET50
        """
        self._check_and_delete_old_file(self.download_dir + "set50_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SET50&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "set50_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir + "set50_stocks.csv", "set50_stocks.csv", "set50_stocks",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "set50_stocks.csv")

    def get_current_sethd_stocks(self):
        """
        Getting today price performance of stocks in SETHD
        """
        self._check_and_delete_old_file(self.download_dir + "sethd_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETHD&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "sethd_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir + "sethd_stocks.csv", "sethd_stocks.csv", "sethd_stocks",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "sethd_stocks.csv")

    def get_current_setclmv_stocks(self):
        """
        Getting today price performance of stocks in SETCLMV
        """
        self._check_and_delete_old_file(self.download_dir + "setclmv_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETCLMV&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "setclmv_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir + "setclmv_stocks.csv", "setclmv_stocks.csv", "setclmv_stocks",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "setclmv_stocks.csv")

    def get_current_sset_stocks(self):
        """
        Getting today price performance of stocks in sSET
        """
        self._check_and_delete_old_file(self.download_dir + "sset_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=sSET&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "sset_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir + "sset_stocks.csv", "sset_stocks.csv", "sset_stocks",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "sset_stocks.csv")

    def get_current_setthsi_stocks(self):
        """
        Getting today price performance of stocks in SETTHSI
        """
        self._check_and_delete_old_file(self.download_dir + "setthsi_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETTHSI&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "setthsi_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir + "setthsi_stocks.csv", "setthsi_stocks.csv", "setthsi_stocks",
                          firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir + "setthsi_stocks.csv")

    def _factsheet_get_page(self, symbol, user_agent=None, proxy=None):
        """Getting company data through factsheet page"""
        
        # Case when user specify user_agent and proxy
        if (user_agent is not None) & (proxy is not None):
            headers = {
                  'accept-encoding': 'gzip, deflate, br',
                  'accept-language': 'en-US,en;q=0.9',
                  'user-agent': user_agent,
                  'accept': '*/*',
                }
            data = requests.get("https://www.set.or.th/set/factsheet.do?symbol={0}&ssoPageId=3&language=en&country=US".format(symbol.upper().replace("&", "%26")), 
                                headers=headers, 
                                proxies={"http": proxy, "https": proxy}).text
        
        elif (user_agent is not None) & (proxy is None):
            headers = {
                  'accept-encoding': 'gzip, deflate, br',
                  'accept-language': 'en-US,en;q=0.9',
                  'user-agent': user_agent,
                  'accept': '*/*',
                }

            data = requests.get("https://www.set.or.th/set/factsheet.do?symbol={0}&ssoPageId=3&language=en&country=US".format(symbol.upper().replace("&", "%26")), 
                                headers=headers).text
        elif (user_agent is None) & (proxy is not None):
            data = requests.get("https://www.set.or.th/set/factsheet.do?symbol={0}&ssoPageId=3&language=en&country=US".format(symbol.upper().replace("&", "%26")), 
                                proxies={"http": proxy, "https": proxy}).text
        else:
            data = requests.get("https://www.set.or.th/set/factsheet.do?symbol={0}&ssoPageId=3&language=en&country=US".format(symbol.upper().replace("&", "%26"))).text
        
        return data

    def get_factsheet_data(self):
        symbol_list = self.get_thai_ticker_list()
        progress_n = 0
        for symbol in symbol_list:
            data = self._factsheet_get_page(symbol.replace("&", "%26"))
            progress_n += 1
            sys.stdout.write("\rDownloading {0}/{1}".format(str(progress_n), str(len(symbol_list))))
            sys.stdout.flush()
            file = open(symbol+".txt","w") 
            file.write(data)
            file.close()
            self._upload_file(self.download_dir + symbol + ".txt", symbol + ".txt", "factsheet",
                          firebase_dir="data/raw/set_website/")
            os.remove(self.download_dir + symbol + ".txt")


def run(chrome_driver_dir="../../tools/chromedriver", download_dir="",
        firebase_credential_path="../../credential/dbsweb-secret.json", bucket="dbsweb-f2346.appspot.com"):
    scraper = Set(chrome_driver_dir, download_dir, firebase_credential_path=firebase_credential_path, bucket=bucket)
    scraper.get_metadata()
    scraper.get_delisting()
    # scraper.get_possible_delisting()
    scraper.get_current_set100_stocks()
    scraper.get_current_set50_stocks()
    scraper.get_current_sethd_stocks()
    scraper.get_current_setclmv_stocks()
    scraper.get_current_sset_stocks()
    scraper.get_current_setthsi_stocks()

def run_factsheet(chrome_driver_dir="../../tools/chromedriver", download_dir="",
        firebase_credential_path="../../credential/dbsweb-secret.json", bucket="dbsweb-f2346.appspot.com"):
    scraper = Set(chrome_driver_dir, download_dir, firebase_credential_path=firebase_credential_path, bucket=bucket)
    scraper.get_factsheet_data()


if __name__ == "__main__":
    run_factsheet()
