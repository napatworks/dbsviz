from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
import selenium
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
from selenium.webdriver.common import action_chains, keys
import sys
import datetime
import numpy as np
import json
import requests
import datetime as dt
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
import os

class Set:
    """
    Data reader from SET website
    """
    def __init__(self, chrome_driver_dir, download_dir):
        
        self.chrome_driver_dir = chrome_driver_dir
        self.download_dir = download_dir
        
        # INITIALIZING CHROME
        option = webdriver.ChromeOptions()
        option.add_argument(" — incognito")
        prefs = {'download.default_directory' : self.download_dir}
        option.add_experimental_option('prefs', prefs)
        self.browser = webdriver.Chrome(self.chrome_driver_dir, chrome_options=option)

        cred = credentials.Certificate("../../credential/dbsweb-secret.json")
        firebase_admin.initialize_app(cred, {
               "storageBucket": "dbsweb-f2346.appspot.com"
           })
    
    def _upload_file(self, local_file_name, file_name, data_type, firebase_dir="data/raw/set_website/"):
        bucket = storage.bucket()
        today_str = dt.date.today().strftime(format="%Y%m%d")
        output_dir = firebase_dir + data_type + "/" + today_str + "/"
        blob = bucket.blob(output_dir + file_name)
        blob.upload_from_filename(local_file_name)

    def _check_and_delete_old_file(self, local_file_name):
        if os.path.isfile(self.download_dir+local_file_name):
            os.remove(self.download_dir+local_file_name)

    def get_metadata(self):
        self._check_and_delete_old_file(self.download_dir+"listedCompanies_en_US.xls")
        self.browser.get("https://www.set.or.th/en/company/companylist.html")
        sleep(1)
        self.browser.find_element_by_xpath('//a[@href="/dat/eod/listedcompany/static/listedCompanies_en_US.xls"]').click()
        sleep(1)
        self._upload_file(self.download_dir+"listedCompanies_en_US.xls", "metadata.xls", "metadata", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"listedCompanies_en_US.xls")
        
    def get_delisting(self):
        self._check_and_delete_old_file(self.download_dir+"delistedSecurities_en_US.xls")
        self.browser.get("https://www.set.or.th/en/company/companylist.html")
        sleep(1)
        self.browser.find_element_by_xpath('//a[@href="/dat/eod/listedcompany/static/delistedSecurities_en_US.xls"]').click()
        sleep(1)
        self._upload_file(self.download_dir+"delistedSecurities_en_US.xls", "delisted.xls", "delisted", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"delistedSecurities_en_US.xls")

    # def get_possible_delisting(self):
    #     self.browser.get("https://www.set.or.th/en/company/companylist.html")
    #     sleep(1)
    #     self.browser.find_element_by_xpath('//a[@onclick="ga(\'send\', \'event\', \'PDF\', \'Companies\', \'PossibleDelistingCompanies - EN\');"]').click()
    #     sleep(1)
    #     _upload_file(self.download_dir+"delistedSecurities_en_US.xls", "delisted.xls", "delisted", firebase_dir="data/raw/set_website/")
    #     sleep(1)
        
    def get_current_set100_stocks(self):
        self._check_and_delete_old_file(self.download_dir+"set100_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SET100&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "set100_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir+"set100_stocks.csv", "set100_stocks.csv", "set100_stocks", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"set100_stocks.csv")
    
    def get_current_set50_stocks(self):
        self._check_and_delete_old_file(self.download_dir+"set50_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SET50&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "set50_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir+"set50_stocks.csv", "set50_stocks.csv", "set50_stocks", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"set50_stocks.csv")

    def get_current_sethd_stocks(self):
        self._check_and_delete_old_file(self.download_dir+"sethd_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETHD&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "sethd_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir+"sethd_stocks.csv", "sethd_stocks.csv", "sethd_stocks", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"sethd_stocks.csv")

    def get_current_setclmv_stocks(self):
        self._check_and_delete_old_file(self.download_dir+"setclmv_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETCLMV&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "setclmv_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir+"setclmv_stocks.csv", "setclmv_stocks.csv", "setclmv_stocks", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"setclmv_stocks.csv")

    def get_current_sset_stocks(self):
        self._check_and_delete_old_file(self.download_dir+"sset_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=sSET&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "sset_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir+"sset_stocks.csv", "sset_stocks.csv", "sset_stocks", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"sset_stocks.csv")

    def get_current_setthsi_stocks(self):
        self._check_and_delete_old_file(self.download_dir+"setthsi_stocks.csv")
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETTHSI&language=en&country=US")
        sleep(1)
        pd.read_html(self.browser.page_source)[2].to_csv(self.download_dir + "setthsi_stocks.csv", sep="|", index=False)
        sleep(1)
        self._upload_file(self.download_dir+"setthsi_stocks.csv", "setthsi_stocks.csv", "setthsi_stocks", firebase_dir="data/raw/set_website/")
        sleep(1)
        os.remove(self.download_dir+"setthsi_stocks.csv")

def run():
    scraper = Set("../../tools/chromedriver", "")
    scraper.get_metadata()
    scraper.get_delisting()
    # scraper.get_possible_delisting()
    scraper.get_current_set100_stocks()
    scraper.get_current_set50_stocks()
    scraper.get_current_sethd_stocks()
    scraper.get_current_setclmv_stocks()
    scraper.get_current_sset_stocks()
    scraper.get_current_setthsi_stocks()

if __name__ == "__main__":
    run()
        
        
        
        
        
        
        
        