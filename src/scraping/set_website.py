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
    
    def get_metadata(self):
        self.browser.get("https://www.set.or.th/en/company/companylist.html")
        sleep(1)
        self.browser.find_element_by_xpath('//a[@href="/dat/eod/listedcompany/static/listedCompanies_en_US.xls"]').click()
        sleep(1)
        
    def get_delisting(self):
        self.browser.get("https://www.set.or.th/en/company/companylist.html")
        sleep(1)
        self.browser.find_element_by_xpath('//a[@href="/dat/eod/listedcompany/static/delistedSecurities_en_US.xls"]').click()
        sleep(1)
        
    def get_possible_delisting(self):
        self.browser.get("https://www.set.or.th/en/company/companylist.html")
        sleep(1)
        self.browser.find_element_by_xpath('//a[@onclick="ga(\'send\', \'event\', \'PDF\', \'Companies\', \'PossibleDelistingCompanies - EN\');"]').click()
        sleep(1)
        
    def get_current_set100_stocks(self):
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SET100&language=en&country=US")
        sleep(1)
        pd.read_html(scraper.get_current_set100_stocks())[2].to_csv("set100_stocks.csv", sep="|", index=False)
    
    def get_current_set50_stocks(self):
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SET50&language=en&country=US")
        sleep(1)
        pd.read_html(scraper.get_current_set100_stocks())[2].to_csv("set50_stocks.csv", sep="|", index=False)
    
    def get_current_sethd_stocks(self):
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETHD&language=en&country=US")
        sleep(1)
        pd.read_html(scraper.get_current_set100_stocks())[2].to_csv("sethd_stocks.csv", sep="|", index=False)
    
    def get_current_setclmv_stocks(self):
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETCLMV&language=en&country=US")
        sleep(1)
        pd.read_html(scraper.get_current_set100_stocks())[2].to_csv("setclmv_stocks.csv", sep="|", index=False)
        
    def get_current_sset_stocks(self):
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=sSET&language=en&country=US")
        sleep(1)
        pd.read_html(scraper.get_current_set100_stocks())[2].to_csv("sset_stocks.csv", sep="|", index=False)
    
    def get_current_setthsi_stocks(self):
        self.browser.get("https://marketdata.set.or.th/mkt/sectorquotation.do?sector=SETTHSI&language=en&country=US")
        sleep(1)
        pd.read_html(scraper.get_current_set100_stocks())[2].to_csv("setthsi_stocks.csv", sep="|", index=False)
        
        
        
        
        
        
        
        
        
        
        