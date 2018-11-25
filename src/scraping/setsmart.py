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

"""
TO DO:
1. run()
2. read metadata from ____
3. get security_id for new stock
4. read metadata from ____
5. check new stock that is not in metadata
6. 
"""

class SetSmart:
	"""
	This class is used for scraping data in various pages on Set Smart portal
	"""
	def __init__(self, id, password, chrome_driver_dir, download_dir):
		
		self.id = id
		self.password = password
		self.chrome_driver_dir = chrome_driver_dir
		self.download_dir = download_dir

		self.request_session = None

		# INITIALIZING CHROME
		option = webdriver.ChromeOptions()
		option.add_argument(" — incognito")
		prefs = {'download.default_directory' : self.download_dir}
		option.add_experimental_option('prefs', prefs)
		self.browser = webdriver.Chrome(self.chrome_driver_dir, chrome_options=option)

	def access_setsmart(self):
		sleep(1)
		# REACHING YUANTA WEBSITE
		self.browser.get('https://www.yuanta.co.th/')

		# ENTERING USERNAME AND PASSWORD
		sleep(1)
		user_id_input = self.browser.find_element_by_id("UserName")
		user_id_input.clear()
		user_id_input.send_keys(self.id)
		password_input = self.browser.find_element_by_id("Password")
		password_input.clear()
		password_input.send_keys(self.password)

		# LOGIN
		self.browser.find_element_by_class_name("btn-login").click()

		# ENTERING SETSMART
		sleep(1)
		self.browser.find_element_by_id("pop-1b4204d4-6484-4184-bfa5-435560d84467").click()
		self.browser.switch_to.window(self.browser.window_handles[1])

	def get_metadata(self):
		self.browser.get("https://www.set.or.th/en/company/companylist.html")
		sleep(1)
		self.browser.find_element_by_xpath('//a[@href="/dat/eod/listedcompany/static/listedCompanies_en_US.xls"]').click()
		sleep(1)
		meta_df = pd.read_html("listedCompanies_en_US.xls")[0]
		meta_df.columns = meta_df.iloc[1]
		meta_df.drop([0,1], inplace=True)
		meta_df.reset_index(inplace=True, drop=True)
		meta_df.to_csv("company_metadata.csv", sep="|", index=False)



	def get_factsheet_data(self, request_session, symbol, data, end_date, symbol_id_master_dict, start_date='01/01/1975'):
		## TO DO: WHERE TO READ symbol_id_master_dict
	    params = (
		    ('securityId', str(symbol_id_master_dict[symbol])),
		    ('viewBy', data),
		    ('period', 'D'),
		    ('beginDate', start_date),
		    ('endDate', end_date),
		    ('chkAdjusted', 'Y'),
		    ('locale', 'en_US'),
		)

	    response = request_session.get('http://sse.yuanta.co.th/factsheetchartdata.html', params=params)

	    data_df = pd.DataFrame(json.loads(response.text)[0])
	    
	    data_df["ticker"] = symbol

	    return data_df

	def _enterFactSheetPage(self):
		# GOING TO COMPANY FACTSHEET DATA PAGE
		action_chains.ActionChains(self.browser).move_to_element(self.browser.find_element_by_class_name('li-company')).perform()
		sleep(1)
		self.browser.find_element_by_xpath('//a[@href="/factsheet.html"]').click()

	def _get_security_id_setsmart(self):
	    return int(self.browser.page_source.split('securityId=')[1].split('&')[0])


	def get_all_security_id(self):
		# GOING TO COMPANY FACTSHEET DATA PAGE
		action_chains.ActionChains(self.browser).move_to_element(self.browser.find_element_by_class_name('li-company')).perform()
		sleep(1)
		self.browser.find_element_by_xpath('//a[@href="/factsheet.html"]').click()
		# INPUT SYMBOL
		## TO DO: FINDING ALL SYMBOL
		symbol_master = {}
		all_symbol = np.array(pd.read_csv('company_metadata.csv', sep='|')['Symbol'])
		n = 0
		all_symbol_n = len(all_symbol)
		# LOOP OVER SYMBOL
		for symbol in all_symbol:
		    n += 1
		    sys.stdout.write("\r{0}/{1}".format(str(n), str(all_symbol_n)))
		    sys.stdout.flush()
		    
		    try:
		        # CLEAR INPUT TEXT AND INPUT SYMBOL
		        self.browser.find_element_by_name('symbol').clear()
		        self.browser.find_element_by_name('symbol').send_keys(symbol)
		        sleep(1)
		        self.browser.find_element_by_name('submit').click()

		        # SAVE RESULT IN DICT
		        symbol_master[symbol] = self._get_security_id_setsmart()
		    except:
		        print('\n{}'.format(symbol))

        # SAVE AS JSON FILE
        ## TO DO: WHERE TO SAVE THE MASTER
		with open('symbol_id_setsmart.json', 'w') as fp:
		    json.dump(symbol_master, fp)

		return symbol_master

	def scrapeFactSheet(self, measure, end_date, sleep_time = 0):
		"""
		@param measure: 'price', 'pe', 'pbv', 'currentRatio', 'de', 'roa', 'roe', 'grossProfitMargin', 'netProfitMargin', 'ebitMargin', 'totalAssetTurnover', 'beta'
		"""
		error_list = []
		nn = 0

		## TO DO: Scrape only stock_id that is not in the master
		with open('symbol_id_setsmart.json') as f:
		    symbol_master = json.load(f)
		all_symbol_n = len(symbol_master.keys())
		s = requests.Session()
		for cookie in self.browser.get_cookies():
		    s.cookies.set(cookie['name'], cookie['value'])

		## TO DO: WHERE TO SAVE THE FILES
		for symbol in symbol_master.keys():
		    nn += 1
		    sys.stdout.write("\r{0}/{1}".format(str(nn), str(all_symbol_n)))
		    sys.stdout.flush()
		    
		    try:
		        tmp_df = self.get_factsheet_data(s, symbol, measure, symbol_id_master_dict=symbol_master, end_date=end_date, start_date='01/01/1900')
		        if nn == 1:
		            data_df = tmp_df
		        else:
		        	# TO DO: save one file for one measure and one stock
		            data_df = pd.concat([data_df, tmp_df])
		    except:
		        print('\n{}'.format(symbol))
		        error_list.append(symbol)
		        tmp_df = self.get_factsheet_data(s, symbol, measure, symbol_id_master_dict=symbol_master, end_date=end_date, start_date='01/01/1900')
		        if nn == 1:
		            data_df = tmp_df
		        else:
		        	# TO DO: save one file for one measure and one stock
		            data_df = pd.concat([data_df, tmp_df])
		    
		    # Sleep
		    if sleep_time > 0:
		    	sleep(np.random.uniform(max(0, sleep_time-2), sleep_time+2))

		data_df.to_csv("{0}_{1}{2}{3}".format(measure, end_date[-4:], end_date[3:5], end_date[:2]), sep="|", index=False)



















