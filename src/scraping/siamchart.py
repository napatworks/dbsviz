import json
import pandas as pd
import numpy as np
import datetime as dt
import time
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support import expected_conditions as EC

import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage

DEFAULT_CHROME_DRIVER_DIR = '/usr/local/bin/chromedriver_v2.38'
SIAMCHART_URL = 'http://siamchart.com/stock-info/'
SIAMCHART_LOGIN_URL = 'http://siamchart.com/forum/showthread.php?152'
SIAMCHART_DIVIDEND_TABLE = 1
SIAMCHART_FINANCIAL_TABLE = 3
SIAMCHART_SPLIT_TABLE = 2

class SiamchartScraper:
    """ data reader for the siam chart site.
    """

    def __init__(self, chrome_driver_dir=None, browser=None):

        self.chrome_driver_dir = DEFAULT_CHROME_DRIVER_DIR if chrome_driver_dir is None else chrome_driver_dir
        self.browser = webdriver.Chrome(executable_path=self.chrome_driver_dir) if browser is None else browser

    def __del__(self):
        self.browser.quit()

    def login_to_siamchart(self, username, password):
        self.browser.get(SIAMCHART_LOGIN_URL)
        script = 'document.getElementById("navbar_username").value=' + '"' + username + '"'
        self.browser.execute_script(script)
        script = 'document.getElementById("navbar_password").value=' + '"' + password + '"'
        self.browser.execute_script(script)
        self.browser.find_element_by_class_name('loginbutton').click()
        return None

    def url_encode_siamchart(self, url):
        return url.replace('&', '_26')

    def get_url(self, ticker):
        url = SIAMCHART_URL + ticker.upper() + '/'
        url = self.url_encode_siamchart(url)
        return url

    def get_page_source(self, ticker, click_qoq=True):

        wait = WebDriverWait(self.browser, 10)
        self.browser.get(self.get_url(ticker))
        if click_qoq:
            num_round_find_qoq = 0
            while num_round_find_qoq <= 5:
                try:
                    # find qoq button
                    qoq_button = wait.until(
                        EC.presence_of_element_located((By.XPATH, "//div[@onclick='displayQoQ();']")))
                    qoq_button.click()
                    break
                except (TimeoutException, WebDriverException):
                    num_round_find_qoq += 1
                    print(ticker, ": retrying finding qoq button...", num_round_find_qoq, ' times')
                    continue
        return self.browser.page_source

    @classmethod
    def get_table_list(cls, page_source):
        return pd.read_html(page_source)

    @classmethod
    def get_financial_statement_table(cls, page_source):
        table_list = cls.get_table_list(page_source)
        if len(table_list) > SIAMCHART_FINANCIAL_TABLE:
            return table_list[SIAMCHART_FINANCIAL_TABLE]
        else:
            return None

def run_scraping_job():

    ## Initialize app
    cred = credentials.Certificate('../../credential/dbsweb-secret.json')
    firebase_admin.initialize_app(cred, {
        'storageBucket': 'dbsweb-f2346.appspot.com'
    })
    bucket = storage.bucket()

    ## set ticker and
    blob = bucket.blob('data/process/set_website/metadata/20181127/metadata.csv')
    meta_table_str = blob.download_as_string().decode('utf-8')
    df_meta = pd.read_csv(StringIO(meta_table_str), sep='|')
    tickers = list(df_meta['symbol'].unique())

    today_str = dt.date.today().strftime(format='%Y%m%d')
    output_dir = 'data/raw/siamchart/' + today_str + '/'

    ## login

    cred_siamchart = json.load(open('../../credential/credential.json', 'r'))
    username = cred_siamchart['SIAMCHART']['USERNAME']
    password = cred_siamchart['SIAMCHART']['PASSWORD']
    ss = SiamchartScraper()
    ss.login_to_siamchart(username=username, password=password)

    # set for-loop for loading
    num_round = 0
    tickers_loaded = []
    tickers_left = list(tickers)
    bad_tickers = set()
    while len(tickers_left) > 0 and num_round < 10:
        for ticker in tickers_left:
            try:
                try_relogin = False
                while True:
                    # Loading data from
                    print(ticker, ': Loading data for ', ticker)
                    page_source = ss.get_page_source(ticker)
                    table_list = ss.get_table_list(page_source)
                    df_financial = ss.get_financial_statement_table(page_source)
                    num_quarter = len(df_financial.columns)
                    print(ticker, ': Done for ticker ', ticker)
                    print(ticker, ': Number of tables ', len(table_list))
                    print(ticker, ': Number of quarters ', num_quarter)

                    # Try relogin in case of session timeout
                    if num_quarter <= 5 and not try_relogin:
                        try:
                            ss.login_to_siamchart(username=username, password=password)
                        except:
                            print(ticker, ': log in not successful, still in login session')
                        try_relogin = True
                        time.sleep(0.5 + 0.5 * np.random.rand())
                        print(ticker, ": relogin and reload the data ... ")
                    else:
                        break

                # save ps to storage
                blob = bucket.blob(output_dir + ticker.lower())
                blob.upload_from_string(page_source.encode('utf-8'))
                print(ticker, ': Successfully upload file to cloud storage')

                tickers_loaded.append(ticker)

            except:
                print(ticker, ': Error loading information for ticker ', ticker)
                bad_tickers.add(ticker)

        tickers_left = sorted(set(tickers_left) - set(tickers_loaded))
        num_round += 1

if __name__ =='__main__':
    run_scraping_job()

