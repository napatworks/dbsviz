import pandas as pd
import numpy as np
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support import expected_conditions as EC

DEFAULT_CHROME_DRIVER_DIR = '/usr/local/bin/chromedriver_v2.38'
SIAMCHART_URL = 'http://siamchart.com/stock-info/'
SIAMCHART_LOGIN_URL = 'http://siamchart.com/forum/showthread.php?152'
SIAMCHART_DIVIDEND_TABLE = 1
SIAMCHART_FINANCIAL_TABLE = 3
SIAMCHART_SPLIT_TABLE = 2

class SiamChartScraper:
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
