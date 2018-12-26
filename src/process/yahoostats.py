from firebase_admin import credentials
from firebase_admin import storage
import firebase_admin

import pandas as pd
import datetime as dt
import os


class YahooStatsProcessor:

    COLUMNS_MAPPER = {'% Held by Insiders 1': 'percent_held_by_insiders',
         '% Held by Institutions 1': 'percent_held_by_institutions',
         '200-Day Moving Average 3': '200_day_moving_average',
         '5 Year Average Dividend Yield 4': '5_year_average_dividend_yield',
         '50-Day Moving Average 3': '50_day_moving_average',
         '52 Week High 3': '52_week_high',
         '52 Week Low 3': '52_week_low',
         '52-Week Change 3': '52_week_change',
         'Avg Vol (10 day) 3': 'avg_vol_10_day',
         'Avg Vol (3 month) 3': 'avg_vol_3_month',
         'Beta (3Y Monthly)': 'beta_3y_monthly',
         'Book Value Per Share (mrq)': 'book_value_per_share',
         'Current Ratio (mrq)': 'current_ratio',
         'Diluted EPS (ttm)': 'diluted_eps_ttm',
         'Dividend Date 3': 'dividend_date',
         'EBITDA': 'ebitda',
         'Enterprise Value 3': 'enterprise_value',
         'Enterprise Value/EBITDA 6': 'enterprise_value_per_ebitda',
         'Enterprise Value/Revenue 3': 'enterprise_value_per_revenue',
         'Ex-Dividend Date 4': 'ex_dividend_date',
         'Fiscal Year Ends': 'fiscal_year_ends',
         'Float': 'float',
         'Forward Annual Dividend Rate 4': 'forward_annual_dividend_rate',
         'Forward Annual Dividend Yield 4': 'forward_annual_dividend_yield',
         'Forward P/E 1': 'forward_pe',
         'Gross Profit (ttm)': 'gross_profit_ttm',
         'Last Split Date 3': 'last_split_date',
         'Last Split Factor (new per old) 2': 'last_split_factor_new_per_old',
         'Levered Free Cash Flow (ttm)': 'levered_free_cash_flow_ttm',
         'Market Cap (intraday) 5': 'market_cap',
         'Most Recent Quarter (mrq)': 'most_recent_quarter',
         'Net Income Avi to Common (ttm)': 'net_income_ttm',
         'Operating Cash Flow (ttm)': 'operating_cash_flow_ttm',
         'Operating Margin (ttm)': 'operating_margin_ttm',
         'PEG Ratio (5 yr expected) 1': 'peg_ratio_5_yr_expected',
         'Payout Ratio 4': 'payout_ratio',
         'Price/Book (mrq)': 'price_per_book',
         'Price/Sales (ttm)': 'price_per_sales_ttm',
         'Profit Margin': 'profit_margin',
         'Quarterly Earnings Growth (yoy)': 'quarterly_earnings_growth_yoy',
         'Quarterly Revenue Growth (yoy)': 'quarterly_revenue_growth_yoy',
         'Return on Assets (ttm)': 'return_on_assets_ttm',
         'Return on Equity (ttm)': 'return_on_equity_ttm',
         'Revenue (ttm)': 'revenue_ttm',
         'Revenue Per Share (ttm)': 'revenue_per_share_ttm',
         'S&P500 52-Week Change 3': 'sp500_52_week_change',
         'Shares Outstanding 5': 'shares_outstanding',
         'Shares Short (prior month ) 4': 'shares_short_prior_month',
         'Shares Short 4': 'shares_short',
         'Short % of Float 4': 'short_percent_of_float',
         'Short % of Shares Outstanding 4': 'short_percent_of_shares_outstanding',
         'Short Ratio 4': 'short_ratio',
         'Total Cash (mrq)': 'total_cash',
         'Total Cash Per Share (mrq)': 'total_cash_per_share',
         'Total Debt (mrq)': 'total_debt',
         'Total Debt/Equity (mrq)': 'total_debt_per_equity',
         'Trailing Annual Dividend Rate 3': 'trailing_annual_dividend_rate',
         'Trailing Annual Dividend Yield 3': 'trailing_annual_dividend_yield',
         'Trailing P/E': 'trailing_pe'}


    def __init__(self, input_date=None, output_date=None, firebase_credential_path="../../credential/dbsweb-secret.json",
                 bucket="dbsweb-f2346.appspot.com"):
        #TODO change to add latest data, but concerned about being slow

        # Initialize app
        if (not len(firebase_admin._apps)):
            cred = credentials.Certificate(firebase_credential_path)
            firebase_admin.initialize_app(cred, {"storageBucket": bucket})
        self.bucket = storage.bucket()

        # Initialize data version
        if input_date is None:
            self.path_dir = self._get_latest_version(prefix='data/raw/yahoo/stats/')
        else:
            self.path_dir = 'data/raw/yahoo/stats/'+str(input_date)

        # Initialize output version
        if output_date is None:
            self.output_dir = 'data/process/yahoo/stats/'+dt.date.today().strftime(format="%Y%m%d")+'/'
        else:
            self.output_dir = 'data/process/yahoo/stats/'+str(output_date)+'/'
        self.output_path = self.output_dir+'statistics.csv'

    def _get_latest_version(self, prefix):
        max_version = '00000000'
        for blob in self.bucket.list_blobs(prefix=prefix):
            max_version = max(max_version, blob.name.split('/')[-2])
        return prefix+max_version

    def _process_data(self, df):
        df_process = df.copy()
        if len(df_process.columns)!= 2:
            print('Warning: not expected format, expected 2 columns tables from yahoo stats')
            return None
        df_process.columns = ['item_original','value']
        df_process['item'] = df_process['item_original'].apply(lambda x: YahooStatsProcessor.COLUMNS_MAPPER.get(x,x))
        return df_process

    def get_output_path(self):
        return self.output_path

    @staticmethod
    def _process_item(item_name):
        """function to generate maps for columns name change
        It is unused in this class, for reference and regenerate columns mappper"""
        s = item_name.lower()
        for c in [' (intraday)',' (mrq)', ' avi to common','(',')']:
            s = s.replace(c,'')
        s = s.replace('p/e','pe')
        s = s.replace('/','_per_')
        s = s.replace(' ','_')
        s = s.replace('-','_')
        s = s.replace('%','percent')
        if s.split('_')[-1].isdigit():
            s = '_'.join(s.split('_')[:-1])
        s = s.replace('&','')
        return s

    def run_processing_data_job(self):
        # TODO modify funciton to be suitable for testing
        local_tmp_file = 'tmp_yahoo_stats.csv'
        df_stats_all = pd.DataFrame()
        for blob in self.bucket.list_blobs(prefix=self.path_dir):
            with open(local_tmp_file, 'wb') as file_obj:
                blob.download_to_file(file_obj)
            ticker = str(blob.name.split('/')[-1]).replace('.csv','')
            df_stats = pd.read_csv(local_tmp_file, sep='|')
            df_stats_process = self._process_data(df_stats)
            df_stats_process['ticker']=ticker
            df_stats_all = df_stats_all.append(df_stats_process)
            print(ticker, ': Successfully process tickers')
        df_stats_all.to_csv(local_tmp_file, sep='|',index=False)
        output_blob = self.bucket.blob(self.output_path)
        output_blob.upload_from_filename(local_tmp_file)

        # delete tmp file
        if os.path.isfile(local_tmp_file):
            os.remove(local_tmp_file)

if __name__=='__main__':
    yh = YahooStatsProcessor()
    yh.run_processing_data_job()

