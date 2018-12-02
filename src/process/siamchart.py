from firebase_admin import credentials
from firebase_admin import storage
import firebase_admin

import pandas as pd
import numpy as np
import os


class SiamchartProcessor:
    ## TODO change from printing to logging

    SIAMCHART_DIVIDEND_TABLE = 1
    SIAMCHART_FINANCIAL_TABLE = 3
    SIAMCHART_SPLIT_TABLE = 2

    def __init__(self, page_source, ticker):

        self.page_source = page_source
        self.ticker = ticker
        self.tables = pd.read_html(self.page_source)

    def _verify_data(self, idx):
        if len(self.tables) < 1:
            print('Warning: Number of tables from raw is less than XXX')
            return False
        else:
            return True

    def get_financial_data(self, raw=False, adjust_quarterly=True):
        df = self.tables[SiamchartProcessor.SIAMCHART_FINANCIAL_TABLE].T
        if raw:
            return df
        df = self._clean_financial_data(df)
        if adjust_quarterly:
            df = self.adjust_quarterly_result(df, self.ticker)
        df.index.name = 'date'
        return df

    def get_dividend_data(self):
        """dfQ=format from reading pagesource from siamchart directly"""
        # load and store dividend
        dfDiv = self.tables[SiamchartProcessor.SIAMCHART_DIVIDEND_TABLE]
        dfDiv = dfDiv[2:]
        dfDiv.columns = ['date', 'dividend', 'close_price', 'dividend_yield']
        dfDiv.set_index('date', inplace=True)
        dfDiv.index = pd.DatetimeIndex(dfDiv.index)
        dfDiv.index = dfDiv.index.date
        dfDiv.sort_index(inplace=True)
        return dfDiv

    @staticmethod
    def _clean_financial_data(df):

        # load and store financial data
        df = df.apply(lambda x: x.map(lambda x: str(x).split('(')[0]))
        df.set_index(0, inplace=True)
        df.rename(columns=df.iloc[0], inplace=True)
        df.drop(df.index[0], inplace=True)
        df.index.name = None

        def float_with_error(x):
            try:
                return float(x)
            except:
                return np.nan
        df = df.applymap(lambda x: float_with_error(x.replace(',', '')))
        df = df.dropna(axis=1, thresh=0.1 * len(df))
        df.index = pd.DatetimeIndex(df.index)
        df['month'] = df.index.month
        df['year'] = df.index.year
        df.index = df.index.date
        df.sort_index(inplace=True)

        # change column name
        columnChangeMap = {'ราคาปิด': 'close_price',
                           'รวมรายได้': 'revenue',
                           'รวมค่าใช้จ่าย': 'expense',
                           'กำไรขั้นต้น': 'gross_profit',
                           'กำไรจากการดำเนินงาน': 'operating_profit',
                           'EBIT': 'ebit',
                           'EBITDA': 'ebitda',
                           'กำไรสุทธิ': 'net_profit',
                           'เงินสด': 'cash',
                           'ลูกหนี้และตั๋วเงินรับ': 'account_receivable',
                           'สินค้าคงเหลือ': 'inventory',
                           'สินทรัพย์หมุนเวียน': 'current_asset',
                           'ที่ดินอาคารและอุปกรณ์': 'property_plant_equipment',
                           'รวมสินทรัพย์': 'total_asset',
                           'เบิกเกินบัญชีและเงินกู้ยืม': 'short_term_loan',
                           'เจ้าหนี้และตั๋วเงินจ่าย': 'account_payable',
                           'หนี้สินระยะยาวครบในหนึ่งปี': 'current_portion_long_term_debt',
                           'หนี้สินหมุนเวียน': 'current_liability',
                           'หนี้สินไม่หมุนเวียน': 'non_current_liability',
                           'รวมหนี้สิน': 'total_liability',
                           'มูลค่าหุ้นที่เรียกชำระแล้ว': 'paid_in_capital',
                           'รวมส่วนของผู้ถือหุ้น': 'total_equity',
                           'เงินสดจากการดำเนินงาน': 'cash_flow_operation',
                           'เงินสดจากการลงทุน': 'cash_flow_investment',
                           'เงินสดจากการจัดหาเงิน': 'cash_flow_financing',
                           'ค่าเสื่อมราคาค่าตัดจำหน่าย': 'depreciation',
                           'หนี้สูญและหนี้สงสัยจะสูญ': 'allowance_doubtful_accounts'}
        df.rename(columns=columnChangeMap, inplace=True)
        return df

    @staticmethod
    def adjust_quarterly_result(df, ticker):
        #TODO Fix bug in adjust quarterly eps: see 2S for example. Bug is from computing numbershare when eps, net_profit are ttm

        RETURN_COLUMN = 'net_profit'
        EPS_COLUMN = 'EPS'

        cols_subtract = ['revenue', 'expense', 'gross_profit', 'operating_profit',
                         'ebit', 'ebitda', 'net_profit', 'cash_flow_operation',
                         'cash_flow_investment', 'cash_flow_financing', 'depreciation',
                         'dividend']
        cols_divide = ['P/E']
        cols_multiply = ['ROA%', 'ROAA%', 'ROE%', 'ROAE%', 'ROCE%']

        all_cols = [EPS_COLUMN] + cols_subtract + cols_divide + cols_multiply
        for col in all_cols:
            if col not in df.columns:
                print('Warning: essential column ', col, ' is missing. Assigning NaN.')
                df[col] = np.NaN

        df.sort_index(inplace=True)
        df['number_of_shares'] = df[RETURN_COLUMN] / df[EPS_COLUMN]
        if 'DPS' in df.columns:
            df['dividend'] = df['DPS'] * df['number_of_shares']
        else:
            df['dividend'] = 0
        df['return_old'] = df[RETURN_COLUMN]

        # ad-hoc adjustment for some ticker due to weird time-sync from siamchart
        first_quarter_month = 3
        if ticker in ['AOT']:
            first_quarter_month = 12

        # coll
        for col in cols_subtract:
            if col in df.columns:
                df['tmp'] = df[col].diff()
                df.ix[df.month != first_quarter_month, col] = df.ix[df.month != first_quarter_month, 'tmp']
            else:
                print('Warning: essential column ', col, ' is missing. Assigning NaN.')
                df[col] = np.nan
        df['eps'] = df['net_profit'] / df['number_of_shares']
        df['dps'] = df['dividend'] / df['number_of_shares']
        del df['tmp']

        # correct pe ratio and related columns
        for col in cols_divide:
            if col in df.columns:
                df[col] = df[col] * df['return_old'] / df[RETURN_COLUMN] / 4
            else:
                df[col] = np.nan

        # correct roe,roa and related stuff
        for col in cols_multiply:
            if col in df.columns:
                df[col] = df[col] / df['return_old'] * df[RETURN_COLUMN] * 4
            else:
                df[col] = np.nan

        return df


def run_processing_data_job():
    #TODO change to absolute path
    #TODO fix date in filepath to latest

    ## Initialize app
    cred = credentials.Certificate('../../credential/dbsweb-secret.json')
    firebase_admin.initialize_app(cred, {
        'storageBucket': 'dbsweb-f2346.appspot.com'
    })
    path_dir = 'data/raw/siamchart/20181127/'
    output_dir = 'data/process/siamchart/financial/20181127/'
    local_tmp_file = 'tmp.csv'
    bucket = storage.bucket()
    for blob in bucket.list_blobs(prefix=path_dir):
        page_source = blob.download_as_string().decode('utf-8')
        ticker = str(blob.name.split('/')[-1])
        sp = SiamchartProcessor(page_source, ticker)
        df_financial = sp.get_financial_data(raw=False, adjust_quarterly=True)
        df_financial.to_csv(local_tmp_file)
        output_blob = bucket.blob(output_dir + ticker + '.csv')
        output_blob.upload_from_filename(local_tmp_file)
        print(ticker, ': Successfully upload file to cloud storage')
    # delete tmp file
    if os.path.isfile(local_tmp_file):
        os.remove(local_tmp_file)

if __name__ =='__main__':
    run_processing_data_job()









