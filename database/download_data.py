#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import csv
import arrow
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time


def get_from_yahoo(tickers, start=None, end=None, sleep=1):
    '''
    Downloads end of day stock data from yahoo finance.

    Parameters
    ----------
    tickers: str or list
        Tickers to download.

    start: str
        Start time .

    end: str
        End time.

    sleep: int
        Time between data downloads to wait.

    Returns
    -------
    data: dict
        Returns the downloaded data where the key is the the ticker, 
        and the item is a pandas dataframe.


    Note: Currently no error checking for invalid tickers.

    '''
    # dict to store the downloaed data
    data = {}

    # if tickers is single string, convert it to list
    if not isinstance(tickers, list):
        tickers = [tickers]

    # Format time into unix time
    if start is None:
        start = arrow.get(0).timestamp  # 1970-01-01
    else:
        start = arrow.get(start).timestamp

    if end is None:
        end = arrow.utcnow().timestamp
    else:
        end = arrow.get(end).timestamp

    # Start Session
    session = requests.session()

    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'

    headers = {'User-Agent': user_agent}

    # get crumb
    url = 'https://ca.finance.yahoo.com/quote/{0}/history?p={0}'

    r = session.get(url.format(tickers[0]), headers=headers)

    # is successfully obtain crumb, get data
    if r.status_code == 200:

        bs_obj = BeautifulSoup(r.text, 'lxml')
        app_main = bs_obj.find('script', text=re.compile('root.App.main')).text
        crumb = re.search('"CrumbStore":{"crumb":"(.*?)"}', app_main).group(1)

        cookies = dict(session.cookies.items())

        data_url = 'https://query1.finance.yahoo.com/v7/finance/download/{}?period1={}&period2={}&interval=1d&events=history&crumb={}'

        for ticker in tickers:

            r = session.get(data_url.format(ticker, start, end, crumb),
                            cookies=cookies, headers=headers)

            if r.status_code == 200:

                ticker_data = r.text
                ticker_data = pd.read_csv(pd.compat.StringIO(ticker_data),
                                          header=0, index_col=0)
                ticker_data.index.names = ['Time']

                data[ticker] = ticker_data

                # wait to not hit server too often
                time.sleep(sleep)

            else:
                print('Failed to retrieve data for {}. Status code: {}'.format(
                    ticker, r.status_code))

    else:
        print('Failed to retrieve data. Status code: {}'.format(r.status_code))


    return data
