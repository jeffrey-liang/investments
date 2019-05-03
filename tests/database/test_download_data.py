#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('../../database')

import arrow
from download_data import get_from_yahoo
from pandas.testing import assert_frame_equal
import pandas as pd
import time

spy = pd.read_csv('test_datasets/SPY.csv', index_col=0, header=0)
spy.index.name = 'Time'
spy.rename(columns={'Adj_Close': 'Adj Close'}, inplace=True)


aapl = pd.read_csv('test_datasets/AAPL.csv', index_col=0, header=0)
aapl.index.name = 'Time'
aapl.rename(columns={'Adj_Close': 'Adj Close'}, inplace=True)

def test_single_download():

    data = get_from_yahoo('SPY', '2019-01-01', '2019-01-07')
    assert_frame_equal(spy, data['SPY'])

    time.sleep(2)

def test_multiple_download():
    data = get_from_yahoo(['SPY', 'AAPL'], '2019-01-01', '2019-01-07')

    assert_frame_equal(spy, data['SPY'])
    assert_frame_equal(aapl, data['AAPL'])


# TODO Non existent ticker tests

