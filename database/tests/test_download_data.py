#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('../')

import arrow
from download_data import get_from_yahoo
from pandas.testing import assert_frame_equal
import pandas as pd
import time

start_period = 1546300800  # 2019-01-01
end_period = 1546905600  # 2019-01-08

spy = pd.read_csv('test_datasets/SPY.csv', index_col=0, header=0)
spy.index.name = 'Time'
aapl = pd.read_csv('test_datasets/AAPL.csv', index_col=0, header=0)
aapl.index.name = 'Time'


def test_single_download():

    data = get_from_yahoo('SPY', start_period, end_period)

    assert_frame_equal(spy, data['SPY'])

def test_multiple_download():
    data = get_from_yahoo(['SPY', 'AAPL'], start_period, end_period)

    assert_frame_equal(spy, data['SPY'])
    assert_frame_equal(aapl, data['AAPL'])

test_single_download()
# TODO Non existent ticker tests
