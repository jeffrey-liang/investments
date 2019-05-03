#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('../../database')

from database import (from_database, tickers_in_database, _update_database)
from db_utils import (create_table, insert_rows, read_table, 
                        tables_in_database, delete_table)

import pytest
import pandas as pd
import sqlite3

spy = pd.read_csv('test_datasets/SPY.csv', index_col=0)
spy.index.name = 'Time'
spy = spy.astype('float64')

aapl = pd.read_csv('test_datasets/AAPL.csv', index_col=0)
aapl.index.name = 'Time'
aapl = aapl.astype('float64')

msft = pd.read_csv('test_datasets/MSFT.csv', index_col=0)
msft.index.name = 'Time'
msft = msft.astype('float64')

def test_tickers_in_database():
    tickers = tickers_in_database(database='test_eod_db')

    assert(len(tickers) == 2)
    assert(tickers[0] == 'SPY')
    assert(tickers[1] == 'AAPL')

def test_from_database():

    data = from_database('SPY', database='test_eod_db')
    pd.testing.assert_frame_equal(spy, data['SPY'])

    data = from_database('SPY', database='test_eod_db', kind='Close')
    pd.testing.assert_frame_equal(spy[['Close']], data['SPY'])

    data = from_database('SPY', database='test_eod_db', start='2019-01-02')
    pd.testing.assert_frame_equal(spy.loc['2019-01-02':], data['SPY'])

    data = from_database('SPY', database='test_eod_db', end='2019-01-05')
    pd.testing.assert_frame_equal(spy.loc[:'2019-01-05'], data['SPY'])

    data = from_database('SPY', database='test_eod_db', start='2019-01-02',
            end='2019-01-05')
    pd.testing.assert_frame_equal(spy.loc['2019-01-02':'2019-01-05'], 
            data['SPY'])

    pd.testing.assert_frame_equal(spy.loc['2019-01-02':'2019-01-05'], 
            data['SPY'])

    data = from_database(['SPY', 'AAPL'], database='test_eod_db')
    pd.testing.assert_frame_equal(spy, data['SPY'])
    pd.testing.assert_frame_equal(aapl, data['AAPL'])

    # ticker not in db
    data = from_database('MSFT', database='test_memory', start='2019-01-01',
        end='2019-01-05')

    pd.testing.assert_frame_equal(msft.loc['2019-01-01':'2019-01-05'], 
            data['MSFT'])


def test_update_database():
    '''

    ticker = 'SPY'
    connection = sqlite3.connect('test_datasets/test_update_db.sqlite3')

    create_table(connection, 'SPY', replace_existing=True)
    insert_rows(connection, 'SPY', spy)

    _update_database(tickers, database='test_update_db')

    data = from_database('SPY', database='test_update_db')

    for ticker in tickers:
        delete_table(connection, ticker)
    '''
    pass
