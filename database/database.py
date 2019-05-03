#!/usr/bin/env python
# -*- coding: utf-8 -*-

from db_utils import (create_table, insert_rows, read_table, 
        tables_in_database)

from download_data import get_from_yahoo
import sqlite3
from pandas.io.sql import DatabaseError


def from_database(tickers, database='eod', kind=None, start=None,
                  end=None):
    '''
    Gets ticker data from database.

    Parameters:
    -----------
    tickers: list or str
        Tickers to fetch.

    database: str
        Which database to query. 

        options: 'eod': end of day stock data

    kind: str
        Column of end of day data.


    '''

    if not isinstance(tickers, list):
        tickers = [tickers]

    data = {}

    databases = {
            'eod': 'data/eod_stock.sqlite3', 
            'test_memory': ':memory:',
            'test_eod_db': 'test_datasets/test_eod_db.sqlite3',
            'test_update_db': 'test_datasets/test_update_db.sqlite3'
            }

    connection = sqlite3.connect(databases[database])

    with connection:

        for ticker in tickers:

            try:

                data[ticker] = read_table(connection=connection, table=ticker,
                                          database=database, start=start,
                                          end=end, kind=kind)

            #except sqlite3.OperationalError:
            except DatabaseError:
                # ticker not in db
                downloaded_data = get_from_yahoo(ticker, start=start,
                                                 end=end)

                downloaded_data[ticker].rename(
                        columns={'Adj Close': 'Adj_Close'}, inplace=True)

                create_table(connection=connection, name=ticker)

                insert_rows(connection=connection, table=ticker,
                            data=downloaded_data[ticker], database=database)


                data[ticker] = read_table(connection=connection, table=ticker,
                                          database=database, start=start,
                                          end=end, kind=kind)
    return data

def tickers_in_database(database):

    databases = {
            'eod': 'data/eod_stock.sqlite3', 
            'test_memory': ':memory:',
            'test_eod_db': 'test_datasets/test_eod_db.sqlite3',
            'test_update_db': 'test_datasets/test_update_db.sqlite3'
            }

    connection = sqlite3.connect(databases[database])

    with connection:

        tickers = tables_in_database(connection)

    return tickers

def _update_database(tickers, database='eod'):

    if not isinstance(tickers, list):
        tickers = [tickers]

    databases = {
            'eod': 'data/eod_stock.sqlite3', 
            'test_memory': ':memory:',
            'test_eod_db': 'test_datasets/test_eod_db.sqlite3',
            'test_update_db': 'test_datasets/test_update_db.sqlite3'
            }

    connection = sqlite3.connect(databases[database])

    cursor = connection.cursor()

    with connection:

        downloaded_data = {}

        for ticker in tickers:

            query_string = "SELECT Time FROM {} LIMIT 1"
            start_time = cursor.execute(query_string.format(ticker)).fetchone()[0]

            query_string = "SELECT Time FROM {} ORDER BY TIME DESC LIMIT 1"
            end_time = cursor.execute(query_string.format(ticker)).fetchone()[0]

            downloaded_data = get_from_yahoo(ticker, start=end_time)

            downloaded_data[ticker].rename(
                        columns={'Adj Close': 'Adj_Close'}, inplace=True)

            insert_rows(connection=connection, table=ticker,
                            data=downloaded_data[ticker], database=database)


            
