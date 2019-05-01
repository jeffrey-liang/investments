#!/usr/bin/env python
# -*- coding: utf-8 -*-

from db_utils import create_table, insert_rows, read_table
from download_data import get_from_yahoo
import sqlite3


def from_database(tickers, database='eod', kind=None, start=None,
                  end=None):

    if not isinstance(tickers, list):
        tickers = [tickers]

    data = {}

    databases = {'eod': 'data/eod_stock.sqlite3'}

    connection = sqlite3.connect(databases[database])

    with connection:

        for ticker in tickers:

            try:
                data[ticker] = read_table(connection=connection, table=ticker,
                                          database=database, start=start,
                                          end=end, kind=kind)

                print(data[ticker])
            except sqlite3.OperationalError:
                # ticker not in db
                downloaded_data = get_from_yahoo(ticker, start=start,
                                                 end=end)

                create_table(connecton=connection, tickers=ticker)

                insert_rows(connection=connection, tickers=ticker,
                            data=downloaded_data, database=database)

                data[ticker] = read_table(connection=connection, table=ticker,
                                          database=database, start=start,
                                          end=end, kind=kind)

    return data


def _update_database(tickers, database='eod'):

    if not isinstance(tickers, list):
        tickers = [tickers]

    databases = {'eod': 'data/eod_stock.db', 'test': 'data/test.db'}

    connection = sqlite3.connect(databases[database])

    cursor = connection.cursor()

    with connection:

        for ticker in tickers:
            pass
