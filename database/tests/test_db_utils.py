#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('../')

import pytest
import sqlite3
import pandas as pd

from db_utils import (create_table, insert_rows, read_table, delete_table,
                      tables_in_database)

spy = pd.read_csv('test_datasets/SPY.csv', index_col=0, header=0)
aapl = pd.read_csv('test_datasets/AAPL.csv', index_col=0, header=0)


def test_create_table():

    connection = sqlite3.connect(':memory:')

    # Normal usage: create table
    create_table(connection, 'SPY', database='eod', replace_existing=False)

    with connection:
        cursor = connection.cursor()

        # get table name
        table = cursor.execute(
            "SELECT name FROM sqlite_master WHERE TYPE='table'").fetchone()[0]

        assert('SPY' == table)

        # get table columns
        cursor.execute('SELECT * FROM SPY')
        columns = [description[0] for description in cursor.description]

        assert('Time' == columns[0])
        assert('Open' == columns[1])
        assert('High' == columns[2])
        assert('Low' == columns[3])
        assert('Close' == columns[4])
        assert('Adj_Close' == columns[5])
        assert('Volume' == columns[6])

    create_table(connection, 'AAPL', database='eod', replace_existing=False)

    with connection:
        cursor = connection.cursor()
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE TYPE='table'")
        tables = tables.fetchall()
        tables = [table[0] for table in tables]

        assert('SPY' == tables[0])
        assert('AAPL' == tables[1])
        assert(len(tables) == 2)

    # replace table, expect no error, just warning
    create_table(connection, 'AAPL', database='eod', replace_existing=True)


def test_delete_table():

    connection = sqlite3.connect(':memory:')

    # create table
    with connection:

        cursor = connection.cursor()

        query_string = 'CREATE TABLE {}(Time TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume REAL)'
        cursor.execute(query_string.format('SPY'))

    # delete table in db
    delete_table(connection, 'SPY')

    with connection:
        cursor = connection.cursor()
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE TYPE='table'")
        tables = tables.fetchall()
        tables = [table[0] for table in tables]

        assert(len(tables) == 0)

    # delete table not in db, expect no error
    delete_table(connection, 'ABC')


def test_tables_in_database():

    connection = sqlite3.connect(':memory:')

    # create table
    with connection:

        cursor = connection.cursor()

        query_string = 'CREATE TABLE {}(Time TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume REAL)'

        cursor.execute(query_string.format('SPY'))
        cursor.execute(query_string.format('AAPL'))

    tables = tables_in_database(connection)
    assert(len(tables) == 2)
    assert(tables[0] == 'SPY')
    assert(tables[1] == 'AAPL')


def test_insert_rows():

    connection = sqlite3.connect(':memory:')
    with connection:
        cursor = connection.cursor()

        query_string = 'CREATE TABLE {}(Time TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume REAL)'

        # create table
        cursor.execute(query_string.format('SPY'))

    # insert rows of spy dataframe
    insert_rows(connection, 'SPY', spy)

    with connection:

        cursor = connection.cursor()
        query_string = "SELECT * FROM SPY"

        data = cursor.execute(query_string).fetchall()

    index = 0
    for time, row in spy.iterrows():
        assert(data[index][0] == time)
        assert(data[index][1] == row['Open'])
        assert(data[index][2] == row['High'])
        assert(data[index][3] == row['Low'])
        assert(data[index][4] == row['Close'])
        assert(data[index][5] == row['Adj_Close'])
        assert(data[index][6] == row['Volume'])
        index += 1

    with connection:
        cursor = connection.cursor()

        query_string = 'CREATE TABLE {}(Time TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume REAL)'

        # create table
        cursor.execute(query_string.format('AAPL'))

    # insert data as list of tuples
    input_data = []
    for index, row in aapl.iterrows():
        input_data.append(
            (index,
             row['Open'],
             row['High'],
             row['Low'],
             row['Close'],
             row['Adj_Close'],
             row['Volume']
             )
        )

    insert_rows(connection, 'AAPL', input_data)

    with connection:

        query_string = "SELECT * FROM AAPL"

        data = cursor.execute(query_string).fetchall()

    index = 0
    for time, row in aapl.iterrows():
        assert(data[index][0] == time)
        assert(data[index][1] == row['Open'])
        assert(data[index][2] == row['High'])
        assert(data[index][3] == row['Low'])
        assert(data[index][4] == row['Close'])
        assert(data[index][5] == row['Adj_Close'])
        assert(data[index][6] == row['Volume'])
        index += 1

    with pytest.raises(sqlite3.OperationalError):
        insert_rows(connection, 'ABC', input_data)


def test_read_table():

    connection = sqlite3.connect(':memory:')

    test_spy = spy.copy()
    test_spy.index.name = 'Time'
    test_spy.rename(columns={'Adj_Close': 'Adj_Close'}, inplace=True)
    test_spy = test_spy.astype('float64')

    input_data = []
    for index, row in test_spy.iterrows():
        input_data.append(
            (index,
             row['Open'],
             row['High'],
             row['Low'],
             row['Close'],
             row['Adj_Close'],
             row['Volume']
             )
        )

    with connection:
        cursor = connection.cursor()

        query_string = 'CREATE TABLE {}(Time TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume REAL)'

        # create table
        cursor.execute(query_string.format('SPY'))

        query_string = "REPLACE INTO {}(Time, Open, High, Low, Close, Adj_Close, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)"

        # input data
        cursor.executemany(query_string.format('SPY'), input_data)

    result = read_table(connection, 'SPY')
    pd.testing.assert_frame_equal(test_spy, result)

    '''Test between times'''
    input_data = []
    for index, row in test_spy.loc['2019-01-01':'2019-01-03'].iterrows():
        input_data.append(
            (index,
             row['Open'],
             row['High'],
             row['Low'],
             row['Close'],
             row['Adj_Close'],
             row['Volume']
             )
        )

    result = read_table(connection, 'SPY',
                        start='2019-01-01', end='2019-01-03')
    pd.testing.assert_frame_equal(
        test_spy.loc['2019-01-01':'2019-01-03'], result)

    ''' Test end time'''
    input_data = []
    for index, row in test_spy.loc[:'2019-01-03'].iterrows():
        input_data.append(
            (index,
             row['Open'],
             row['High'],
             row['Low'],
             row['Close'],
             row['Adj_Close'],
             row['Volume']
             )
        )

    result = read_table(connection, 'SPY', end='2019-01-03')
    pd.testing.assert_frame_equal(test_spy.loc[:'2019-01-03'], result)

    ''' Test start time'''

    input_data = []
    for index, row in test_spy.loc['2019-01-01':].iterrows():
        input_data.append(
            (index,
             row['Open'],
             row['High'],
             row['Low'],
             row['Close'],
             row['Adj_Close'],
             row['Volume']
             )
        )

    result = read_table(connection, 'SPY', start='2019-01-01')
    pd.testing.assert_frame_equal(test_spy.loc['2019-01-01':], result)

    '''Test read only one column'''
    input_data = []
    for index, row in test_spy.iterrows():
        input_data.append(
            (index, row['Close'])
        )

    result = read_table(connection, 'SPY', kind='Close')
            
    pd.testing.assert_frame_equal(test_spy[['Close']], result)



