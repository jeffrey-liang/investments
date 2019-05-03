#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd

def create_table(connection, name, database='eod', replace_existing=False):
    '''
    Creates a table into database.

    Parameters:
    -----------
    connection: str
        

    '''

    with connection:
        cursor = connection.cursor()

        try:
            if database == 'eod':
                query_string = 'CREATE TABLE {}(Time TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Adj_Close REAL, Volume REAL)'
            cursor.execute(query_string.format(name))

        except sqlite3.OperationalError as e:
            # table already exists

            if replace_existing:
                cursor.execute('DROP TABLE IF EXISTS {}'.format(name))
                cursor.execute(query_string.format(name))
            else:
                print('Warning: {}.'.format(e))


def delete_table(connection, table):

    with connection:
        cursor = connection.cursor()

        cursor.execute('DROP TABLE IF EXISTS {}'.format(table))


def tables_in_database(connection):

    with connection:
        cursor = connection.cursor()

        query_string = "SELECT name FROM sqlite_master WHERE TYPE='table'"
        tables = cursor.execute(query_string)
        tables = tables.fetchall()
        tables = [table[0] for table in tables]

    return tables


def insert_rows(connection, table, data, database='eod'):

    if isinstance(data, pd.DataFrame):
        data_list = []
        for index, row in data.iterrows():
            data_list.append(
                (index,
                 row['Open'],
                 row['High'],
                 row['Low'],
                 row['Close'],
                 row['Adj_Close'],
                 row['Volume']
                 )
            )
        data = data_list

    with connection:

        cursor = connection.cursor()

        if database == 'eod' or 'test_memory':
            query_string = "REPLACE INTO {}(Time, Open, High, Low, Close, Adj_Close, Volume) VALUES(?, ?, ?, ?, ?, ?, ?)"

        cursor.executemany(query_string.format(table), data)


def read_table(connection, table, database='eod', start=None, end=None,
               kind=None):

    data = None

    with connection:
        cursor = connection.cursor()

        if database == 'eod' or 'test_eod_db':

            if kind is None:
                kind = '*'
            else:
                kind = 'Time,{}'.format(kind)

            if start is None and end is None:
                query_string = "SELECT {} FROM {}"
                query_string = query_string.format(kind, table)

            elif start is None and end is not None:
                query_string = "SELECT {} FROM {} WHERE Time <= '{}'"
                query_string = query_string.format(kind, table, end)

            elif start is not None and end is None:
                query_string = "SELECT {} FROM {} WHERE Time >= '{}'"
                query_string = query_string.format(kind, table, start)

            elif start is not None and end is not None:
                query_string = "SELECT {} FROM {} WHERE Time BETWEEN '{}' AND '{}'"
                query_string = query_string.format(kind, table, start, end)

            data = pd.read_sql_query(query_string, connection,
                                     index_col='Time')

    return data
