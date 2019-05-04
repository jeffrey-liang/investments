#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../database')

from database import from_database

from collections import namedtuple
import pickle

Transaction = namedtuple('Transaction', ['ticker', 'action', 'price', 
                                        'shares', 'fees', 'time'])

Position = namedtuple('Position', ['average_price', 'shares'])

def load(path):
    with open(path, 'rb') as f:
        return pickle.load(f)
    

def save(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f)

class Portfolio(object):

    def __init__(self):
        self.active_tickers = []
        self.transactions = []
        self.positions = {}

    def add_order(self, ticker, action, price, shares, fees, time):

        order = Transaction(ticker, action, price, shares, fees, time)
        self.transactions.append(order)

        if ticker not in self.active_tickers:
            self.active_tickers.append(ticker)

        self._update(order)

    def _update(self, transaction):

        try:
            position = self.positions[transaction.ticker]

            if transaction.action == 'BUY':

                new_shares = position.shares + transaction.shares
                average_price = (
                        (position.shares * position.average_price
                        + transaction.shares * transaction.price)
                        / new_shares)


                position = position._replace(
                        shares = new_shares, average_price=average_price
                        )

                self.positions[transaction.ticker] = position

            elif transaction.action == 'SELL':

                new_shares = position.shares - transaction.shares

                position = position._replace(shares=new_shares)

                if new_shares != 0:
                    self.positions[transaction.ticker] = position
                else:
                    del self.positions[transaction.ticker]
                    self.active_tickers.remove(transaction.ticker)


        except KeyError:
            # new ticker
            self.positions[transaction.ticker] = Position(
                    shares=transaction.shares,
                    average_price=transaction.price)
        

    def summary(self):
        pass

    def returns(self):
        pass
    
    def equity_curve(self):
        pass

    def portfolio_state(self):
        pass

