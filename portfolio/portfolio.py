#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../database')

from database import from_database

from collections import namedtuple
import pickle
import pandas as pd
#import arrow
import datetime as dt


TradeTransaction = namedtuple('TradeTransaction', ['ticker', 'action', 'price', 
                                        'shares', 'fees', 'time'])

CashTransaction = namedtuple('CashTransaction', ['amount', 'time'])

Position = namedtuple('Position', ['ticker', 'average_price', 'shares'])

Equity_ledger = namedtuple('Equity_ledger', ['time', 'value'])

def load(path):
    with open(path, 'rb') as f:
        return pickle.load(f)
    
def save(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f)

class Portfolio(object):

    def __init__(self, initial_cash=0):
        self.active_tickers = []
        self.transactions = [] # stores all transactions
        self.positions = {}

        self.portfolio_start_date = None
        self.initial_cash = 0
        self.portfolio_initial_equity = self.initial_cash

        self.total_equity = self.initial_cash
        self.cash = self.initial_cash
        self.market_value = 0
        self.last_update = None

    def add_trade(self, ticker, action, price, shares, fees, time):

        transaction = TradeTransaction(ticker, action, price, shares, fees, time)
        self.transactions.append(transaction)

        if ticker not in self.active_tickers:
            self.active_tickers.append(ticker)

        self._update_positions(transaction)

    def add_cash_transaction(self, amount, time):
        pass

    def _update_positions(self, transaction):

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
                    ticker=transaction.ticker,
                    shares=transaction.shares,
                    average_price=transaction.price)
        
    def _update_account(self):
        pass

    def summary(self):
        pass

    def returns(self, ticker=None):

        if ticker is not None:
            pass
    
    def equity_curve(self):
        pass


class PortfolioHistory(object):

    def __init__(self, shares=None, cash=None, total_equity=None, 
            tickers=None, activity=None):

        # Keeps track of shares in portfolio for each ticker at all times
        if shares is None:
            self.shares = {}
        else:
            self.shares = self.shares

        # Keeps track of cash in portfolio at all times
        if cash is None:
            self.cash = {}
        else:
            self.cash = cash

        # Keeps track of total_equity in portfolio at all times
        if total_equity is None:
            self.total_equity = {}
        else:
            self.total_equity = total_equity

        # Keeps track of shares for each ticker at all times
        if tickers is None:
            self.tickers = {}
        else:
            self.tickers = tickers

        # Keeps track of all transactions at each time
        if activity is None:
            self.activity = {}
        else:
            self.activity = activity


        #self.portfolio_start_time = arrow.now().datetime
        self.portfolio_start_time = pd.Timestamp.now()

        self.tickers_in_portfolio_history = []

    def load_prior_history(self, transactions):

        for transaction in transactions:
            if pd.Timestamp(transaction.time) < self.portfolio_start_time:
                self.portfolio_start_time = pd.Timestamp(transaction.time)

            if isinstance(transaction, TradeTransaction):

                if transaction.ticker not in self.tickers_in_portfolio_history:
                    self.tickers_in_portfolio_history.append(transaction.ticker)
            try:
                self.activity[transaction.time].append(transaction)

            except KeyError:
                self.activity[transaction.time] = [transaction]


        date_range = pd.date_range(start=self.portfolio_start_time,
                                end=pd.Timestamp('2019-02-01'))
                                #end=pd.Timestamp.now().date())

        for time in date_range:
            previous = time - dt.timedelta(days=1)
            previous = previous.strftime('%Y-%m-%d')
            time = time.strftime('%Y-%m-%d')

            self.shares[time] = {}
            self.cash[time] = {}
            self.total_equity[time] = {}

            try:
                transactions = self.activity[time] 

                for transaction in transactions:

                    if isinstance(transaction, CashTransaction):
                        cash = self.cash[previous]
                        self.cash[time] = (cash + transaction.amount)

                    elif isinstance(transaction, TradeTransaction):
                        shares = self.shares[previous][transaction.ticker]

                        if transaction.action == 'BUY':
                            self.shares[time][transaction.ticker] = (
                                    shares + transaction.shares
                                    )
                        elif transaction.action == 'SELL':
                            self.shares[time][transaction.ticker] = (
                                    shares - transaction.shares
                                    )

                    #equty = self.total_equity[previous]


            except KeyError:

                if isinstance(transaction, CashTransaction):
                    self.cash[time] = self.cash[previous]

                elif isinstance(transaction, TradeTransaction):
                    self.shares[time] = self.shares[previous]




                        


            
