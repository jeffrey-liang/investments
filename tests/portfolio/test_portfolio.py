#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.append('../../portfolio')
sys.path.append('../../database')

import pickle

from database import from_database
from portfolio import (Portfolio, PortfolioHistory, load, save,
        TradeTransaction, CashTransaction)



def test_portfolio():

    # add order
    portfolio = Portfolio()

    portfolio.add_trade('SPY', 'BUY', 10, 100, 0, '2019-01-01') 

    assert(len(portfolio.active_tickers) == 1)
    assert(len(portfolio.transactions) == 1)

    assert(portfolio.transactions[0].ticker == 'SPY')
    assert(portfolio.transactions[0].action == 'BUY')
    assert(portfolio.transactions[0].price == 10)
    assert(portfolio.transactions[0].shares == 100)
    assert(portfolio.transactions[0].fees == 0)
    assert(portfolio.transactions[0].time == '2019-01-01')

    assert(portfolio.positions['SPY'].average_price == 10)
    assert(portfolio.positions['SPY'].shares == 100)

    # add another order, same ticker
    portfolio.add_trade('SPY', 'BUY', 13, 50, 0, '2019-01-02') 

    assert(len(portfolio.active_tickers) == 1)
    assert(len(portfolio.transactions) == 2)

    assert(portfolio.positions['SPY'].average_price == 11)
    assert(portfolio.positions['SPY'].shares == 150)

    # add another order, different ticker
    portfolio.add_trade('AAPL', 'BUY', 15, 50, 0, '2019-01-02') 

    assert(len(portfolio.active_tickers) == 2)
    assert(len(portfolio.transactions) == 3)

    assert(portfolio.positions['AAPL'].average_price == 15)
    assert(portfolio.positions['AAPL'].shares == 50)

    # add another order, sell all shares of one ticker
    portfolio.add_trade('AAPL', 'SELL', 15, 50, 0, '2019-01-02') 

    assert(len(portfolio.active_tickers) == 1)
    assert('AAPL' not in portfolio.active_tickers)
    assert(len(portfolio.transactions) == 4)

    portfolio.add_trade('SPY', 'SELL', 13, 50, 0, '2019-01-02') 

    assert(len(portfolio.active_tickers) == 1)
    assert('SPY' in portfolio.active_tickers)
    assert(len(portfolio.transactions) == 5)

    assert(portfolio.positions['SPY'].shares == 100)

def test_save_load():

    portfolio = Portfolio()
    portfolio.add_trade('SPY', 'BUY', 10, 100, 0, '2019-01-01') 

    save(portfolio, 'test_portfolio.pkl')
    loaded_portfolio = load('test_portfolio.pkl')

    assert(len(loaded_portfolio.active_tickers) == 1)
    assert(len(loaded_portfolio.transactions) == 1)

    assert(loaded_portfolio.transactions[0].ticker == 'SPY')
    assert(loaded_portfolio.transactions[0].action == 'BUY')
    assert(loaded_portfolio.transactions[0].price == 10)
    assert(loaded_portfolio.transactions[0].shares == 100)
    assert(loaded_portfolio.transactions[0].fees == 0)
    assert(loaded_portfolio.transactions[0].time == '2019-01-01')

    assert(loaded_portfolio.positions['SPY'].average_price == 10)
    assert(loaded_portfolio.positions['SPY'].shares == 100)

def test_portfolio_history():

    test_transactions = [
            CashTransaction(10000, '2019-01-01'),
            TradeTransaction('SPY', 'BUY', 200, 10, 0, '2019-01-03'),
            TradeTransaction('AAPL', 'BUY', 100, 10, 0, '2019-01-04'),
            TradeTransaction('AAPL', 'BUY', 100, 10, 0, '2019-01-05'),
            TradeTransaction('SPY', 'BUY', 100, 10, 0, '2019-01-05'),
            TradeTransaction('AAPL', 'SELL', 100, 10, 0, '2019-01-07'),
            TradeTransaction('AAPL', 'SELL', 100, 10, 0, '2019-01-08')
            ]

    port_history = PortfolioHistory()
    port_history.load_prior_history(test_transactions)
    #print(port_history.shares)
    #print(port_history.activity)


test_portfolio_history()

    




