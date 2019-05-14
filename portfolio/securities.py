#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
from dataclasses import dataclass
from typing import Any

@dataclass
class Security(object):

    ticker_name: str = None
    in_portfolio: bool = False
    shares_in_portfolio: int = 0
    historical_transactions: Any = None
'''


class Security(object):

    def __init__(self, ticker_name, in_portfolio=False,
            shares_in_portfolio=0, active_since=None, 
            historical_transactions=None):

        self.ticker_name = ticker_name
        self.in_portfolio = in_portfolio
        self.shares_in_portfolio = shares_in_portfolio
        self.active_since = None

        if historical_transactions is None:
            self.historical_transactions = {}
        else:
            self.historical_transactions = historical_transactions



