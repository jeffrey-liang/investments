#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../portfolio')

from securities import Security


def test_securities():

    sec = Security('SPY')
    assert(isinstance(sec, Security))
    assert(sec.ticker_name == 'SPY')
    assert(sec.in_portfolio == False)
    assert(sec.shares_in_portfolio == 0)
    assert(sec.historical_transactions == {})


