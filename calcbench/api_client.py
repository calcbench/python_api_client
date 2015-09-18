'''
Created on Mar 14, 2015

@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
'''
from __future__ import print_function
import os
import requests
import json
import pandas as pd

_CALCBENCH_USER_NAME = os.environ.get("CALCBENCH_USERNAME")
_CALCBENCH_PASSWORD = os.environ.get("CALCBENCH_PASSWORD")
_CALCBENCH_API_URL_BASE = "https://www.calcbench.com/api/{0}"
_CALCBENCH_LOGON_URL = 'https://www.calcbench.com/account/LogOnAjax'
_SSL_VERIFY = True

_SESSION = None

def _calcbench_session():  
    global _SESSION
    if not _SESSION:
        if not (_CALCBENCH_PASSWORD and _CALCBENCH_USER_NAME):
            raise ValueError("No credentials supplied, either call set_credentials or set \
CALCBENCH_USERNAME and CALCBENCH_PASSWORD environment variables.")
        _session = requests.Session()
        r = _session.post(_CALCBENCH_LOGON_URL, 
                  {'email' : _CALCBENCH_USER_NAME, 
                   'strng' : _CALCBENCH_PASSWORD, 
                   'rememberMe' : 'true'},
                  verify=_SSL_VERIFY)
        r.raise_for_status()
        if r.text != 'true':
            raise ValueError('Incorrect Credentials, use the email and password you use to login to Calcbench.')
        else:
            _SESSION = _session
    return _SESSION

def set_credentials(cb_username, cb_password):
    '''Set your calcbench credentials.
    
    username is the email address you use to login to calcbench.com.
    '''
    global _CALCBENCH_USER_NAME
    global _CALCBENCH_PASSWORD
    _CALCBENCH_USER_NAME = cb_username
    _CALCBENCH_PASSWORD = cb_password
    _calcbench_session() #Make sure credentials work.

def normalized_data(company_identifiers, 
                    metrics, 
                    start_year, 
                    start_period,
                    end_year,
                    end_period):
    '''Normalized data.
    
    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.
    
    Args:
        company_identifiers: a sequence of tickers, eg ['msft', 'goog', 'appl']
        metrics: a sequence of metrics, eg. ['revenue', 'accountsreceivable']
        start_year: first year of data to get
        start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        end_year: last year of data to get
        end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
    
    Returns:
        A Pandas.Dataframe with the periods as the index and columns indexed by metric and ticker
    '''

    data = normalized_data_raw(company_identifiers, metrics, start_year, start_period, end_year, end_period)
    if not data:
        return pd.DataFrame()
    quarterly = start_period and end_period
    if quarterly:
        build_period = _build_quarter_period
    else:
        build_period = _build_annual_period
   
    for d in data:                          
        d['period'] = build_period(d)
        d['ticker'] = d['ticker'].upper()

        
    data = pd.DataFrame(data)
    data.set_index(keys=['ticker', 'metric', 'period'],
                   inplace=True)
    data = data.unstack('metric')['value']
    data = data.unstack('ticker')
    data = data[metrics]
    return data


def normalized_data_raw(company_identifiers, 
                    metrics, 
                    start_year, 
                    start_period,
                    end_year,
                    end_period):
    '''
    Normalized data.
    
    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.
    
    Args:
        company_identifiers: a sequence of tickers, eg ['msft', 'goog', 'appl']
        metrics: a sequence of metrics, eg. ['revenue', 'accountsreceivable']
        start_year: first year of data to get
        start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        end_year: last year of data to get
        end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        
    Returns:
        A list of dictionaries with keys ['ticker', 'calendar_year', 'calendar_period', 'metric', 'value'].
        
        For example
            [
                {
                    "ticker": "MSFT",
                    "calendar_year": 2010,
                    "calendar_period": 1,
                    "metric": "revenue",
                    "value": 14503000000
                },
                {
                    "ticker": "MSFT",
                    "calendar_year": 2010,
                    "calendar_period": 2,
                    "metric": "revenue",
                    "value": 16039000000
                },
            ]
    '''
    
        
    url = _CALCBENCH_API_URL_BASE.format("/NormalizedValues")
    payload = {"start_year" : start_year,
           'start_period' : start_period,
           'end_year' : end_year,
           'end_period' : end_period,
           'company_identifiers' : company_identifiers,
           'metrics' : metrics,
           }
    response = _calcbench_session().post(
                                    url,
                                    data=json.dumps(payload), 
                                    headers={'content-type' : 'application/json'},
                                    verify=_SSL_VERIFY
                                    )
    response.raise_for_status()
    data = response.json()
    return data

def _build_quarter_period(data_point):
    return pd.Period(year=data_point.pop('calendar_year'),
                     quarter=data_point.pop('calendar_period'),
                     freq='q')

def _build_annual_period(data_point):
    data_point.pop('calendar_period')
    return pd.Period(year=data_point.pop('calendar_year'), freq='a')

    
def tickers(SIC_codes=[], index=None):
    '''Return a list of tickers in the peer-group'''
    companies = _companies(SIC_codes, index)
    tickers = [co['ticker'] for co in companies]
    return tickers

def companies(SIC_codes=[], index=None):
    '''Return a DataFrame with company details'''
    companies = _companies(SIC_codes, index)
    return pd.DataFrame(companies)
    
def _companies(SIC_codes, index):
    if not(SIC_codes or index):
        raise ValueError('Must supply SIC_code or index')    
    if index:
        if index not in ("SP500", "DJIA"):
            raise ValueError("index must be either 'SP500' or 'DJIA'")
        query = "index={0}".format(index)
    else:
        query = '&'.join("SICCodes={0}".format(SIC_code) for SIC_code in SIC_codes)
    url = _CALCBENCH_API_URL_BASE.format("companies?" + query)
    r = _calcbench_session().get(url, verify=_SSL_VERIFY)
    r.raise_for_status()
    return r.json()
    
if __name__ == '__main__':
    dow_tickers = tickers(index="DJIA")
    metrics = ['netincome', 'paymentsofdividends', "PaymentsForRepurchaseOfCommonStock", "ProceedsFromIssuanceOfCommonStock"]
    data = normalized_data(company_identifiers=dow_tickers, metrics=metrics, start_year=2009, start_period=0, end_year=2015, end_period=0)
    print(companies(SIC_codes=[7372, 'asdf']))
    data = normalized_data(company_identifiers=['ibm', 'msft'], 
                          metrics=['revenue', 'assets', ],
                          start_year=2010,
                          start_period=1,
                          end_year=2014,
                          end_period=4)
    print(data)
 