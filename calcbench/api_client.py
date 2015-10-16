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


def normalized_dataframe(company_identifiers, 
                    metrics, 
                    start_year, 
                    start_period,
                    end_year,
                    end_period):
    '''Normalized data.
    
    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.
    
    Args:
        company_identifiers: a sequence of tickers (or CIK codes), eg ['msft', 'goog', 'appl']
        metrics: a sequence of metrics, see the full list @ https://www.calcbench.com/home/excel#availableMetrics eg. ['revenue', 'accountsreceivable']
        start_year: first year of data
        start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        end_year: last year of data
        end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
    Returns:
        A Pandas.Dataframe with the periods as the index and columns indexed by metric and ticker
    '''

    data = normalized_raw(company_identifiers, metrics, start_year, start_period, end_year, end_period)
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

normalized_data = normalized_dataframe # used to call it normalized_data.

def normalized_raw(company_identifiers, 
                    metrics, 
                    start_year, 
                    start_period,
                    end_year,
                    end_period):
    '''
    Normalized data.
    
    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.
    
    Args:
        company_identifiers: a sequence of tickers (or CIK codes), eg ['msft', 'goog', 'appl']
        metrics: a sequence of metrics, see the full list @ https://www.calcbench.com/home/excel#availableMetrics eg. ['revenue', 'accountsreceivable']
        start_year: first year of data
        start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        end_year: last year of data
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


def as_reported_raw(company_identifier, 
                     statement_type, 
                     period_type='annual', 
                     all_periods=False, 
                     descending_dates=False):
    '''
    As-Reported Data.
    
    Get statements as reported by the filing company
    
    Args:
        company_identifier: a ticker or a CIK code, eg 'msft'
        statement_type: one of ('income', 'balance', 'cash', 'change-in-equity', 'comprehensive-income')
        period_type: either 'annual' or 'quarterly'
        all_periods: get all history or only the last four, True or False.
        descending_dates: return columns in oldest -> newest order.
        
    Returns:
        An object with columns and line items lists.  The columns have fiscal_period, period_start, period_end and instant values.
        The line items have label, local_name (the XBRL tag name), tree_depth (the indent amount), is_subtotal (whether or not the line item is computed from above metrics) and facts.
        The facts are in the same order as the columns and have fact_ids (an internal Calcbench ID), unit_of_measure (USD etc), effective_value (the reported value), and format_type.
         
         Example:
             {
                "entity_name": "Microsoft Corp",
                "name": "INCOME STATEMENTS",
                "period_type": 2,
                "columns": [
                            {"fiscal_period": "Y 2008",
                            "period_start": "2007-07-01",
                            "period_end": "2008-06-30",
                            "instant": false
                        },...],
                "line_items" : [
                            {"label": "Revenue",
                            "local_name": "SalesRevenueNet",
                            "tree_depth": 3,
                            "is_subtotal": false,
                            "facts": [
                                {
                                    "fact_id": 5849672,
                                    "unit_of_measure": "USD",
                                    "effective_value": 60420000000,
                                    "format_type": "currency",
                                    "text_fact_id": 5849351
                                },...]}
                    ...]
                }        
    '''
    url = _CALCBENCH_API_URL_BASE.format("asReported")
    payload = {'companyIdentifier' : company_identifier,
               'statementType' : statement_type,
               'periodType' : period_type,
               'allPeriods' : all_periods,
               'descendingDates' : descending_dates}
    response = _calcbench_session().get(url, 
                                        params=payload, 
                                        headers={'content-type' : 'application/json'}, 
                                        verify=_SSL_VERIFY)
    response.raise_for_status()
    data = response.json()
    return data
    
    
def breakouts_raw(company_identifiers=None, metrics=[], start_year=None, 
                 start_period=None, end_year=None, end_period=None, period_type='annual'):
    '''
    Breakouts
    
    Get breakouts/segments, see https://www.calcbench.com/breakout.
    
    Args:
        company_identifiers : list of tickers or CIK codes
        metrics : list of breakouts, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName".
        start_year: first year of data to get
        start_period: first period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
        end_year: last year of data to get
        end_period: last period of data to get. 0 for annual data, 1, 2, 3, 4 for quarterly data.
        period_type: quarterly or annual, only applicable when other period data not supplied.
        
    Returns:
        A list of breakout points.  The points correspond to the lines @ https://www.calcbench.com/breakout.  For each requested metric there will \
        be a the formatted value and the unformatted value denote bya  _effvalue suffix.  The label is the dimension label associated with the values.
        
        
    '''
    if len(metrics) == 0:
        raise(ValueError("Need to supply at least one breakout."))
    if period_type not in ('annual', 'quarterly'):
        raise(ValueError("period_type must be in ('annual', 'quarterly')"))
    payload = {'companiesParameters' : {'entireUniverse' : len(company_identifiers) == 0,
                                       'companyIdentifiers' : company_identifiers},
              'periodParameters' : {'year' : start_year,
                                    'period' : start_period,
                                    'endYear' : end_year,
                                    'endPeriod' : end_period,
                                    'periodType' : period_type},
              'pageParameters' : {'metrics' : metrics}}
    url = _CALCBENCH_API_URL_BASE.format('breakouts')
    response = _calcbench_session().post(url,
                                         data=json.dumps(payload),
                                         headers={'content-type' : 'application/json'},
                                         verify=_SSL_VERIFY)
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

    
def tickers(SIC_codes=[], index=None, all_companies=False):
    '''Return a list of tickers in the peer-group'''
    companies = _companies(SIC_codes, index, all_companies)
    tickers = [co['ticker'] for co in companies]
    return tickers

def companies(SIC_codes=[], index=None, all_companies=False):
    '''Return a DataFrame with company details'''
    companies = _companies(SIC_codes, index, all_companies)
    return pd.DataFrame(companies)
    
def _companies(SIC_codes, index, all_companies=False):
    if not(SIC_codes or index or all_companies):
        raise ValueError('Must supply SIC_code or index')
    query = "universe=true"  
    if index:
        if index not in ("SP500", "DJIA"):
            raise ValueError("index must be either 'SP500' or 'DJIA'")
        query = "index={0}".format(index)
    elif SIC_codes:
        query = '&'.join("SICCodes={0}".format(SIC_code) for SIC_code in SIC_codes)
    url = _CALCBENCH_API_URL_BASE.format("companies?" + query)
    r = _calcbench_session().get(url, verify=_SSL_VERIFY)
    r.raise_for_status()
    return r.json()
    
    
def _test_locally():
    global _CALCBENCH_API_URL_BASE
    global _CALCBENCH_LOGON_URL
    global _SSL_VERIFY
    _CALCBENCH_API_URL_BASE = "https://localhost:444/api/{0}"
    _CALCBENCH_LOGON_URL = 'https://localhost:444/account/LogOnAjax'
    _SSL_VERIFY = False
    print(companies(all_companies=True))
    print(breakouts_raw(['msft'], ['operatingSegmentRevenue']))
    
if __name__ == '__main__':
    data = normalized_data(company_identifiers=['ibm', 'msft'], 
                          metrics=['revenue', 'assets', ],
                          start_year=2010,
                          start_period=1,
                          end_year=2014,
                          end_period=4)
    print(data)
 