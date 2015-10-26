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
import re


SESSION_STUFF = {'calcbench_user_name' : os.environ.get("CALCBENCH_USERNAME"),
                 'calcbench_password' : os.environ.get("CALCBENCH_PASSWORD"),
                 'api_url_base' : "https://www.calcbench.com/api/{0}",
                 'logon_url' : 'https://www.calcbench.com/account/LogOnAjax',
                 'ssl_verify' : True,
                 'session' : None,
                 }


def _calcbench_session():  
    session = SESSION_STUFF.get('session')
    if not session:
        user_name = SESSION_STUFF.get('calcbench_user_name')
        password = SESSION_STUFF.get('calcbench_password')
        if not (user_name and password):
            raise ValueError("No credentials supplied, either call set_credentials or set \
CALCBENCH_USERNAME and CALCBENCH_PASSWORD environment variables.")
        session = requests.Session()
        r = session.post(SESSION_STUFF['logon_url'], 
                  {'email' : user_name, 
                   'strng' : password, 
                   'rememberMe' : 'true'},
                  verify=SESSION_STUFF['ssl_verify'])
        r.raise_for_status()
        if r.text != 'true':
            raise ValueError('Incorrect Credentials, use the email and password you use to login to Calcbench.')
        else:
            SESSION_STUFF['session'] = session
            
    return session

def set_credentials(cb_username, cb_password):
    '''Set your calcbench credentials.
    
    username is the email address you use to login to calcbench.com.
    '''
    SESSION_STUFF['calcbench_user_name'] = cb_username
    SESSION_STUFF['calcbench_password'] = cb_password
    _calcbench_session() #Make sure credentials work.


def normalized_dataframe(company_identifiers=[], 
                    metrics=[], 
                    start_year=None, 
                    start_period=None,
                    end_year=None,
                    end_period=None,
                    entire_universe=False):
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

    data = normalized_raw(company_identifiers, metrics, start_year,
                          start_period, end_year, end_period, entire_universe)
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
        try:
            d['value'] = float(d['value'])
        except ValueError:
            pass
        
    data = pd.DataFrame(data)
    data.set_index(keys=['ticker', 'metric', 'period'],
                   inplace=True)
    data = data.unstack('metric')['value']
    data = data.unstack('ticker')
    try:
        data = data[metrics]
    except KeyError as e:
        if "not in index" in str(e):
            raise KeyError('{0}, metrics are case sensitive.'.format(e))
        else:
            raise e

    return data

    
    
def _build_quarter_period(data_point):
    return pd.Period(year=data_point.pop('calendar_year'),
                     quarter=data_point.pop('calendar_period'),
                     freq='q')

def _build_annual_period(data_point):
    data_point.pop('calendar_period')
    return pd.Period(year=data_point.pop('calendar_year'), freq='a')

    

normalized_data = normalized_dataframe # used to call it normalized_data.

def normalized_raw(company_identifiers=[], 
                    metrics=[], 
                    start_year=None, 
                    start_period=None,
                    end_year=None,
                    end_period=None,
                    entire_universe=False):
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
    if bool(company_identifiers) == entire_universe:
        raise ValueError("Must pass either company_identifiers or entire_universe=True")
    
        
    url = SESSION_STUFF['api_url_base'].format("/NormalizedValues")
    payload = {"start_year" : start_year,
           'start_period' : start_period,
           'end_year' : end_year,
           'end_period' : end_period,
           'company_identifiers' : list(company_identifiers),
           'metrics' : metrics,
           'entire_universe' : entire_universe
           }
    response = _calcbench_session().post(
                                    url,
                                    data=json.dumps(payload), 
                                    headers={'content-type' : 'application/json'},
                                    verify=SESSION_STUFF['ssl_verify']
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
    url = SESSION_STUFF['api_url_base'].format("asReported")
    payload = {'companyIdentifier' : company_identifier,
               'statementType' : statement_type,
               'periodType' : period_type,
               'allPeriods' : all_periods,
               'descendingDates' : descending_dates}
    response = _calcbench_session().get(url, 
                                        params=payload, 
                                        headers={'content-type' : 'application/json'}, 
                                        verify=SESSION_STUFF['ssl_verify'])
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
    url = SESSION_STUFF['api_url_base'].format('breakouts')
    response = _calcbench_session().post(url,
                                         data=json.dumps(payload),
                                         headers={'content-type' : 'application/json'},
                                         verify=SESSION_STUFF['ssl_verify'])
    response.raise_for_status()
    data = response.json()
    return data
    

def tickers(SIC_codes=[], index=None, universe=False):
    '''Return a list of tickers in the peer-group'''
    companies = _companies(SIC_codes, index, universe)
    tickers = [co['ticker'] for co in companies]
    return tickers

def companies(SIC_codes=[], index=None, entire_universe=False):
    '''Return a DataFrame with company details'''
    companies = _companies(SIC_codes, index, entire_universe)
    return pd.DataFrame(companies)
    
def _companies(SIC_codes, index, entire_universe=False):
    if not(SIC_codes or index or entire_universe):
        raise ValueError('Must supply SIC_code or index')
    query = "universe=true"  
    if index:
        if index not in ("SP500", "DJIA"):
            raise ValueError("index must be either 'SP500' or 'DJIA'")
        query = "index={0}".format(index)
    elif SIC_codes:
        query = '&'.join("SICCodes={0}".format(SIC_code) for SIC_code in SIC_codes)
    url = SESSION_STUFF['api_url_base'].format("companies?" + query)
    r = _calcbench_session().get(url, verify=SESSION_STUFF['ssl_verify'])
    r.raise_for_status()
    return r.json()

def companies_raw(SIC_codes=[], index=None, entire_universe=False):
    return _companies(SIC_codes, index, entire_universe)
    
    
def _test_locally():
    SESSION_STUFF['api_url_base'] = "https://localhost:444/api/{0}"
    SESSION_STUFF['logon_url'] = 'https://localhost:444/account/LogOnAjax'
    SESSION_STUFF['ssl_verify'] = False
        
        
    normalized_data(company_identifiers=['msft', 'orcl'], 
                          metrics=['revenue', 'assets', ],
                          start_year=2010,
                          start_period=1,
                          end_year=2014,
                          end_period=4)
    print(companies(entire_universe=True))
    print(breakouts_raw(['msft'], ['operatingSegmentRevenue']))
    
if __name__ == '__main__':
    _test_locally()
    data = normalized_data(entire_universe=True, 
                          metrics=['revenue', 'assets', ],
                          start_year=2010,
                          start_period=1,
                          end_year=2014,
                          end_period=4)
    print(data)
 
