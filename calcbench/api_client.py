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
import warnings

try:
    import pandas as pd  
    import numpy as np
except ImportError:
    "Can't find pandas, won't be able to use the funtions that return dataframes."
    pass


_SESSION_STUFF = {'calcbench_user_name' : os.environ.get("CALCBENCH_USERNAME"),
                 'calcbench_password' : os.environ.get("CALCBENCH_PASSWORD"),
                 'api_url_base' : "https://www.calcbench.com/api/{0}",
                 'logon_url' : 'https://www.calcbench.com/account/LogOnAjax',
                 'domain' : 'https://www.calcbench.com/{0}',
                 'ssl_verify' : True,
                 'session' : None,
                 }


def _calcbench_session():  
    session = _SESSION_STUFF.get('session')
    if not session:
        user_name = _SESSION_STUFF.get('calcbench_user_name')
        password = _SESSION_STUFF.get('calcbench_password')
        if not (user_name and password):
            raise ValueError("No credentials supplied, either call set_credentials or set \
                                CALCBENCH_USERNAME and CALCBENCH_PASSWORD environment variables.")
        session = requests.Session()
        r = session.post(_SESSION_STUFF['logon_url'],
                  {'email' : user_name, 
                   'strng' : password, 
                   'rememberMe' : 'true'},
                  verify=_SESSION_STUFF['ssl_verify'])
        r.raise_for_status()
        if r.text != 'true':
            raise ValueError('Incorrect Credentials, use the email and password you use to login to Calcbench.')
        else:
            _SESSION_STUFF['session'] = session
    return session

def _rig_for_testing(domain='localhost:444', suppress_http_warnings=True):
    _SESSION_STUFF['api_url_base'] = 'https://' + domain + '/api/{0}'
    _SESSION_STUFF['logon_url'] = 'https://' + domain + '/account/LogOnAjax'
    _SESSION_STUFF['domain'] = 'https://' + domain
    _SESSION_STUFF['ssl_verify'] = False
    _SESSION_STUFF['session'] = None
    if suppress_http_warnings:
        from requests.packages.urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def _json_POST(end_point, payload):
    url = _SESSION_STUFF['api_url_base'].format(end_point)
    response = _calcbench_session().post(url,
                                        data=json.dumps(payload),
                                        headers={'content-type' : 'application/json'},
                                        verify=_SESSION_STUFF['ssl_verify'])
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(response.content)
        raise e
    return response.json()

def set_credentials(cb_username, cb_password):
    '''Set your calcbench credentials.
    
    username is the email address you use to login to calcbench.com.
    '''
    _SESSION_STUFF['calcbench_user_name'] = cb_username
    _SESSION_STUFF['calcbench_password'] = cb_password
    _calcbench_session() #Make sure credentials work.


def normalized_dataframe(company_identifiers=[], 
                    metrics=[], 
                    start_year=None, 
                    start_period=None,
                    end_year=None,
                    end_period=None,
                    entire_universe=False,
                    filing_accession_number=None,
                    point_in_time=False):
    '''Normalized data.
    
    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.
    
    Args:
        company_identifiers: a sequence of tickers (or CIK codes), eg ['msft', 'goog', 'appl']
        metrics: a sequence of metrics, see the full list @ https://www.calcbench.com/home/excel#availableMetrics eg. ['revenue', 'accountsreceivable']
        start_year: first year of data
        start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        end_year: last year of data
        end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4        
        entire_universe: Get data for all companies, this can take a while, talk to Calcbench before you do this in production.
        accession_id: Filing Accession ID from the SEC's Edgar system.
    Returns:
        A Pandas.Dataframe with the periods as the index and columns indexed by metric and ticker
    '''
    data = normalized_raw(company_identifiers=list(company_identifiers), 
                          metrics=metrics, 
                          start_year=start_year,
                          start_period=start_period, 
                          end_year=end_year, 
                          end_period=end_period, 
                          entire_universe=entire_universe,
                          point_in_time=point_in_time,
                          filing_accession_number=filing_accession_number)
    if not data:
        warnings.warn("No data found")
        return pd.DataFrame()
    quarterly = start_period and end_period
    if quarterly:
        build_period = _build_quarter_period
    else:
        build_period = _build_annual_period
   
    metrics_found = set()
    for d in data:                          
        d['period'] = build_period(d)
        d['ticker'] = d['ticker'].upper()
        try:
            d['value'] = float(d['value'])
        except ValueError:
            pass
        metrics_found.add(d['metric'])
    
    missing_metrics = set(metrics) - metrics_found
    if missing_metrics:
        warnings.warn("Did not find metrics {0}".format(missing_metrics))
    data = pd.DataFrame(data)
    data.set_index(keys=['ticker', 'metric', 'period'],
                   inplace=True)
    data = data.unstack('metric')['value']
    for column_name in data.columns.values:
        #Try to make the columns the right type
        try:
            data[column_name] = pd.to_numeric(data[column_name], errors='raise')
        except ValueError:
            if 'date' in column_name.lower():
                data[column_name] = pd.to_datetime(data[column_name], errors='coerce')
        
    for missing_metric in missing_metrics:
        data[missing_metric] = np.nan # We want columns for every requested metric.     
    data = data.unstack('ticker')
    #data = data[[metric for metric in metrics if metric in metrics_found]]  I don't know what this was doing, akittredge August 2016

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
                    entire_universe=False,
                    filing_accession_number=None,
                    point_in_time=False,
                    include_trace=False):
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
        entire_universe: Get data for all companies, this can take a while, talk to Calcbench before you do this in production.
        accession_id: Filing Accession ID from the SEC's Edgar system.
        include_trace: Include the facts used to calculate the normalized value.
        
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
    if [bool(company_identifiers), bool(entire_universe), bool(filing_accession_number)].count(True) != 1:
        raise ValueError("Must pass either company_identifiers and accession id or entire_universe=True")
    
    if filing_accession_number and any([company_identifiers, start_year, start_period, end_year, end_period, entire_universe]):
        raise ValueError("Accession IDs are specific to a filing, no other qualifiers make sense.")
        
    payload = {"start_year" : start_year,
           'start_period' : start_period,
           'end_year' : end_year,
           'end_period' : end_period,
           'company_identifiers' : list(company_identifiers),
           'metrics' : metrics,
           'entire_universe' : entire_universe,
           'filing_accession_number' : filing_accession_number,
           'point_in_time' : point_in_time,
           'include_trace' : include_trace,
           }
    return _json_POST("NormalizedValues", payload)

def point_in_time(company_identifiers=[], all_footnotes=False, 
                  update_date=None, metrics=[], all_history=False,
                  entire_universe=False):
    '''
    Just for point-in-time footnotes now.
    '''
    data = mapped_raw(company_identifiers=company_identifiers, 
                      all_footnotes=all_footnotes, 
                      point_in_time=True,
                      update_date=update_date,
                      metrics=metrics,
                      all_history=all_history,
                      entire_universe=entire_universe)
    if not data:
        return pd.DataFrame()
    data = pd.DataFrame(data)
    data.calendar_period = data.calendar_period.astype('category', categories=[1, 2, 3, 4, 0], ordered=True) # So annual is last in sorting.
    data.fiscal_period = data.fiscal_period.astype('category', categories=[1, 2, 3, 4, 0], ordered=True) 
    return data.sort_values(['ticker', 'metric', 'calendar_year', 'calendar_period']).reset_index(drop=True)

def mapped_raw(company_identifiers=[], all_footnotes=False, point_in_time=False, 
               update_date=None, metrics=[], all_history=False, entire_universe=False):
    payload = {
               'companiesParameters' : {'companyIdentifiers' : company_identifiers, 'entireUniverse' :  entire_universe},
               'pageParameters' : {'pointInTime' : point_in_time, 
                                   'allFootnotes' : all_footnotes, 
                                   'metrics' : metrics},
            }
    period_parameters = {'allHistory' : all_history}
    if update_date:
        period_parameters['updateDate'] = update_date.isoformat()
    payload['periodParameters'] = period_parameters
    return _json_POST("mappedData", payload)

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
    url = _SESSION_STUFF['api_url_base'].format("asReported")
    payload = {'companyIdentifier' : company_identifier,
               'statementType' : statement_type,
               'periodType' : period_type,
               'allPeriods' : all_periods,
               'descendingDates' : descending_dates}
    response = _calcbench_session().get(url, 
                                        params=payload, 
                                        headers={'content-type' : 'application/json'}, 
                                        verify=_SESSION_STUFF['ssl_verify'])
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
    return _json_POST('breakouts', payload)

def document_search(company_identifiers=None, 
                    full_text_search_term=None,
                    year=None, 
                    period=0, 
                    period_type='annual',                 
                    all_footnotes=False, 
                    document_type=None,
                    block_tag_name=None,):        
    '''
    Footnotes and other text
    
    Search for footnotes and other , see https://www.calcbench.com/footnote.
    
    Args:
        company_identifiers : list of tickers or CIK codes
        start_year: first year of data to get
        start_period: first period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
        end_year: last year of data to get
        end_period: last period of data to get. 0 for annual data, 1, 2, 3, 4 for quarterly data.
        period_type: quarterly or annual, only applicable when other period data not supplied.
        
    Returns:
        A list of text documents (footnotes)
        
        
    '''
    if not any([full_text_search_term, all_footnotes, document_type, block_tag_name]):
        raise(ValueError("Need to supply at least one search parameter."))
    if period_type not in ('annual', 'quarterly'):
        raise(ValueError("period_type must be in ('annual', 'quarterly'))"))
    payload = {'companiesParameters' : {'entireUniverse' : len(company_identifiers) == 0,
                                        'companyIdentifiers' : company_identifiers},
               'periodParameters' : {'year' : year,
                                     'period' : period,
                                     'periodType' : period_type},
               'pageParameters' : {'fullTextQuery' : full_text_search_term,
                                   'allFootnotes' : all_footnotes,
                                   'footnoteType' : document_type,
                                   'footnoteTag' : block_tag_name}}
    results = {'moreResults' : True}
    while results['moreResults']:
        results = _json_POST('footnoteSearch', payload)
        for result in results['footnotes']:
            yield result
        payload['pageParameters']['startEntityID'] = results['nextGroupStartEntityID']
        
def document_contents(blob_id, SEC_ID, SEC_URL=None):
    url = _SESSION_STUFF['domain'].format('query/disclosureBySECLink')
    payload = {'blobid' : blob_id, 'secid' : SEC_ID, 'url' : SEC_URL}
    response = _calcbench_session().get(url,
                                        params=payload,
                                        headers={'content-type': 'application/json'},
                                        verify=_SESSION_STUFF['ssl_verify'])
    response.raise_for_status()
    return response.json()['blobs'][0]

def tag_contents(accession_id, block_tag_name):
    url = _SESSION_STUFF['domain'].format('query/disclosuresByTag')
    payload = {'accession_ids' : accession_id, 'block_tag_name' : block_tag_name}
    response = _calcbench_session().get(url, 
                                        params=payload,
                                        headers={'content-type' : 'application/json'},
                                        verify=_SESSION_STUFF['ssl_verify'])
    response.raise_for_status()
    return response.json()[0]['blobs'][0]

def tickers(SIC_codes=[], index=None, company_identifiers=[], entire_universe=False):
    '''Return a list of tickers in the peer-group'''
    companies = _companies(SIC_codes, index, company_identifiers, entire_universe)
    tickers = [co['ticker'] for co in companies]
    return tickers


def companies(SIC_codes=[], index=None, company_identifiers=[], entire_universe=False):
    '''Return a DataFrame with company details'''
    companies = _companies(SIC_codes, index, company_identifiers, entire_universe)
    return pd.DataFrame(companies)
    
def _companies(SIC_code, index, company_identifiers, entire_universe=False):
    if not(SIC_code or index or entire_universe or company_identifiers):
        raise ValueError('Must supply SIC_code, index or company_identifiers or entire_univers.')
    payload = {}
 
    if index:
        if index not in ("SP500", "DJIA"):
            raise ValueError("index must be either 'SP500' or 'DJIA'")
        payload['index'] = index
    elif SIC_code:
        payload['SICCodes'] = SIC_code
    elif company_identifiers:
        payload['companyIdentifiers'] = ','.join(company_identifiers)
    else:
        payload['universe'] = True
    url = _SESSION_STUFF['api_url_base'].format("companies")
    r = _calcbench_session().get(url, params=payload, verify=_SESSION_STUFF['ssl_verify'])
    r.raise_for_status()
    return r.json()

def companies_raw(SIC_codes=[], index=None, company_identifiers=[], entire_universe=False):
    return _companies(SIC_codes, index, company_identifiers, entire_universe)

def company_disclosures(ticker, period=None, year=None, statement_type=None):    
    payload = {'companyIdentifier' : ticker}
    if period:
        payload['period'] = period
    if year:
        payload['year'] = year
    if statement_type:
        payload['statementType'] = statement_type
    url = _SESSION_STUFF['api_url_base'].format('companyDisclosures')
    r = _calcbench_session().get(url, params=payload, verify=_SESSION_STUFF['ssl_verify'])
    r.raise_for_status()
    return r.json()
                          
def disclosure_text(network_id):
    url = _SESSION_STUFF['api_url_base'].format('disclosure')
    r = _calcbench_session().get(url, params={'networkID': network_id}, verify=_SESSION_STUFF['ssl_verify'])
    r.raise_for_status()
    return r.json()

def available_metrics():
    url = _SESSION_STUFF['api_url_base'].format('availableMetrics')
    r = _calcbench_session().get(url, verify=_SESSION_STUFF['ssl_verify'])
    r.raise_for_status()
    return r.json()

def filings(company_identifier, include_non_xbrl=True):
    url = _SESSION_STUFF['api_url_base'].format('filings')
    r = _calcbench_session().get(url, params={'companyIdentifier' : company_identifier})
    r.raise_for_status()
    return r.json()
    
if __name__ == '__main__':

    #_rig_for_testing()
    
    metrics = [
    'revenue', 
    'netincome',
    'cash'
]

    print(document_contents(blob_id='326149_section270', SEC_ID=326149))
    
    data = normalized_data(entire_universe=True, 
                          metrics=['current_assets', 
                   'current_liabilities', 
                   'assets', 
                   'retained_earnings', 
                   'ebit', 
                   'MarketCapAtEndOfPeriod',
                  'liabilites',
                  'revenue'],
                          start_year=2010,
                          start_period=0,
                          end_year=2014,
                          end_period=0)
    print(data)
