# Calcbench API Client

A Python client for Calcbench's API.

Calcbench is an interface to the XBRL encoded 10-(K|Q) documents public companies file on the SEC's Edgar system.

## Installation

Pip Installation

    pip install calcbench-api-client
    
## Credentials

Use your Calcbench username (email) and password.  Get a free two week Calcbench trial @ https://www.calcbench.com/join.

To set your credentials either set `CALCBENCH_USERNAME` and `CALCBENCH_PASSWORD` environment variables or call:

    calcbench.set_credentials({calcbench_username}, {calcbench_password})
    

## Examples

Example Jupyter notebooks @ https://github.com/calcbench/notebooks.

### Standardized Data

The data behind https://www.calcbench.com/multi.  List of all metrics @ https://www.calcbench.com/home/standardizedmetrics.

    calcbench.standardized_data(company_identifiers=['msft', 'ibm'], metrics=['revenue', 'assets'], start_year=2010, start_period=1, end_year=2014, end_period=4)
    
### As-Reported Face Statement

The data behind https://www.calcbench.com/detail

	calcbench.as_reported('msft', 'income')
	
### Tickers
Company identifiers, tickers in most cases, can be retrieved by Standard Industrial Classification (SIC) code or Index
    
    calcbench.tickers(index='DJIA')

### Disclosures

The data behind https://www.calcbench.com/disclosures

Search for disclosures, for instance to search for "going concern" in coal company filings:

	coal_companies = cb.tickers(SIC_codes=[1200])
	
	cb.text_search(company_identifiers=coal_companies, full_text_search_term='"going concern"', year=2015, period=0)

### New Filings Push Notification

Be notified when Calcbench has processed new filings.

Requires Calcbench to generate a subscription for you and installing the `azure-servicebus` package

Then

    calcbench.handle_filings(lambda filing: print(filing), {service bus subscription})

## Support

andrew@calcbench.com

## Credit
@someben https://github.com/calcbench/python_api_client/commit/6c2312525fa365acc91bd8e979037fc2492845f3