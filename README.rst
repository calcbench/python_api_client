Calcbench Client
================

A Python client for Calcbench's API.

Calcbench normalizes the data XBRL tagged accounting metrics in the 10-K and 10-Q documents public companies file the with SEC.  If you are spending a lot of time on Edgar and know some Python this package might make your life easier.

Get a free two week Calcbench trial @ https://www.calcbench.com/join.

Your Calcbench username (your email) and Calcbench password are your credentials for this package.

See examples @ https://www.calcbench.com/home/api


To install the client with pip use: 

    pip install git+git://github.com/calcbench/python_api_client.git
    
To set your credentials either set CALCBENCH_USERNAME and CALCBENCH_PASSWORD environment variables or call:

    calcbench.set_credentials({calcbench_username}, {calcbench_password})
    
To get normalized data call `normalized_dataframe`, for instance:

    calcbench.normalized_dataframe(company_identifiers=['msft', 'ibm'], metrics=['revenue', 'assets'], start_year=2010, start_period=1, end_year=2014, end_period=4)
    
To get 'As Reported' statements, call `as_reported_raw`, for instance:

	calcbench.as_reported('msft', 'income')
	
To get breakout/segments call `breakouts_raw`, for instance:

	calcbench.breakouts_raw(company_identifiers=['MSFT', 'AXP'], metrics=['operatingSegmentRevenue', 'operatingSegmentAssets'])

Company identifiers, tickers in most cases, can be retrieved by Standard Industrial Classification (SIC) code or index, for instance
    
    calcbench.tickers(index='DJIA')
This is a work in progress.  Let me know if you have suggestions or encounter bugs, andrew@calcbench.com.
