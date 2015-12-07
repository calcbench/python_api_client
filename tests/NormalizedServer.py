'''
Created on Dec 7, 2015

@author: Andrew

This file covers the server, not the client
'''
import unittest
from tests import get_test_session
import calcbench as cb

HOST = 'localhost:444'
class Normalized(unittest.TestCase):
    get_data = lambda payload : cb.api_client._json_POST("normalizedvalues", payload)
    def setUp(self):
        #self.prod_session = cb.api_client._calcbench_session()
        self.calcbench_session = get_test_session


    def testNormalizedDateRangeAnnualTrace(self):
        payload = {"start_year"  : 2014, "start_period" :  1, "end_year" : 2014, "end_period" : 4,
                    "company_identifiers" : ["msft"], "metrics" : ["revenue", "taxespayable"], "include_trace" : True}
        data = Normalized.get_data(payload)
        expected_data = [
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 1,
        "metric": "Revenue",
        "value": 20403000000,
        "trace_facts": [
            {
                "local_name": "SalesRevenueNet",
                "negative_weight": False,
                "XBRL_fact_value": 20403000000
            }
        ]
    },
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 2,
        "metric": "Revenue",
        "value": 23382000000,
        "trace_facts": [
            {
                "local_name": "SalesRevenueNet",
                "negative_weight": False,
                "XBRL_fact_value": 23382000000
            }
        ]
    },
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 3,
        "metric": "Revenue",
        "value": 23201000000,
        "trace_facts": [
            {
                "local_name": "SalesRevenueNet",
                "negative_weight": False,
                "XBRL_fact_value": 23201000000
            }
        ]
    },
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 4,
        "metric": "Revenue",
        "value": 26470000000,
        "trace_facts": [
            {
                "local_name": "SalesRevenueNet",
                "negative_weight": False,
                "XBRL_fact_value": 26470000000
            }
        ]
    },
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 3,
        "metric": "TaxesPayable",
        "value": 11603000000,
        "trace_facts": [
            {
                "local_name": "AccruedIncomeTaxesCurrent",
                "negative_weight": False,
                "XBRL_fact_value": 903000000
            },
            {
                "local_name": "AccruedIncomeTaxesNoncurrent",
                "negative_weight": False,
                "XBRL_fact_value": 10700000000
            }
        ]
    },
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 1,
        "metric": "TaxesPayable",
        "value": 10094000000,
        "trace_facts": [
            {
                "local_name": "AccruedIncomeTaxesCurrent",
                "negative_weight": False,
                "XBRL_fact_value": 694000000
            },
            {
                "local_name": "AccruedIncomeTaxesNoncurrent",
                "negative_weight": False,
                "XBRL_fact_value": 9400000000
            }
        ]
    },
    {
        "ticker": "MSFT",
        "calendar_year": 2014,
        "calendar_period": 4,
        "metric": "TaxesPayable",
        "value": 12011000000,
        "trace_facts": [
            {
                "local_name": "AccruedIncomeTaxesCurrent",
                "negative_weight": False,
                "XBRL_fact_value": 711000000
            },
            {
                "local_name": "AccruedIncomeTaxesNoncurrent",
                "negative_weight": False,
                "XBRL_fact_value": 11300000000
            }
        ]
    }
                         
                         ]
        self.assertListEqual(data, expected_data, "Date Range Annual Failed")

    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()