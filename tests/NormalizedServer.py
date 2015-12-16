'''
Created on Dec 7, 2015

@author: Andrew

This file covers the server, not the client
'''
import unittest
from tests import get_test_session
import calcbench as cb
import json

HOST = 'localhost:444'
class Normalized(unittest.TestCase):
    def setUp(self):
        #self.prod_session = cb.api_client._calcbench_session()
        self.calcbench_session = get_test_session()  
    
    @staticmethod
    def normalized_point_sort_key(data_point, period_type='calendar'):
        return tuple(data_point[key] for key in ('ticker', 'metric', '{0}_year'.format(period_type), '{0}_period'.format(period_type)))

    @staticmethod
    def normalized_point_sort_key_fiscal(data_point):
        return Normalized.normalized_point_sort_key(data_point, 'fiscal')
    
    @staticmethod
    def get_data(payload):
        return cb.api_client._json_POST("normalizedvalues", payload)

    def testNormalizedDateRangeAnnualTrace(self):
        payload = {"start_year"  : 2014, "start_period" :  1, "end_year" : 2014, "end_period" : 4,
                    "company_identifiers" : ["msft"], "metrics" : ["revenue", "taxespayable"], "include_trace" : True}
        data = self.get_data(payload)
        expected_data = json.loads("""
[{"metric":"revenue","ticker":"MSFT","value":20403000000.00000000,"calendar_year":2014,"calendar_period":1,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":20403000000.00000000}]},{"metric":"revenue","ticker":"MSFT","value":23382000000.00000000,"calendar_year":2014,"calendar_period":2,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":23382000000.00000000}]},{"metric":"revenue","ticker":"MSFT","value":23201000000.00000000,"calendar_year":2014,"calendar_period":3,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":23201000000.00000000}]},{"metric":"revenue","ticker":"MSFT","value":26470000000.00000000,"calendar_year":2014,"calendar_period":4,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":26470000000.00000000}]},{"metric":"taxespayable","ticker":"MSFT","value":11603000000.00000000,"calendar_year":2014,"calendar_period":3,"trace_facts":[{"local_name":"AccruedIncomeTaxesCurrent","negative_weight":false,"XBRL_fact_value":903000000.00000000},{"local_name":"AccruedIncomeTaxesNoncurrent","negative_weight":false,"XBRL_fact_value":10700000000.00000000}]},{"metric":"taxespayable","ticker":"MSFT","value":10094000000.00000000,"calendar_year":2014,"calendar_period":1,"trace_facts":[{"local_name":"AccruedIncomeTaxesCurrent","negative_weight":false,"XBRL_fact_value":694000000.00000000},{"local_name":"AccruedIncomeTaxesNoncurrent","negative_weight":false,"XBRL_fact_value":9400000000.00000000}]},{"metric":"taxespayable","ticker":"MSFT","value":12011000000.00000000,"calendar_year":2014,"calendar_period":4,"trace_facts":[{"local_name":"AccruedIncomeTaxesCurrent","negative_weight":false,"XBRL_fact_value":711000000.00000000},{"local_name":"AccruedIncomeTaxesNoncurrent","negative_weight":false,"XBRL_fact_value":11300000000.00000000}]}]
        """)
                         
        
        self.assertListEqual(sorted(data, key=self.normalized_point_sort_key), sorted(expected_data, key=self.normalized_point_sort_key), "Date Range Annual Failed")

    def testNormalizedDatePeriodBackQuarterlyTrace(self):
        payload = {"period_type" : "quarterly", "periods_back" : 4, "company_identifiers" : ["msft"], "metrics" : ["revenue", "taxespayable"], 'include_trace' : True}
        data = sorted(self.get_data(payload), key=self.normalized_point_sort_key)
        expected_data = json.loads('''
        [{"metric":"revenue","ticker":"MSFT","value":26470000000.00000000,"calendar_year":2014,"calendar_period":4,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":26470000000.00000000}]},{"metric":"revenue","ticker":"MSFT","value":21729000000.00000000,"calendar_year":2015,"calendar_period":1,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":21729000000.00000000}]},{"metric":"revenue","ticker":"MSFT","value":22180000000.00000000,"calendar_year":2015,"calendar_period":2,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":22180000000.00000000}]},{"metric":"revenue","ticker":"MSFT","value":20379000000.00000000,"calendar_year":2015,"calendar_period":3,"trace_facts":[{"local_name":"SalesRevenueNet","negative_weight":false,"XBRL_fact_value":20379000000.00000000}]},{"metric":"taxespayable","ticker":"MSFT","value":12507000000.00000000,"calendar_year":2015,"calendar_period":3,"trace_facts":[{"local_name":"AccruedIncomeTaxesCurrent","negative_weight":false,"XBRL_fact_value":607000000.00000000},{"local_name":"AccruedIncomeTaxesNoncurrent","negative_weight":false,"XBRL_fact_value":11900000000.00000000}]},{"metric":"taxespayable","ticker":"MSFT","value":12558000000.00000000,"calendar_year":2015,"calendar_period":1,"trace_facts":[{"local_name":"AccruedIncomeTaxesCurrent","negative_weight":false,"XBRL_fact_value":758000000.00000000},{"local_name":"AccruedIncomeTaxesNoncurrent","negative_weight":false,"XBRL_fact_value":11800000000.00000000}]},{"metric":"taxespayable","ticker":"MSFT","value":12011000000.00000000,"calendar_year":2014,"calendar_period":4,"trace_facts":[{"local_name":"AccruedIncomeTaxesCurrent","negative_weight":false,"XBRL_fact_value":711000000.00000000},{"local_name":"AccruedIncomeTaxesNoncurrent","negative_weight":false,"XBRL_fact_value":11300000000.00000000}]}]
        ''')
        expected_data = sorted(expected_data, key=self.normalized_point_sort_key)
        self.assertListEqual(expected_data, data, "Periods back failed")
        
    def testRatiosDateRangeQuarterly(self):
        payload = {"start_year" : 2002, "start_period" : 0, "end_year" : 2015, "end_period" : 0, "company_identifiers" : ["msft"], "metrics" : ["assetturn"], "use_fiscal_periods" : False}
        data = self.get_data(payload)
        expected_data = json.loads('''
       [{"metric":"assetturn","ticker":"MSFT","value":0.75026900,"calendar_year":2008,"calendar_period":0},{"metric":"assetturn","ticker":"MSFT","value":0.72560400,"calendar_year":2009,"calendar_period":0},{"metric":"assetturn","ticker":"MSFT","value":0.64342600,"calendar_year":2010,"calendar_period":0},{"metric":"assetturn","ticker":"MSFT","value":0.60791900,"calendar_year":2011,"calendar_period":0},{"metric":"assetturn","ticker":"MSFT","value":0.54657300,"calendar_year":2012,"calendar_period":0},{"metric":"assetturn","ticker":"MSFT","value":0.50371800,"calendar_year":2013,"calendar_period":0},{"metric":"assetturn","ticker":"MSFT","value":0.53103100,"calendar_year":2014,"calendar_period":0}]
        ''')
        self.assertListEqual(sorted(expected_data, key=self.normalized_point_sort_key), sorted(data, key=self.normalized_point_sort_key))
        
    def testClientCase(self):
        payload = {"point_in_time": False,   "filing_accession_number": "0000950103-15-008508", "metrics": [ "DiscountedFutureNetCashFlowsRelatingToProvedOilAndGasReservesFutureCashInflows", "SalesReturnsAllowancesAndDiscounts", "TreasuryStockShares", "PrepaidRent", "period_end","ProvedReservesOil","Revenue"]}
        data = self.get_data(payload)
        expected_data = json.loads('''
                [{"metric":"Revenue","ticker":"SHPG","value":1655000000.00000000,"fiscal_year":2015,"fiscal_period":3},{"metric":"period_end","ticker":"SHPG","value":"2015-09-30","fiscal_year":2015,"fiscal_period":3},{"metric":"TreasuryStockShares","ticker":"SHPG","value":9700000.00000000,"fiscal_year":2015,"fiscal_period":3}]
        ''')
        self.assertListEqual(sorted(expected_data, key=Normalized.normalized_point_sort_key_fiscal), 
                                    sorted(data, key=Normalized.normalized_point_sort_key_fiscal),
                             "Client Case Failed.")

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()