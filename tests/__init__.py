'''
Created on Dec 7, 2015

@author: Andrew
'''
import calcbench as cb

def get_test_session(host='localhost:444'):
    cb.api_client._SESSION_STUFF['logon_url'] = cb.api_client._SESSION_STUFF['logon_url'].replace('www.calcbench.com', host)
    cb.api_client._SESSION_STUFF['api_url_base'] = cb.api_client._SESSION_STUFF['api_url_base'].replace('www.calcbench.com', host)
    cb.api_client._SESSION_STUFF['ssl_verify'] = False
    return cb.api_client._calcbench_session()