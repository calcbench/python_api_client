#!/usr/bin/env python
# coding: utf-8
try:
    from azure.servicebus import ServiceBusClient
except ImportError:
    "Will not be able to use the listener"
    pass

import json
import warnings

TOPIC = 'filings'

def handle_filings(handler, #type: ()->void
                    connection_string,
                    subscription):
    sb_client = ServiceBusClient.from_connection_string(connection_string)
    subscription = sb_client.get_subscription(TOPIC, subscription)
    for message in subscription.get_receiver():
        try:
            handler(message)
            message.complete()
        except Exception as e:
            warnings.warn(str(e))

if __name__ == "__main__":
    import configparser
    config = configparser.ConfigParser()
    config.read('calcbench.ini')
    subscription = config['ServiceBus']['subscription']
    def filing_handler(filing):
        print(filing)

    handle_filings(filing_handler, 
    connection_string="Endpoint=sb://calcbench.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iyg1HHDSe7lXfNeuRX3ZzuATXgCrga0edFgdXh0GC0s=",
    subscription="andrew_test")
    