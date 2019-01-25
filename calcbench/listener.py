#!/usr/bin/env python
# coding: utf-8
try:
    from azure.servicebus import ServiceBusService, Message
except ImportError:
    "Will not be able to use the listener"
    pass

import json
import warnings

TOPIC = 'filings'

def handle_filings(handler, #type: ()->void
                    subscription,  
                    readonly_shared_access_key_value="Cb7VhLR6eJxsurCSPtXQHOJvlkU84CCCx2oB+T/so6Q=", 
                    service_bus_namespace='calcbench'):
    
    bus_service = ServiceBusService(service_namespace=service_bus_namespace,
                                    shared_access_key_name="public",
                                    shared_access_key_value=readonly_shared_access_key_value)
    while True:
        try:
            message = bus_service.receive_subscription_message(TOPIC, subscription)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            warnings.warn(str(e))
        else:
            if(message.body):
                try:
                    filing = json.loads(message.body)
                    handler(filing)
                    message.delete()
                except Exception as e:
                    warnings.warn(str(e))