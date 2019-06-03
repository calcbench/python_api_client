#!/usr/bin/env python
# coding: utf-8
try:
    from azure.servicebus import ServiceBusClient
    from azure.servicebus.control_client import (
        ServiceBusService,
        Message,
        Topic,
        Rule,
        DEFAULT_RULE_NAME,
    )

except ImportError:
    "Will not be able to use the listener"
    pass

import json
import warnings

TOPIC = "filings"


def handle_filings(
    handler,  # type: ()->void
    connection_string,
    subscription,
):
    sb_client = ServiceBusClient.from_connection_string(connection_string)
    subscription = sb_client.get_subscription(TOPIC, subscription)
    for message in subscription.get_receiver():
        try:
            handler(message)
            message.complete()
        except Exception as e:
            warnings.warn(str(e))


def create_filter(
    service_namespace,
    shared_access_key_name,
    shared_access_key_value,
    subscription_name,
    filter_expression,
):
    bus_service = ServiceBusService(
        service_namespace=service_namespace,
        shared_access_key_name=shared_access_key_name,
        shared_access_key_value=shared_access_key_value,
    )
    existing_rules = bus_service.list_rules(
        topic_name=TOPIC, subscription_name=subscription_name
    )
    for rule in existing_rules:
        bus_service.delete_rule(
            topic_name=TOPIC, subscription_name=subscription_name, rule_name=rule.name
        )
    rule = Rule()
    rule.filter_type = "SqlFilter"
    rule.filter_expression = filter_expression
    bus_service.create_rule(
        topic_name=TOPIC,
        subscription_name=subscription_name,
        rule_name="CustomFilter",
        rule=rule,
    )


if __name__ == "__main__":
    import configparser

    config = configparser.ConfigParser()
    config.read("calcbench.ini")
    subscription = config["ServiceBus"]["Subscription"]
    connection_string = config["ServiceBus"]["ConnectionString"]
    service_namespace = config["ServiceBus"]["ServiceNamespace"]
    shared_access_key_name = config["ServiceBus"]["SharedAccessKeyName"]
    shared_access_key_value = config["ServiceBus"]["SharedAccessKeyValue"]

    def filing_handler(filing):
        print(filing)

    create_filter(
        service_namespace=service_namespace,
        shared_access_key_name=shared_access_key_name,
        shared_access_key_value=shared_access_key_value,
        subscription_name=subscription,
        filter_expression="FilingType = 'eightk_earningsPressRelease'",
    )
    handle_filings(
        filing_handler, connection_string=connection_string, subscription=subscription
    )

