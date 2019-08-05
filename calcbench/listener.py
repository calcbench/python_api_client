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
    handler,  
    connection_string,
    subscription_name,
    filter_expression="1=1",
):
    """Listen for new filings from Calcbench

    Pass in a function that process each filing.

    Usage::
        >>> def filing_handler(filing):
        >>>     year = filing["fiscal_year"]
        >>>     period = filing["fiscal_period"]
        >>>     ticker = filing["ticker"]
        >>>     data = point_in_time(
        >>>             company_identifiers=[ticker],
        >>>             start_year=year,
        >>>             start_period=period,
        >>>             end_year=year,
        >>>             end_period=period,
        >>>             use_fiscal_period=True,
        >>>             all_face=True,
        >>>             all_footnotes=True,
        >>>             )
        >>>    print(data)
        >>>
        >>> handle_filings(
        >>>     filing_handler,
        >>>     connection_string=connection_string,
        >>>     subscription_name=subscription,
        >>>     filter_expression="FilingType = 'annualQuarterlyReport'",
        >>> )


    """
    sb_client = ServiceBusClient.from_connection_string(connection_string)
    subscription = sb_client.get_subscription(TOPIC, subscription_name)
    _create_filter(
        sb_client.mgmt_client,
        subscription_name=subscription_name,
        filter_expression=filter_expression,
    )

    for message in subscription.get_receiver():
        try:
            filing = json.loads(b"".join(message.body))
            handler(filing)
            message.complete()
        except Exception as e:
            warnings.warn(str(e))


def _create_filter(bus_service, subscription_name, filter_expression):
    bus_service.delete_rule(
        topic_name=TOPIC,
        subscription_name=subscription_name,
        rule_name=DEFAULT_RULE_NAME,
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
    from api_client import point_in_time

    config = configparser.ConfigParser()
    config.read("calcbench.ini")
    subscription = config["ServiceBus"]["Subscription"]
    connection_string = config["ServiceBus"]["ConnectionString"]

    def filing_handler(filing):
        year = filing["fiscal_year"]
        period = filing["fiscal_period"]
        ticker = filing["ticker"]
        data = point_in_time(
            company_identifiers=[ticker],
            start_year=year,
            start_period=period,
            end_year=year,
            end_period=period,
            use_fiscal_period=True,
            all_face=True,
            all_footnotes=True,
        )
        print(data)

    handle_filings(
        filing_handler,
        connection_string=connection_string,
        subscription_name=subscription,
        filter_expression="FilingType = 'annualQuarterlyReport'",
    )
