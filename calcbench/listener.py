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
import logging

from .api_client import _cast_filing_fields

logger = logging.getLogger(__name__)

TOPIC = "filings"


def handle_filings(
    handler, connection_string, subscription_name, filter_expression="1=1"
):
    """Listen for new filings from Calcbench

    Pass in a function that process each filing.

    Usage::
        >>> def filing_handler(filing):
        >>>     accession_id = filing['calcbench_id']
        >>>     data = point_in_time(
        >>>             accession_id=accession_id
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
            body_bytes = b"".join(message.body)
            filing = json.loads(body_bytes)
            filing = _cast_filing_fields(filing)
        except Exception:
            logger.exception(f"Exception Parsing {body_bytes}")
            message.abandon()
        else:
            try:
                handler(filing)
            except Exception as e:
                logger.exception(f"Exception Processing {filing}\n{e}")                
            else:
                message.complete()


def _create_filter(bus_service, subscription_name, filter_expression):
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

