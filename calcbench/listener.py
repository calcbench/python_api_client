#!/usr/bin/env python
# coding: utf-8
try:
    from azure.servicebus import ServiceBusClient
except ImportError:
    "Will not be able to use the listener"
    pass

import json
import logging

from .api_client import _cast_filing_fields

logger = logging.getLogger(__name__)

TOPIC = "filings"


def handle_filings(handler, connection_string, subscription_name):
    """Listen for new filings from Calcbench

    Pass in a function that process each filing.

    Usage::
        >>> def filing_handler(filing):
        >>>     if filing["filing_type"] != "annualQuarterlyReport":
        >>>         return
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
        >>> )
    """
    with ServiceBusClient.from_connection_string(conn_str=connection_string) as client:
        with client.get_subscription_receiver(
            topic_name=TOPIC, subscription_name=subscription_name
        ) as receiver:
            for message in receiver:
                body_bytes = b"".join(message.body)
                try:
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
