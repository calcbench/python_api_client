#!/usr/bin/env python
# coding: utf-8
try:
    from azure.servicebus import SubscriptionClient
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
    sub_client = SubscriptionClient.from_connection_string(
        conn_str=connection_string, name=subscription_name, topic=TOPIC
    )

    for message in sub_client.get_receiver():
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