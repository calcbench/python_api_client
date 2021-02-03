#!/usr/bin/env python
# coding: utf-8
try:
    from azure.servicebus import ServiceBusClient, AutoLockRenewer
    from azure.servicebus.aio import ServiceBusClient as AsyncServiceBusClient
except ImportError:
    "Will not be able to use the listener"
    pass


import json
import logging
from typing import Callable, Iterable, cast


from .filing import Filing

logger = logging.getLogger(__name__)

TOPIC = "filings"


def handle_filings(
    handler: Callable[[Filing], None],
    connection_string: str = "Endpoint=sb://calcbench.servicebus.windows.net/;SharedAccessKeyName=public;SharedAccessKey=Cb7VhLR6eJxsurCSPtXQHOJvlkU84CCCx2oB+T/so6Q=",
    subscription_name: str = None,
):
    """Listen for new filings from Calcbench

    https://github.com/calcbench/notebooks/blob/master/filing_listener.ipynb.

    :param handler: function that "handles" the filing, for instance getting data from Calcbench and writing it to your database
    :param connection_string: azure service bus connection string
    :param subscription_name: service bus subscription, Calcbench will give this to you

    Usage::
        >>> def filing_handler(filing):
        >>>     if not filing.standardized_XBRL:
        >>>         return
        >>>     accession_id = filing.calcbench_id
        >>>     data = point_in_time(
        >>>             accession_id=accession_id
        >>>             all_face=True,
        >>>             all_footnotes=True,
        >>>             )
        >>>     print(data)
        >>>
        >>> handle_filings(
        >>>     filing_handler,
        >>>     subscription_name=subscription,
        >>> )
    """
    if not subscription_name:
        raise ValueError("Need to supply subscription_name")

    with AutoLockRenewer() as renewer:
        with ServiceBusClient.from_connection_string(
            conn_str=connection_string
        ) as client:
            with client.get_subscription_receiver(
                topic_name=TOPIC, subscription_name=subscription_name
            ) as receiver:
                for message in receiver:
                    renewer.register(receiver, message, max_lock_renewal_duration=300)
                    body_bytes = b"".join(message.body)
                    try:
                        filing = Filing(**json.loads(body_bytes))
                    except Exception:
                        logger.exception(f"Exception Parsing {body_bytes}")
                        receiver.abandon_message(message)
                    else:
                        try:
                            logger.info(f"Handling {filing}")
                            handler(filing)
                        except Exception as e:
                            logger.exception(
                                f"Exception Processing {filing}\n delivery count: {message.delivery_count}\n{e}"
                            )
                        else:
                            receiver.complete_message(message)


async def handle_filings_async(
    handler: Callable[[Filing], None],
    connection_string: str = "Endpoint=sb://calcbench.servicebus.windows.net/;SharedAccessKeyName=public;SharedAccessKey=Cb7VhLR6eJxsurCSPtXQHOJvlkU84CCCx2oB+T/so6Q=",
    subscription_name: str = None,
):
    """
    https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/servicebus/azure-servicebus/samples/async_samples/receive_subscription_async.py
    """
    servicebus_client = AsyncServiceBusClient.from_connection_string(
        conn_str=connection_string
    )
    if not subscription_name:
        raise ValueError("Need to supply subscription_name")

    async with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(
            topic_name=TOPIC, subscription_name=subscription_name
        )
        async with receiver:
            while True:
                received_msgs = await receiver.receive_messages()
                for msg in received_msgs:
                    message_body = cast(Iterable[bytes], msg.body)
                    body_bytes = b"".join(message_body)
                    try:
                        filing = Filing(**json.loads(body_bytes))
                    except Exception:
                        logger.exception(f"Exception Parsing {body_bytes}")
                        await msg.abandon()
                    else:
                        logger.info(f"Handling {filing}")
                        handler(filing)
                    await msg.complete()