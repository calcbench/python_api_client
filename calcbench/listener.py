#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Callable, Iterable, Optional, cast

try:
    from azure.servicebus import AutoLockRenewer, ServiceBusClient, ServiceBusReceiver
    from azure.servicebus.aio import ServiceBusClient as AsyncServiceBusClient
    from azure.servicebus._common.message import ServiceBusReceivedMessage
    from azure.servicebus.exceptions import MessageNotFoundError
except ImportError:
    "Will not be able to use the listener"
    pass


from .filing import Filing

logger = logging.getLogger(__name__)

TOPIC = "filings"

CONNECTION_STRING = "Endpoint=sb://calcbench.servicebus.windows.net/;SharedAccessKeyName=public;SharedAccessKey=Cb7VhLR6eJxsurCSPtXQHOJvlkU84CCCx2oB+T/so6Q="


def handle_filings(
    handler: Callable[[Filing], None],
    connection_string: str = CONNECTION_STRING,
    subscription_name: Optional[str] = None,
    topic=TOPIC,
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
        >>> subscription = "talk to calcbench to get a subscription"
        >>> handle_filings(
        >>>     filing_handler,
        >>>     subscription_name=subscription,
        >>> )
    """
    if not subscription_name:
        raise ValueError("Need to supply subscription_name")
    last_deferred_check = datetime.now()
    with AutoLockRenewer() as renewer:
        client = ServiceBusClient.from_connection_string(
            conn_str=connection_string, debug=False
        )
        with client:
            receiver = client.get_subscription_receiver(
                topic_name=topic,
                subscription_name=subscription_name,
                auto_lock_renewer=renewer,
            )
            with receiver:
                message: ServiceBusReceivedMessage
                # Getting deferred messages before starting the iterator works better
                for message in _get_deferred_messages(receiver):
                    _process_message(
                        handler=handler, message=message, receiver=receiver
                    )
                for message in receiver:
                    _process_message(
                        handler=handler, message=message, receiver=receiver
                    )

                    # Every two minutes check if there are any deferred messages and try to process them
                    if (datetime.now() - last_deferred_check) > timedelta(minutes=2):
                        try:
                            deferred_messages = _get_deferred_messages(receiver)
                        except Exception:
                            logger.exception("exception getting deferred messages")
                        else:
                            for message in deferred_messages:
                                _process_message(
                                    handler=handler, message=message, receiver=receiver
                                )
                        finally:
                            last_deferred_check = datetime.now()


def _process_message(
    handler: Callable[[Filing], None],
    message: "ServiceBusReceivedMessage",
    receiver: "ServiceBusReceiver",
):
    body_bytes = b"".join(cast(Iterable[bytes], message.body))
    try:
        body_json = json.loads(body_bytes)
        filing = Filing(**body_json)
    except Exception:
        logger.exception(f"Exception Parsing {body_bytes}")
        receiver.dead_letter_message(message)
    else:
        try:
            logger.info(f"Handling {message}")
            handler(filing)
        except Exception:
            logger.exception(
                f"Exception Processing {message}\n delivery count: {message.delivery_count}, deferring",
            )
            receiver.defer_message(message=message)
        else:
            receiver.complete_message(message)
            logger.debug(f"completed message {message}")


def _get_deferred_messages(receiver: "ServiceBusReceiver"):
    """
    This is the best I could do to figure out how to get deferred messages without maintaining state on the client.

    I asked about this on stackover flow @ https://stackoverflow.com/questions/70974772/servicebus-retry-logic

    If MSFT ever comes up with a fix for https://github.com/Azure/azure-sdk-for-python/issues/22918 this should be replaced.

    """
    number_peeked = 0
    logger.debug("Looking for deferred messages")
    peeked_messages = receiver.peek_messages(max_message_count=10)
    while len(peeked_messages) > 0:
        logger.debug(
            f"Found {','.join(str(m.sequence_number) for m in peeked_messages)} messages in the queue that might be deferred"
        )
        sequence_number: int = 0
        for peeked_message in peeked_messages:
            number_peeked += 1
            sequence_number = cast(int, peeked_message.sequence_number)
            enqueued_time = cast(datetime, peeked_message.enqueued_time_utc)
            delivery_count = cast(int, peeked_message.delivery_count)
            time_in_queue = datetime.now(timezone.utc) - enqueued_time
            minutes_to_wait = 4**delivery_count
            retry_wait = min(timedelta(minutes=minutes_to_wait), timedelta(days=1))
            logger.debug(
                f"Processing message seq # {sequence_number}, enqued (UTC) @ {enqueued_time}, delivery count {delivery_count} {peeked_message}"
            )
            if delivery_count == 0:
                continue
            if time_in_queue > retry_wait:
                try:
                    deferred_message = receiver.receive_deferred_messages(
                        sequence_number
                    )[0]
                except MessageNotFoundError:
                    # This is fine.  If the messages are not deferred getting them with receive_deferred_messages will not work.  This happens for old messages.
                    logger.debug(
                        f"message not found for message seq # {sequence_number}"
                    )
                    pass
                else:
                    logger.debug(
                        f"found deferred message seq # {sequence_number} {peeked_message}"
                    )
                    yield deferred_message
            else:
                logger.debug(f"delay time not met for {sequence_number}")
        peeked_messages = receiver.peek_messages(
            max_message_count=10,
            sequence_number=sequence_number + 1,
        )
    logger.debug(f"Done looking for deferred messages, peeked {number_peeked}")


async def handle_filings_async(
    handler: Callable[[Filing], None],
    connection_string: str = CONNECTION_STRING,
    subscription_name: Optional[str] = None,
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
