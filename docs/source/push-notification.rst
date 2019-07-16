Push Notifications
==================

Recieve notification when Calcbench has processed new SEC filings.  Useful for keeping your database up-to-date at low-latency.

.. warning::
    This requires Calcbench to create a subscription for you.

Requires the ``azure-servicebus`` package::

    $  pip install azure-servicebus

.. autofunction:: calcbench.handle_filings