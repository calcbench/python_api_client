Low-latency New Data Notification
=================================

Low-latency notification when Calcbench processes new filings from the SEC and publishes data.

Calcbench pushes messages onto a queue when we publish new data.  Your process listens on the queue.  
When you recieve a message you call the Calcbench API to retrieve data then add the data to your pipeline.

Implement the listener as below in a script that runs in an always on daemon managed by something like :code:`systemd`.

If your system is down, messages will remain in the queue for 7 days.  If you process throws an exception prior to completion the messages will be put back on the queue.

.. warning::
    This requires Calcbench to create a subscription for you.

Requires the ``azure-servicebus`` package::

    $  pip install azure-servicebus

.. autofunction:: calcbench.handle_filings
.. autoclass:: calcbench.filing.Filing
    :noindex: