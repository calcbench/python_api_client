Getting Started
===============

.. _install:

Installing the Client
---------------------


Install the Calcbench client from pip::

    $ pip install calcbench-api-client

Obtain Credentials
------------------

The API uses the same credentials as calcbench.com.  If you do not have Calcbench credentials you can sign up for free two-week trial @ https://www.calcbench.com/join.

.. warning::
     Talk to us before you start coding, Calcbench data is not free.

Set Credentials
---------------

.. autofunction:: calcbench.set_credentials

Error Retry
-----------

.. autofunction:: calcbench.enable_backoff