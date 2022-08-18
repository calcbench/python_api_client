Getting Started
===============

.. warning::
     Calcbench data is not free and API access is not included in the standard Calcbench subscription.

.. _install:

Installing the Client
---------------------


Install the Calcbench client from pip::

    $ pip install calcbench-api-client

Obtain Credentials
------------------

The API uses the same credentials as calcbench.com.  If you do not have Calcbench credentials you can sign up for free two-week trial @ https://www.calcbench.com/join.

Save Credentials
----------------

Credentials can be stored in the "CALCBENCH_USERNAME" and "CALCBENCH_PASSWORD" environment variables OR set the calcbench_api credentials on your Keychain|Secret Service|Windows Credential Locker

Set Credentials At Runtime
--------------------------

Call cb.set_credentials('user@calcbench.com', 'mypassword')

On an interactive shell the package will ask for credentials as input




Set Credentials
---------------

.. autofunction:: calcbench.set_credentials

Error Retry
-----------

.. autofunction:: calcbench.enable_backoff

Network Proxy
-------------
.. autofunction:: calcbench.set_proxies


Logging
-------
.. autofunction:: calcbench.turn_on_logging