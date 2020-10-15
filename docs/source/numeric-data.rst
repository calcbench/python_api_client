Numeric Data
============

Calcbench extracts all of the GAAP numbers in section 8, face statments and footnotes, of the 10-K/Qs.

Standardized
------------

Calcbench standardizes +1000 metrics to handle differences filers's tagging.  The list of stardized points is @ https://www.calcbench.com/home/standardizedmetrics

.. autofunction:: calcbench.standardized_data

Point-In-Time
-------------

Our standardized data with timestamps.  Useful for backtesting quantitative strategies.

.. autofunction:: calcbench.point_in_time

Raw XBRL Data
-------------

Data as reported in the XBRL documents

.. autofunction:: calcbench.raw_xbrl_raw

Dimensional
-----------

Segments: geographic and operating, and other dimensionalized tabular data.

.. autofunction:: calcbench.dimensional_raw


.. autoclass:: calcbench.api_client.Period
    :members:
    :undoc-members:
    :noindex:


.. autoclass:: calcbench.api_client.PeriodType
    :members:
    :undoc-members:
    :noindex:

.. autoclass:: calcbench.api_client.CompanyIdentifierScheme
    :members:
    :undoc-members:

.. autoclass:: calcbench.api_client.PeriodType