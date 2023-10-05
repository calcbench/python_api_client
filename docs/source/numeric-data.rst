Standardized Numeric Financials
===============================

Calcbench extracts all of the GAAP numbers in section 8, face statments and footnotes, of the 10-K/Qs.  Face financials from earnings press-releases and 8-Ks are also included.

Standardized
------------

Calcbench standardizes +1000 metrics to handle differences in filers's tagging.  The list of stardized points is @ https://www.calcbench.com/home/standardizedmetrics

.. autofunction:: calcbench.standardized
.. autofunction:: calcbench.standardized_raw

.. autoclass:: calcbench.api_query_params.Period
    :members:
    :undoc-members:
    :noindex:


.. autoclass:: calcbench.api_query_params.PeriodType
    :members:
    :undoc-members:
    :noindex:

.. autoclass:: calcbench.api_query_params.CompanyIdentifierScheme
    :members:
    :undoc-members:

.. autoclass:: calcbench.api_query_params.PeriodType

.. autoclass:: calcbench.models.standardized
    :members:
    :undoc-members:

.. autoclass:: calcbench.models.trace