import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Sequence
import itertools

try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

from calcbench.standardized_numeric import StandardizedPoint

from calcbench.api_client import (
    CompanyIdentifiers,
    PeriodArgument,
    PeriodType,
    _json_POST,
    _try_parse_timestamp,
)


def dimensional_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: int = None,
    start_period: PeriodArgument = None,
    end_year: int = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
):
    """Segments and Breakouts

    The data behind the breakouts/segment page, https://www.calcbench.com/breakout.

    :param sequence company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param sequence metrics: list of dimension tuple strings, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName"
    :param int start_year: first year of data to get
    :param int start_period: first period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param int end_year: last year of data to get
    :param int end_period: last period of data to get. 0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param str period_type: 'quarterly' or 'annual', only applicable when other period data not supplied.
    :return: A list of points.  The points correspond to the lines @ https://www.calcbench.com/breakout.  For each requested metric there will be a the formatted value and the unformatted value denote bya  _effvalue suffix.  The label is the dimension label associated with the values.
    :rtype: sequence

    Usage::
      >>> cb.dimensional_raw(company_identifiers=['fdx'], metrics=['OperatingSegmentRevenue'], start_year=2018)

    """
    if len(metrics) == 0:
        raise (ValueError("Need to supply at least one breakout."))
    if period_type not in ("annual", "quarterly"):
        raise (ValueError("period_type must be in ('annual', 'quarterly')"))

    payload = {
        "companiesParameters": {
            "entireUniverse": len(company_identifiers) == 0,
            "companyIdentifiers": company_identifiers,
        },
        "periodParameters": {
            "year": end_year or start_year,
            "period": start_period,
            "endYear": start_year,
            "periodType": period_type,
            "asOriginallyReported": False,
        },
        "pageParameters": {
            "metrics": metrics,
            "dimensionName": "Segment",
            "AsOriginallyReported": False,
        },
    }
    return _json_POST("dimensionalData", payload)


@dataclass
class IntangibleCategory(dict):
    category: str
    useful_life_upper_range: float
    useful_life_lower_range: float
    amount: float

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


@dataclass
class BusinessCombination(dict):
    acquisition_date: datetime
    date_reported: datetime
    date_originally_reported: datetime
    parent_company: str
    parent_company_state: str
    parent_company_SIC_code: str
    parent_company_ticker: str
    purchase_price: dict
    trace_link: str
    intangible_categories: Dict[str, IntangibleCategory]
    standardized_PPA_points: Dict[str, StandardizedPoint]
    target: str

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for name in names:
            setattr(self, name, None)
        for k, v in kwargs.items():
            if k in ("acquisition_date", "date_reported", "date_originally_repoted"):
                setattr(self, k, _try_parse_timestamp(v))
            elif k == "intangible_categories":
                setattr(
                    self,
                    k,
                    {
                        category: IntangibleCategory(**value)
                        for category, value in v.items()
                    },
                )
            elif k in names:
                setattr(self, k, v)
            self[k] = v


def business_combinations_raw(
    company_identifiers: CompanyIdentifiers = [], accession_id: int = None
):
    payload = {
        "companiesParameters": {"companyIdentifiers": company_identifiers},
        "periodParameters": {"accessionID": accession_id},
    }
    for combination in _json_POST("businessCombinations", payload):
        yield BusinessCombination(**combination)


standardized_metrics = [
    "BusinessCombinationAssetsAcquiredCashAndEquivalents",
    "BusinessCombinationAssetsAcquiredReceivables",
    "BusinessCombinationAssetsAcquiredInventory",
    "BusinessCombinationAssetsDeferredTaxAssetsCurrent",
    "BusinessCombinationAssetsMarketableSecurities",
    "BusinessCombinationAssetsPrepaidExpense",
    "BusinessCombinationAssetsAcquiredPropertyPlantAndEquipment",
    "BusinessCombinationAssetsAquiredGoodwill",
    "FinitelivedIntangibleAssetsAcquired",
    "IndefinitelivedIntangibleAssetsAcquired",
    "BusinessCombinationAssetsDeferredTaxAssetsNoncurrent",
    "BusinessCombinationAssetsFinancialAssets",
    "BusinessCombinationRecognizedIdentifiableAssetsAcquiredAndLiabilitiesAssumedAssets",
    "BusinessCombinationLiabilitiesAssumedCurrentLiabilitiesAccountsPayable",
    "BusinessCombinationLiabilitiesAssumedDeferredRevenue",
    "BusinessCombinationLiabilitiesAssumedCurrentDeferredRevenue",
    "BusinessCombinationLiabilitiesAssumedDeferredTaxLiabilitiesCurrent",
    "BusinessCombinationLiabilitiesAssumedCurrentLongTermDebt",
    "BusinessCombinationLiabilitiesAssumedDeferredTaxLiabilitiesNoncurrent",
    "BusinessCombinationLiabilitiesAssumedLongTermDebt",
    "BusinessCombinationLiabilitiesAssumedCapitalLeaseObligation",
    "BusinessCombinationLiabilitiesAssumedContingentLiability",
    "BusinessCombinationLiabilitiesAssumedFinancialLiabilities",
    "BusinessCombinationLiabilitiesAssumedRestructuringLiabilities",
    "BusinessCombinationLiabilitiesAssumed",
    "BusinessCombinationAssetsAcquiredAndLiabilitiesAssumedNet",
    "BusinessCombinationAcquiredGoodwillAndLiabilitiesAssumedNet",
    "BusinessCombinationNoncontrollingInterest",
    "BusinessCombinationAcquiredGoodwillAndLiabilitiesAssumedLessNoncontrollingInterest",
]

finite_lived_intangible_assets = [
    "FinitelivedIntangibleAssetsAcquired_Customer",
    "FinitelivedIntangibleAssetsAcquired_RandD",
    "FinitelivedIntangibleAssetsAcquired_IP",
    "FinitelivedIntangibleAssetsAcquired_Tech",
    "FinitelivedIntangibleAssetsAcquired_Contracts",
    "FinitelivedIntangibleAssetsAcquired_Licenses",
    "FinitelivedIntangibleAssetsAcquired_Marketing",
]


columns = [
    "acquisition_date",
    "date_reported",
    "target",
    "parent_company",
    "parent_company_state",
    "parent_company_ticker",
]

USEFUL_LIFE_HIGH_COLUMN_LABEL = "useful_life_high"
USEFUL_LIFE_LOW_COLUMN_LABEL = "useful_life_low"


def business_combinations(
    company_identifiers: CompanyIdentifiers = [], accession_id: int = None
) -> "pd.DataFrame":

    data = business_combinations_raw(
        company_identifiers=company_identifiers, accession_id=accession_id
    )
    rows = []
    for datum in data:
        row = {key: datum[key] for key in columns}
        for metric in standardized_metrics:
            value = datum.standardized_PPA_points.get(metric)
            if value:
                row[metric] = value["value"]
        for asset_category in finite_lived_intangible_assets:
            value = datum.intangible_categories.get(asset_category)
            if value:
                row[asset_category] = value.amount
                row[
                    f"{asset_category}_{USEFUL_LIFE_HIGH_COLUMN_LABEL}"
                ] = value.useful_life_upper_range
                row[
                    f"{asset_category}_{USEFUL_LIFE_LOW_COLUMN_LABEL}"
                ] = value.useful_life_lower_range
        rows.append(row)

    df = pd.DataFrame(
        data=rows,
        columns=columns
        + standardized_metrics
        + list(
            itertools.chain.from_iterable(
                [
                    [
                        asset_category,
                        f"{asset_category}_{USEFUL_LIFE_HIGH_COLUMN_LABEL}",
                        f"{asset_category}_{USEFUL_LIFE_LOW_COLUMN_LABEL}",
                    ]
                    for asset_category in finite_lived_intangible_assets
                ]
            )
        ),
    )
    return df
