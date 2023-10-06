try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

import dataclasses
import itertools
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Generator, Optional
from calcbench.api_query_params import CompanyIdentifiers

from calcbench.standardized_numeric import StandardizedPoint

from .api_client import _json_POST, _try_parse_timestamp


@dataclass
class IntangibleCategory(dict):
    category: str
    useful_life_upper_range: float
    useful_life_lower_range: float
    value: float

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for name in names:
            setattr(self, name, None)
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)
            self[k] = v


@dataclass
class BusinessCombination(dict):
    acquisition_date: datetime
    date_reported: datetime
    date_originally_reported: datetime
    parent_company: str
    parent_company_state: str
    parent_company_SIC_code: str
    parent_company_ticker: str
    purchase_price: StandardizedPoint
    trace_link: str
    intangible_categories: Dict[str, IntangibleCategory]
    standardized_PPA_points: Dict[str, StandardizedPoint]
    target: str
    enterprise_value: float

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


STANDARDIZED_METRICS = [
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
    "BusinessCombinationAssetsOtherAssets",
    "BusinessCombinationLiabilitiesAssumedOtherLiabilities",
]

FINITE_LIVED_INTANGIBLE_ASSETS = [
    "FinitelivedIntangibleAssetsAcquired_Customer",
    "FinitelivedIntangibleAssetsAcquired_RandD",
    "FinitelivedIntangibleAssetsAcquired_IP",
    "FinitelivedIntangibleAssetsAcquired_Tech",
    "FinitelivedIntangibleAssetsAcquired_Contracts",
    "FinitelivedIntangibleAssetsAcquired_Licenses",
    "FinitelivedIntangibleAssetsAcquired_Marketing",
]


COLUMNS = [
    "acquisition_date",
    "date_reported",
    "date_originally_reported",
    "target",
    "parent_company",
    "parent_company_state",
    "parent_company_ticker",
    "purchase_price",
    "enterprise_value",
]

USEFUL_LIFE_LOW_COLUMN_LABEL = "useful_life_low"
USEFUL_LIFE_HIGH_COLUMN_LABEL = "useful_life_high"


def business_combinations_raw(
    company_identifiers: Optional[CompanyIdentifiers] = [],
    accession_id: Optional[int] = None,
) -> Generator[BusinessCombination, None, None]:
    """Purchase price allocation for mergers and acquisitions.

    :param company_identifiers: Companies for which to retrieve data
    :param accession_id: Calcbench accession(filing) id  for which to retrieve data.  Get data for one filing.

    """
    payload = {
        "companiesParameters": {"companyIdentifiers": company_identifiers},
        "periodParameters": {"accessionID": accession_id},
    }
    for combination in _json_POST("businessCombinations", payload):
        yield BusinessCombination(**combination)


def business_combinations(
    company_identifiers: Optional[CompanyIdentifiers] = [],
    accession_id: Optional[int] = None,
) -> "pd.DataFrame":
    """Purchase price allocation for mergers and acquisitions.

    Columns are standardized metrics.

    :param company_identifiers: Companies for which to retrieve data
    :param accession_id: Calcbench accession(filing) id  for which to retrieve data.  Get data for one filing.

    """
    data = business_combinations_raw(
        company_identifiers=company_identifiers, accession_id=accession_id
    )
    rows = []
    for datum in data:
        row = {key: datum.get(key) for key in COLUMNS}
        for metric in STANDARDIZED_METRICS:
            standardized_point = datum.standardized_PPA_points.get(metric)
            if standardized_point:
                row[metric] = standardized_point.get("value")
        for asset_category in FINITE_LIVED_INTANGIBLE_ASSETS:
            intangible_category = datum.intangible_categories.get(asset_category)
            if intangible_category:
                row[asset_category] = intangible_category.value
                row[
                    f"{asset_category}_{USEFUL_LIFE_LOW_COLUMN_LABEL}"
                ] = intangible_category.useful_life_lower_range
                row[
                    f"{asset_category}_{USEFUL_LIFE_HIGH_COLUMN_LABEL}"
                ] = intangible_category.useful_life_upper_range
        row["purchase_price"] = datum.purchase_price.get("value")
        row["enterprise_value"] = datum.enterprise_value
        rows.append(row)
    intangible_colums = list(
        itertools.chain.from_iterable(
            [
                [
                    asset_category,
                    f"{asset_category}_{USEFUL_LIFE_LOW_COLUMN_LABEL}",
                    f"{asset_category}_{USEFUL_LIFE_HIGH_COLUMN_LABEL}",
                ]
                for asset_category in FINITE_LIVED_INTANGIBLE_ASSETS
            ]
        )
    )
    df_columns = COLUMNS + intangible_colums + STANDARDIZED_METRICS
    df = pd.DataFrame(data=rows, columns=df_columns)
    for date_column in [
        "acquisition_date",
        "date_reported",
        "date_originally_reported",
    ]:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")  # type: ignore
    return df
