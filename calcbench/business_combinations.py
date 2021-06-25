try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

import dataclasses
import itertools
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Generator

from calcbench.standardized_numeric import StandardizedPoint

from .api_client import CompanyIdentifiers, _json_POST, _try_parse_timestamp


@dataclass
class IntangibleCategory(dict):
    category: str
    useful_life_upper_range: float
    useful_life_lower_range: float
    amount: float

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
    company_identifiers: CompanyIdentifiers = [], accession_id: int = None
) -> Generator[BusinessCombination, None, None]:
    """Data about mergers and acquisitions.

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
    company_identifiers: CompanyIdentifiers = [], accession_id: int = None
) -> "pd.DataFrame":
    """Data about mergers and acquisitions.

    Columns are standardized metrics.

    :param company_identifiers: Companies for which to retrieve data
    :param accession_id: Calcbench accession(filing) id  for which to retrieve data.  Get data for one filing.

    """
    data = business_combinations_raw(
        company_identifiers=company_identifiers, accession_id=accession_id
    )
    rows = []
    for datum in data:
        row = {key: datum[key] for key in COLUMNS}
        for metric in STANDARDIZED_METRICS:
            standardized_point = datum.standardized_PPA_points.get(metric)
            if standardized_point:
                row[metric] = standardized_point.get("value")
        for asset_category in FINITE_LIVED_INTANGIBLE_ASSETS:
            intangible_category = datum.intangible_categories.get(asset_category)
            if intangible_category:
                row[asset_category] = intangible_category.amount
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
    return df


DB_NAMES_TO_LEGACY_COLUMNS = {
    "FinitelivedIntangibleAssetsAcquired_Customer": "Intangible Assets Acquired, Customer",
    "FinitelivedIntangibleAssetsAcquired_Customer_useful_life_low": "Intangible Assets Acquired, Customer Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Customer_useful_life_high": "Intangible Assets Acquired, Customer Est Useful Life - High (or Mid) (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_RandD": "Intangible Assets Acquired, R&D",
    "FinitelivedIntangibleAssetsAcquired_RandD_useful_life_low": "Intangible Assets Acquired, R&D Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_RandD_useful_life_high": "Intangible Assets Acquired, R&D Est Useful Life - High (or Mid) (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_IP": "Intangible Assets Acquired, IP",
    "FinitelivedIntangibleAssetsAcquired_IP_useful_life_low": "Intangible Assets Acquired, IP Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_IP_useful_life_high": "Intangible Assets Acquired, IP Est Useful Life - High (or Mid) (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Tech": "Intangible Assets Acquired, Technology",
    "FinitelivedIntangibleAssetsAcquired_Tech_useful_life_low": "Intangible Assets Acquired, Technology Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Tech_useful_life_high": "Intangible Assets Acquired, Technology Est Useful Life - High (or Mid) (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Contracts": "Intangible Assets Acquired, Contracts",
    "FinitelivedIntangibleAssetsAcquired_Contracts_useful_life_low": "Intangible Assets Acquired, Contracts Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Contracts_useful_life_high": "Intangible Assets Acquired, Contracts Est Useful Life - High (or Mid) (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Licenses": "Intangible Assets Acquired, Rights & Licenses",
    "FinitelivedIntangibleAssetsAcquired_Licenses_useful_life_low": "Intangible Assets Acquired, Rights & Licenses Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Licenses_useful_life_high": "Intangible Assets Acquired, Rights & Licenses Est Useful Life - High (or Mid) (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Marketing": "Intangible Assets Acquired, Marketing",
    "FinitelivedIntangibleAssetsAcquired_Marketing_useful_life_low": "Intangible Assets Acquired, Marketing Est Useful Life - Low (Yrs)",
    "FinitelivedIntangibleAssetsAcquired_Marketing_useful_life_high": "Intangible Assets Acquired, Marketing Est Useful Life - High (or Mid) (Yrs)",
    "BusinessCombinationAssetsAcquiredCashAndEquivalents": "Business Combination, Assumed Cash And Equivalents",
    "BusinessCombinationAssetsAcquiredReceivables": "Business Combination, Assumed Accounts Receivable",
    "BusinessCombinationAssetsAcquiredInventory": "Business Combination, Assumed Inventory",
    "BusinessCombinationAssetsDeferredTaxAssetsCurrent": "Business Combination, Deferred Tax Assets, Current",
    "BusinessCombinationAssetsMarketableSecurities": "Business Combination, Marketable Securities",
    "BusinessCombinationAssetsPrepaidExpense": "Business Combination, Prepaid Expense",
    "BusinessCombinationAssetsAcquiredPropertyPlantAndEquipment": "Business Combination, Assumed Property Plant And Equipment",
    "BusinessCombinationAssetsAquiredGoodwill": "Business Combination, Portion Of Purchase Price Allocated To Goodwill",
    "FinitelivedIntangibleAssetsAcquired": "Finite Lived Intangible Assets Acquired",
    "IndefinitelivedIntangibleAssetsAcquired": "Indefinite Lived Intangible Assets Acquired",
    "BusinessCombinationAssetsDeferredTaxAssetsNoncurrent": "Business Combination, Deferred Tax Assets, Noncurrent",
    "BusinessCombinationAssetsFinancialAssets": "Business Combination, Financial Assets",
    "BusinessCombinationAssetsOtherAssets": "BusinessCombination AssetsOtherAssets",
    "BusinessCombinationRecognizedIdentifiableAssetsAcquiredAndLiabilitiesAssumedAssets": "Business Combination, Assumed Assets",
    "BusinessCombinationLiabilitiesAssumedCurrentLiabilitiesAccountsPayable": "Business Combination, Assumed Accounts Payable",
    "BusinessCombinationLiabilitiesAssumedDeferredRevenue": "Business Combination, Liabilities Assumed Deferred Revenue",
    "BusinessCombinationLiabilitiesAssumedCurrentDeferredRevenue": "Business Combination, Deferred Revenue, Current",
    "BusinessCombinationLiabilitiesAssumedDeferredTaxLiabilitiesCurrent": "Business Combination, Deferred Tax Liabilities Current",
    "BusinessCombinationLiabilitiesAssumedCurrentLongTermDebt": "Business Combination, Current Portion of Long Term Debt",
    "BusinessCombinationLiabilitiesAssumedDeferredTaxLiabilitiesNoncurrent": "Business Combination, Deferred Tax Liabilities Noncurrent",
    "BusinessCombinationLiabilitiesAssumedLongTermDebt": "Business Combination, Assumed Long Term Debt",
    "BusinessCombinationLiabilitiesAssumedCapitalLeaseObligation": "Business Combination, Capital Lease Obligation",
    "BusinessCombinationLiabilitiesAssumedContingentLiability": "Business Combination, Contingent Liability",
    "BusinessCombinationLiabilitiesAssumedFinancialLiabilities": "Business Combination, Financial Liabilities",
    "BusinessCombinationLiabilitiesAssumedRestructuringLiabilities": "Business Combination, Restructuring Liabilities",
    "BusinessCombinationLiabilitiesAssumedOtherLiabilities": "BusinessCombination LiabilitiesAssumedOtherLiabilities",
    "BusinessCombinationLiabilitiesAssumed": "Business Combination, Assumed Liabilities",
    "BusinessCombinationAssetsAcquiredAndLiabilitiesAssumedNet": "Business Combination, Assets Acquired And Liabilities Assumed, Net (Pre GoodWill)",
    "BusinessCombinationAcquiredGoodwillAndLiabilitiesAssumedNet": "Business Combination, Assets Acquired, Goodwill, And Liabilities Assumed, Net",
    "BusinessCombinationNoncontrollingInterest": "Business Combination, Noncontrolling Interest",
    "BusinessCombinationAcquiredGoodwillAndLiabilitiesAssumedLessNoncontrollingInterest": "Business Combination, Assets Acquired, Goodwill, And Liabilities Assumed, Less Noncontrolling Interest",
}


def legacy_report(
    company_identifiers: CompanyIdentifiers = [], accession_id: int = None
):
    """
    This is used by a Calcbench client that shall not be named.
    """
    data = business_combinations(
        company_identifiers=company_identifiers, accession_id=accession_id
    )
    data = data.rename(DB_NAMES_TO_LEGACY_COLUMNS, axis=1)
    for column in DB_NAMES_TO_LEGACY_COLUMNS.values():
        data[f"{column} As % of Purchase Price"] = data[column] / data.purchase_price
    return data
