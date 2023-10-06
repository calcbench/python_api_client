from enum import Enum
from typing import Dict, Optional, Sequence

try:
    import pandas as pd
except ImportError:
    pass

from calcbench.api_client import (
    _json_POST,
)
from calcbench.api_query_params import CompanyIdentifiers, PeriodArgument, PeriodType
from calcbench.standardized_numeric import (
    StandardizedPoint,
)


class DimensionalDataPoint(StandardizedPoint):
    """
    The data returned by calls to the dimensional api end-point

    """

    container: str
    dimensions: Dict[str, str]


Metric = Enum(
    "Metric",
    [
        "OperatingSegmentRevenue",
        "OperatingSegmentAssets",
        "OperatingSegmentLongLivedAssets",
        "OperatingSegmentCapitalExpenditures",
        "OperatingSegmentDepreciation",
        "OperatingSegmentOperatingIncome",
        "OperatingSegmentInterestExpense",
        "OperatingSegmentGoodwill",
        "GeographicalSegmentRevenue",
        "GeographicalSegmentAssets",
        "GeographicalSegmentLongLivedAssets",
        "GeographicalSegmentCapitalExpenditures",
        "GeographicalSegmentDepreciation",
        "GeographicalSegmentGeographicalIncome",
        "GeographicalSegmentInterestExpense",
        "GeographicalSegmentGoodwill",
        "DeferredTaxAssets",
        "DeferredTaxLiabilities",
        "IncomeTaxReconciliation",
        "EffectiveIncomeTaxReconciliation",
        "AssetsAndLiabilitiesAtFairValue",
        "AssetsAndLiabilitiesAtFairValueLevel1",
        "AssetsAndLiabilitiesAtFairValueLevel2",
        "AssetsAndLiabilitiesAtFairValueLevel3",
        "FairValueOfPensionPlanAssets",
        "FairValueOfPensionPlanAssetsLevel1",
        "FairValueOfPensionPlanAssetsLevel2",
        "FairValueOfPensionPlanAssetsLevel3",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetTransfersIntoLevel3",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetTransfersOutOfLevel3",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetGainLossIncludedInEarnings",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetGainLossIncludedInOtherComprehensiveIncomeLoss",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetPurchases",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetSales",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetSettlements",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetIssues",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetValue",
        "FairValueMeasurementWithUnobservableInputsReconciliationLiabilityTransfersIntoLevel3",
        "FairValueMeasurementWithUnobservableInputsReconciliationLiabilityTransfersOutOfLevel3",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilityGainLossIncludedInEarnings",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilityGainLossIncludedInOtherComprehensiveIncome",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilityPurchases",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilitySales",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilitySettlements",
        "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisLiabilityIssues",
        "FairValueMeasurementWithUnobservableInputsReconciliationsRecurringBasisLiabilityValue",
        "DebtInstrumentFaceAmount",
        "DebtInstrumentUnamortizedDiscountPremiumNet",
        "DebtInstrumentCarryingAmount",
        "DebtInstrumentInterestRateStatedPercentage",
        "DebtInstrumentInterestRateEffectivePercentage",
        "DebtInstrumentMaturityDate",
        "DebtInstrumentMaturityYear",
        "DerivativeNotionalAmount",
        "DerivativeAssetNotionalAmount",
        "DerivativeLiabilityNotionalAmount",
        "DerivativeFairValueOfDerivativeAsset",
        "DerivativeFairValueOfDerivativeLiability",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsGrantsInPeriod",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsForfeitedInPeriod",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsVestedInPeriod",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedWeightedAverageGrantDateFairValue",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsGrantsInPeriodWeightedAverageGrantDateFairValue",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsVestedInPeriodWeightedAverageGrantDateFairValue",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsForfeituresWeightedAverageGrantDateFairValue",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsVestedInPeriodTotalFairValue",
        "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeLowerRangeLimit",
        "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeUpperRangeLimit",
        "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeNumberOfOutstandingOptions",
        "SharebasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptionsWeightedAverageRemainingContractualTerm",
        "SharebasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeOutstandingOptionsWeightedAverageExercisePriceBeginningBalance",
        "ShareBasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeNumberOfExercisableOptions",
        "SharebasedCompensationSharesAuthorizedUnderStockOptionPlansExercisePriceRangeExercisableOptionsWeightedAverageExercisePrice",
        "DisposalGroup",
        "FiniteLivedIntangibleAssetsGross",
        "FiniteLivedIntangibleAssetsAccumulatedAmortization",
        "FiniteLivedIntangibleAssetsNet",
        "ImpairmentOfIntangibleAssetsExcludingGoodwill",
        "IndefiniteLivedIntangibleAssetsExcludingGoodwill",
        "RealEstateAndAccumulatedDepreciationAmountOfEncumbrances",
        "RealEstateAndAccumulatedDepreciationInitialCostOfLand",
        "RealEstateAndAccumulatedDepreciationInitialCostOfBuildingsAndImprovements",
        "RealEstateAndAccumulatedDepreciationCostsCapitalizedSubsequentToAcquisitionCarryingCosts",
        "SECScheduleIIIRealEstateAndAccumulatedDepreciationCostsCapitalizedSubsequentToAcquisitionLand",
        "SECScheduleIIIRealEstateAndAccumulatedDepreciationCostsCapitalizedSubsequentToAcquisitionBuildingsAndImprovements",
        "RealEstateAndAccumulatedDepreciationCarryingAmountOfLand",
        "RealEstateAndAccumulatedDepreciationCarryingAmountOfBuildingsAndImprovements",
        "RealEstateAccumulatedDepreciation",
        "RealEstateGrossAtCarryingValue",
        "ConcentrationRiskPercentageCustomer",
        "ConcentrationRiskPercentageSupplier",
        "BusinessCombinationConsideration",
        "BusinessCombination",
        "BusinessCombinationAdjustment",
        "BusinessCombinationAsAdjusted",
        "BusinessCombinationIntangibleAssetsAcquired",
        "BusinessCombinationIntangibleAssetsAcquiredWeightedAverageUsefulLife",
        "EquityMethodInvestments",
        "IncomeLossFromEquityMethodInvestments",
        "EquityMethodInvestmentDividendsOrDistributions",
        "EquityMethodInvestmentRealizedGainLossOnDisposal",
        "filing_date",
        "BusinessCombinationAcquisitionDate",
        "BusinessCombinationPurchasePrice",
        "PaymentsToAcquireBusinessesGross",
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "BusinessAcquisitionCostOfAcquiredEntityCashPaid",
        "BusinessCombinationConsiderationTransferredIncludingEquityInterestInAcquireeHeldPriorToCombination1",
        "BusinessCombinationConsiderationTransferredEquityInterestsIssuedAndIssuable",
        "BusinessAcquisitionEquityInterestsIssuedOrIssuableNumberOfSharesIssued",
        "BusinessCombinationAcquisitionRelatedCosts",
        "BusinessCombinationContingentConsiderationLiability",
        "BusinessCombinationContingentConsiderationArrangementsRangeOfOutcomesValueLow",
        "BusinessCombinationContingentConsiderationArrangementsRangeOfOutcomesValueHigh",
        "BusinessCombinationAssetsAcquiredCashAndEquivalents",
        "BusinessCombinationAssetsAcquiredReceivables",
        "BusinessCombinationAssetsAcquiredInventory",
        "BusinessCombinationAssetsAcquiredPropertyPlantAndEquipment",
        "BusinessCombinationAssetsAquiredGoodwill",
        "FinitelivedIntangibleAssetsAcquired",
        "IndefinitelivedIntangibleAssetsAcquired",
        "AcquiredFiniteLivedIntangibleAssetsWeightedAverageUsefulLife",
        "BusinessCombinationRecognizedIdentifiableAssetsAcquiredAndLiabilitiesAssumedAssets",
        "BusinessCombinationLiabilitiesAssumedCurrentLiabilitiesAccountsPayable",
        "BusinessCombinationLiabilitiesAssumedLongTermDebt",
        "BusinessCombinationLiabilitiesAssumedDeferredRevenue",
        "BusinessCombinationLiabilitiesAssumed",
        "BusinessCombinationBargainPurchaseGainRecognizedAmount",
        "BusinessCombinationBargainPurchaseGainAdjustment",
        "BusinessAcquisitionsProFormaRevenue",
        "BusinessAcquisitionCostOfAcquiredEntityTransactionCosts",
        "PlanName",
        "DefinedBenefitPlanNetPeriodicBenefitCost",
        "DefinedBenefitPlanServiceCost",
        "DefinedBenefitPlanInterestCost",
        "DefinedBenefitPlanExpectedReturnOnPlanAssets",
        "DefinedBenefitPlanAmortizationOfTransitionObligationsAssets",
        "DefinedBenefitPlanAmortizationOfPriorServiceCostCredit",
        "DefinedBenefitPlanAmortizationOfGainsLoss",
        "DefinedBenefitPlanRecognizedNetGainLossDueToSettlementsAndCurtailments",
        "DefinedBenefitPlanOtherCosts",
        "DefinedBenefitPlanBenefitObligation",
        "DefinedBenefitPlanChangeInBenefitObligationServiceCost",
        "DefinedBenefitPlanChangeInBenefitObligationInterestCost",
        "DefinedBenefitPlanContributionsByPlanParticipants",
        "DefinedBenefitPlanActuarialGainLoss",
        "DefinedBenefitPlanBenefitsPaid",
        "DefinedBenefitPlanDirectBenefitsPaid",
        "DefinedBenefitPlanPlanAmendments",
        "DefinedBenefitPlanBusinessCombinationsAndAcquisitionsBenefitObligation",
        "DefinedBenefitPlanChangeInDiscountRate",
        "DefinedBenefitPlanTransfers",
        "DefinedBenefitPlanForeignCurrencyExchangeRateChangesBenefitObligation",
        "DefinedBenefitPlanDBOOtherChanges",
        "DefinedBenefitPlanAccumulatedBenefitObligation",
        "DefinedBenefitPlanFairValueOfPlanAssets",
        "DefinedBenefitPlanActualReturnOnPlanAssets",
        "DefinedBenefitPlanContributionsByEmployer",
        "DefinedBenefitPlanContributionsByPlanParticipants_Asset",
        "DefinedBenefitPlanBenefitsPaid_Asset",
        "DefinedBenefitPlanPurchasesSalesAndSettlements",
        "DefinedBenefitPlanForeignCurrencyExchangeRateChangesPlanAssets",
        "DefinedBenefitPlanAssetsOtherChanges",
    ],
)


def dimensional(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[Metric] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = True,
    trace_url: bool = False,
    as_originally_reported: bool = False,
) -> "pd.DataFrame":
    """
    Segments and Breakouts in a DataFrame

    The data behind the breakouts/segment page, https://www.calcbench.com/breakout.

    If there are no results an empty dataframe is returned

    :param sequence company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param metrics: list of dimension tuple strings, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName"
    :param int start_year: first year of data to get
    :param start_period: first period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param int end_year: last year of data to get
    :param end_period: last period of data to get. 0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param period_type: only applicable when other period data not supplied.
    :param trace_url: include a column with URL that point to the source document.
    :param as_originally_reported: Show the first reported, rather than revised, values
    :return: A list of points.  The points correspond to the lines @ https://www.calcbench.com/breakout.  For each requested metric there will be a the formatted value and the unformatted value denote bya  _effvalue suffix.  The label is the dimension label associated with the values.
    :rtype: pd.DataFrame

    Usage::

      >>> cb.dimensional(
      >>>   company_identifiers=cb.tickers(index="DJIA"),
      >>>   metrics=["OperatingSegmentRevenue", "OperatingSegmentOperatingIncome"],
      >>>   period_type="annual",
      >>> )

    """
    raw_data = dimensional_raw(
        company_identifiers=company_identifiers,
        metrics=metrics,
        start_year=start_year,
        start_period=start_period,
        end_year=end_year,
        end_period=end_period,
        period_type=period_type,
        all_history=all_history,
        as_originally_reported=as_originally_reported,
    )

    raw_data = [
        {
            **d,
            **[
                {"axis": axis, "member": member}
                for axis, member in d["dimensions"].items()
            ][0],
        }
        for d in raw_data
    ]

    if not raw_data:
        return pd.DataFrame()

    columns = ["value"]
    if trace_url:
        columns = columns + ["trace_url"]
    df = pd.DataFrame(raw_data)
    if period_type == PeriodType.Annual:
        period_index = pd.PeriodIndex(df["fiscal_year"], freq="A")
    elif period_type == PeriodType.Quarterly:
        period_index = pd.PeriodIndex(
            year=df["fiscal_year"], quarter=df["fiscal_period"]
        )
    else:
        period_index = df[["fiscal_year", "fiscal_period"]].apply(
            lambda x: "-".join(x.astype(str).values), axis=1
        )
    df["fiscal_period"] = period_index
    df = df.set_index(
        [
            "ticker",
            "axis",
            "metric",
            "member",
            "fiscal_period",
        ]
    )
    return df[columns]


def dimensional_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[Metric] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = True,
    as_originally_reported: bool = False,
) -> Sequence[DimensionalDataPoint]:
    """Segments and Breakouts

    The data behind the breakouts/segment page, https://www.calcbench.com/breakout.

    :param company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param metrics: list of dimension tuple strings, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName"
    :param start_year: first year of data to get
    :param start_period: first period of data to get.
    :param end_year: last year of data to get
    :param end_period: last period of data to get.
    :param period_type: Only applicable when other period data not supplied.
    :param all_history: Get data for all history
    :param as_originally_reported: Show the first reported, rather than revised, values

    Usage::
      >>> cb.dimensional_raw(company_identifiers=['fdx'],
      >>>   metrics=['OperatingSegmentRevenue'],
      >>>   start_year=2018
      >>> )

    """
    if len(metrics) == 0:
        raise (ValueError("Need to supply at least one metric."))

    payload = {
        "companiesParameters": {
            "entireUniverse": len(company_identifiers) == 0,
            "companyIdentifiers": company_identifiers,
        },
        "periodParameters": {
            "year": end_year or start_year,
            "period": start_period,
            "endYear": start_year,
            "endPeriod": end_period,
            "periodType": period_type,
            "asOriginallyReported": False,
            "allHistory": all_history,
        },
        "pageParameters": {
            "metrics": metrics,
            "dimensionName": "Segment",
            "AsOriginallyReported": as_originally_reported,
        },
    }
    return _json_POST("dimensionalData", payload)
