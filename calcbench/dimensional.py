from dataclasses import dataclass
from typing import Callable, Dict, Literal, Sequence, Union

import pandas as pd

from calcbench.api_client import (
    CompanyIdentifiers,
    PeriodArgument,
    PeriodType,
    _json_POST,
)
from calcbench.standardized_numeric import (
    StandardizedPoint,
    _build_annual_period,
    _build_quarter_period,
)


class DimensionalDataPoint(StandardizedPoint):
    """
    The data returned by calls to the dimensional api end-point

    """

    container: str
    dimensions: Dict[str, str]


SEGMENTS = Literal[
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
]


def dimensional(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[SEGMENTS] = [],
    start_year: int = None,
    start_period: PeriodArgument = None,
    end_year: int = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = True,
) -> "pd.DataFrame":
    raw_data = dimensional_raw(
        company_identifiers=company_identifiers,
        metrics=metrics,
        start_year=start_year,
        start_period=start_period,
        end_year=end_year,
        end_period=end_period,
        period_type=period_type,
        all_history=all_history,
    )
    build_period: Callable[[DimensionalDataPoint, bool], Union[str, "pd.Period"]]
    if period_type == PeriodType.Annual:
        build_period = _build_annual_period
    elif period_type == PeriodType.Quarterly:
        build_period = _build_quarter_period
    else:
        build_period = lambda d, _: f"{d['fiscal_year']}-{d['fiscal_period']}"

    raw_data = [
        {
            **d,
            **[
                {"axis": axis, "member": member}
                for axis, member in d["dimensions"].items()
            ][0],
            **{"fiscal_period": build_period(d, use_fiscal_period=True)},
        }
        for d in raw_data
    ]

    return pd.DataFrame(raw_data).set_index(
        [
            "ticker",
            "axis",
            "metric",
            "member",
            "fiscal_period",
        ]
    )[["value"]]


def dimensional_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[SEGMENTS] = [],
    start_year: int = None,
    start_period: PeriodArgument = None,
    end_year: int = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = True,
) -> Sequence[DimensionalDataPoint]:
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
            "periodType": period_type,
            "asOriginallyReported": False,
            "allHistory": all_history,
        },
        "pageParameters": {
            "metrics": metrics,
            "dimensionName": "Segment",
            "AsOriginallyReported": False,
        },
    }
    return _json_POST("dimensionalData", payload)
