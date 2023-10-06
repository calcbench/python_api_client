from calcbench.api_client import _json_GET
from calcbench.api_query_params import CompanyIdentifier, PeriodType
from calcbench.models.face import FaceStatement, StatementType


def face_statement(
    company_identifier: CompanyIdentifier,
    statement_type: StatementType,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = False,
    descending_dates: bool = False,
) -> FaceStatement:
    """Face Statements.

    face statements as reported by the filing company


    :param company_identifier: a ticker or a CIK code, eg 'msft'
    :param statement_type: one of ('income', 'balance', 'cash', 'change-in-equity', 'comprehensive-income')
    :param period_type: annual|quarterly|cummulative|combined
    :param all_periods: get all history or only the last four, True or False.
    :param descending_dates: return columns in oldest -> newest order.
    :return: Data for the statement

    Usage::
        >>> calcbench.face_statement('msft', 'income')

    """

    payload = {
        "companyIdentifier": company_identifier,
        "statementType": statement_type,
        "periodType": period_type,
        "allPeriods": all_history,
        "descendingDates": descending_dates,
    }
    data = _json_GET("api/faceStatement", payload)

    return FaceStatement(**data)
