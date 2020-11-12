import pandas as pd
from calcbench.api_client import _SESSION_STUFF, _calcbench_session

try:
    import pandas as pd
except ImportError:
    pass


def available_metrics():
    """Standardized Metrics Dictionary

    See https://www.calcbench.com/home/standardizedmetrics
    """
    url = _SESSION_STUFF["api_url_base"].format("availableMetrics")
    r = _calcbench_session().get(url, verify=_SESSION_STUFF["ssl_verify"])
    r.raise_for_status()
    return r.json()


def available_metrics_dataframe() -> pd.DataFrame:
    """Standardized Metrics Dictionary

    See https://www.calcbench.com/home/standardizedmetrics
    """

    metrics_df = pd.DataFrame()
    for category, metrics in available_metrics().items():
        for metric in metrics:
            metrics_df = metrics_df.append(
                {"category": category, **metric}, ignore_index=True
            )
    return metrics_df
