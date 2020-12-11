from calcbench.api_client import _json_GET

try:
    import pandas as pd
except ImportError:
    pass


def available_metrics():
    """Standardized Metrics Dictionary

    See https://www.calcbench.com/home/standardizedmetrics
    """
    return _json_GET("api/availableMetrics")


def available_metrics_dataframe():
    """Standardized Metrics Dictionary

    See https://www.calcbench.com/home/standardizedmetrics
    """

    return pd.DataFrame(
        [
            {"category": category, **metric}
            for category, metrics in available_metrics().items()
            for metric in metrics
        ]
    )
