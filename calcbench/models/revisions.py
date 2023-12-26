from enum import Enum


class Revisions(Enum):
    """
    Which version of facts to get.
    """

    All = 0
    """
    All of the revisions.
    """
    AsOriginallyreported = 1
    """
    First reported value only
    """
    MostRecent = 2
    """
    Most recent reported value only
    """
