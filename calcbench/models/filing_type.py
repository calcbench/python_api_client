from enum import Enum


class FilingType(str, Enum):
    BusinessWirePR_filedAfterAn8K = "BusinessWirePR_filedAfterAn8K"
    """
    Wire press release recieved after an earnings 8-K.  Calcbench does not process these.
    """
    BusinessWirePR_replaced = "BusinessWirePR_replaced"
    """
    Wire press release for which an 8-K has subsequently been received.

    This value should no longer be returned by the filings API.
    """
    proxy = "proxy"
    annualQuarterlyReport = "annualQuarterlyReport"
    eightk_earningsPressRelease = "eightk_earningsPressRelease"
    """
    Wire press release, OR an earnings press release filed as an 8-K.

    The wire should be filed again as an 8-K.

    The wire will have the document_type of "WIREPR" while the 8-K will have the document_type of "8-K"
    """
    eightk_guidanceUpdate = "eightk_guidanceUpdate"
    eightk_conferenceCallTranscript = "eightk_conferenceCallTranscript"
    eightk_presentationSlides = "eightk_presentationSlides"
    eightk_monthlyOperatingMetrics = "eightk_monthlyOperatingMetrics"
    eightk_earningsPressRelease_preliminary = "eightk_earningsPressRelease_preliminary"
    eightk_earningsPressRelease_correction = "eightk_earningsPressRelease_correction"
    eightk_other = "eightk_other"
    commentLetter = "commentLetter"
    """
    from the SEC - UPLOAD
    """
    commentLetterResponse = "commentLetterResponse"
    """
    From the filer - CORRESP
    """
    form_3 = "form_3"
    form_4 = "form_4"
    form_5 = "form_5"
    eightk_nonfinancial = "eightk_nonfinancial"
    NT10KorQ = "NT10KorQ"
    """
    Not in Time 10-K/Q.  Notification that 10-K/Q will not be filed in time.
    """
    S = "S"
    Four24B = "Four24B"
    institutionalOwnsership_13F = "institutionalOwnsership_13F"
