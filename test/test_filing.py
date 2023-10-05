import json
from unittest import TestCase

from calcbench.models.filing import Filing


class FilingTest(TestCase):
    def test_service_bus_deserialization(self):
        body_bytes = b'{"is_xbrl":false,"filing_id":0,"entity_id":1,"CIK":null,"standardized_XBRL":false,"has_standardized_data":false,"standardized_data_compressed":""}'
        body_json = json.loads(body_bytes)
        Filing(**body_json)
        print(body_json)

    def test_pydantic_building(self):
        d = {
            "is_xbrl": True,
            "is_wire": False,
            "calcbench_id": 282238,
            "filing_id": 1695052,
            "sec_accession_id": "0001564590-21-005399",
            "sec_html_url": "https://www.sec.gov/Archives/edgar/data/1000045/000156459021005399/0001564590-21-005399-index.htm",
            "document_type": "10-Q",
            "filing_type": "annualQuarterlyReport",
            "filing_date": "2021-02-11T00:00:00",
            "fiscal_period": 3,
            "fiscal_year": 2021,
            "calcbench_accepted": "2021-02-11T08:42:38",
            "calcbench_finished_load": "2021-02-11T08:46:28",
            "entity_id": 1,
            "ticker": "NICK",
            "entity_name": "NICHOLAS FINANCIAL INC",
            "CIK": "0001000045",
            "period_index": 11,
            "period_end_date": "2020-12-31T00:00:00",
            "percentage_revenue_change": -0.034475611441204916,
            "this_period_revenue": 14474000.0,
            "link1": "https://www.sec.gov/Archives/edgar/data/1000045/000156459021005399/nick-10q_20201231.htm",
            "link2": "https://www.sec.gov/Archives/edgar/data/1000045/000156459021005399/nick-ex311_9.htm",
            "link3": "https://www.sec.gov/Archives/edgar/data/1000045/000156459021005399/nick-ex312_8.htm",
            "standardized_XBRL": True,
            "has_standardized_data": True,
        }
        f = Filing(**d)
