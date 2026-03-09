import unittest

from app import FinanceService


class FakeCell:
    def __init__(self, value):
        self.value = value


class FakeWorksheet:
    def __init__(self, values, max_row):
        self.values = values
        self.max_row = max_row

    def __getitem__(self, key):
        return FakeCell(self.values.get(key))


class FakeWorkbook:
    def __init__(self, ws):
        self.sheetnames = ["AFFAIRES 2026"]
        self._ws = ws

    def __getitem__(self, name):
        if name != "AFFAIRES 2026":
            raise KeyError(name)
        return self._ws


class FinanceServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = FinanceService(
            {
                "FINANCE_WORKBOOK_PATH": "unused.xlsx",
                "FINANCE_SHEET_NAME": "AFFAIRES 2026",
                "FINANCE_CACHE_FILE": "",
            }
        )

    def test_row_helpers_exist_on_service(self):
        self.assertTrue(hasattr(self.service, "_read_row_map"))
        self.assertTrue(hasattr(self.service, "_extract_row_data"))

    def test_parse_affaires_sheet_reads_headers_and_first_data_row(self):
        values = {
            "A11": "Client",
            "B11": "Affaire",
            "A12": "Nom client",
            "B12": "Nom affaire",
            "A14": "CLIENT A",
            "B14": "AFFAIRE A",
            "C14": "SYT",
            "D14": "001",
            "F14": 1000,
        }
        ws = FakeWorksheet(values=values, max_row=14)
        workbook = FakeWorkbook(ws)

        result = self.service.parse_affaires_sheet(workbook)

        self.assertEqual(result["headers"]["main"]["client"], "Client")
        self.assertEqual(result["headers"]["sub"]["affaire"], "Nom affaire")
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["client"], "CLIENT A")
        self.assertEqual(result["rows"][0]["affaire"], "AFFAIRE A")
        self.assertEqual(result["rows"][0]["_excel_row"], 14)


if __name__ == "__main__":
    unittest.main()
