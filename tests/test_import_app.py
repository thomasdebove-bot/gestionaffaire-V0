import unittest


class AppImportTests(unittest.TestCase):
    def test_import_app_module_exposes_asgi_app_and_service(self):
        import app

        self.assertTrue(hasattr(app, "FinanceService"))
        self.assertTrue(hasattr(app, "app"))
        self.assertTrue(callable(app.app))


if __name__ == "__main__":
    unittest.main()
