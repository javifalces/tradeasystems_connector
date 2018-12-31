import unittest

from tradeasystems_connector.fundamental_data.excel_ratio_data_importer import ExcelRatioData
from tradeasystems_connector.tests import user_settings_tests


class ExcelRatioDataImporterTestCase(unittest.TestCase):
    user_settings = user_settings_tests
    excelImporter = ExcelRatioData(user_settings)

    # def setUp(self):
    #     import os
    #     os.environ[
    #         "TRADEA_RATIO_INPUT_PATH"] = "D:\javif\Coding\Python\AInvesting\tradeasystems_connector\tests\fundamental_data\ratio_test"
    #     pass

    def test_download_excels(self):
        self.setUp()
        imported = self.excelImporter.download()

        self.assertIsNotNone(imported)
        self.assertTrue(imported)
