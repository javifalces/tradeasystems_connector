import datetime
import unittest

from tradeasystems_connector.fundamental_data.morningstar_fundamental_data import MorningStarFundamentalData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.tests import user_settings_tests


class MorningstarFundamentalDataTestCase(unittest.TestCase):
    instrument = Instrument('AAPL', asset_type=AssetType.us_equity, currency=Currency.usd)
    instrument_es = Instrument('TEF.MC', asset_type=AssetType.es_equity, currency=Currency.eur)
    user_settings = user_settings_tests
    fundamental_data_object = MorningStarFundamentalData(user_settings)

    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)

    def test_download_data(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_assets_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)

    def test_download_wrong(self):
        instrument = Instrument('AAPqwL', asset_type=AssetType.us_equity, currency=Currency.usd)

        downloadTest = self.fundamental_data_object.download(instrument,
                                                             fundamental_ratio=Ratio.fundamental_ebitda_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNone(downloadTest)

    def test_calculate_gross_margin(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_gross_margin_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)
        self.assertTrue(Ratio.ratio in downloadTest.columns)
        self.assertEqual(len(downloadTest.columns), 1)

    def test_calculate_book_value_per_share(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_book_value_per_share_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)
        self.assertTrue(Ratio.ratio in downloadTest.columns)
        self.assertEqual(len(downloadTest.columns), 1)

    def test_calculate_ev(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_enterprise_value_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)
        self.assertTrue(Ratio.ratio in downloadTest.columns)
        self.assertEqual(len(downloadTest.columns), 1)

    #
    def test_all_fundamentals(self):
        ratioList = Ratio.fundamental_list_Y
        for ratio in ratioList:
            print('testing ratio %s ' % ratio)
            downloadTest = self.fundamental_data_object.download(self.instrument,
                                                                 fundamental_ratio=ratio,
                                                                 fromDate=self.fromDate, toDate=self.toDate)
            self.assertIsNotNone(downloadTest)
            self.assertNotEquals(len(downloadTest), 0)
            self.assertTrue(Ratio.ratio in downloadTest.columns)
            self.assertEqual(len(downloadTest.columns), 1)

    def test_download_data_es(self):
        downloadTest = self.fundamental_data_object.download(self.instrument_es,
                                                             fundamental_ratio=Ratio.fundamental_assets_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)

    def test_all_fundamentals_es(self):
        ratioList = Ratio.fundamental_list_Y
        for ratio in ratioList:
            print('testing ES ratio %s ' % ratio)
            downloadTest = self.fundamental_data_object.download(self.instrument_es,
                                                                 fundamental_ratio=ratio,
                                                                 fromDate=self.fromDate, toDate=self.toDate)
            self.assertIsNotNone(downloadTest)
            self.assertNotEquals(len(downloadTest), 0)
            self.assertTrue(Ratio.ratio in downloadTest.columns)
            self.assertEqual(len(downloadTest.columns), 1)
