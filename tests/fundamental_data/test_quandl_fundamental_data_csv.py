import datetime
import unittest

from tradeasystems_connector.fundamental_data.quandl_fundamental_data_csv import QuandlFundamentalDataCSV
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.tests import user_settings_tests
from tradeasystems_connector.util.instrument_util import getSF0Tickers, getInstrumentList


class QuandlFundamentalDataCSVTestCase(unittest.TestCase):
    instrument = Instrument('AAPL', asset_type=AssetType.us_equity, currency=Currency.usd)
    user_settings = user_settings_tests
    fundamental_data_object = QuandlFundamentalDataCSV(user_settings)

    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)

    @unittest.skip
    def test_download_data(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_ebitda_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)

    @unittest.skip
    def test_calculate_download_data(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_enterprise_value_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)

    @unittest.skip
    def test_download_wrong(self):
        instrument = Instrument('AAPqwL', asset_type=AssetType.us_equity, currency=Currency.usd)

        downloadTest = self.fundamental_data_object.download(instrument,
                                                             fundamental_ratio=Ratio.fundamental_ebitda_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNone(downloadTest)

    @unittest.skip
    def test_calculate_gross_margin(self):
        downloadTest = self.fundamental_data_object.download(self.instrument,
                                                             fundamental_ratio=Ratio.fundamental_gross_margin_Y,
                                                             fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(downloadTest)
        self.assertNotEquals(len(downloadTest), 0)
        self.assertTrue(Ratio.ratio in downloadTest.columns)
        self.assertEqual(len(downloadTest.columns), 1)

    @unittest.skip
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

    @unittest.skip
    def test_all_fundamentals_allSymbols(self):
        fromDate = datetime.datetime(year=2000, day=1, month=1)
        toDate = datetime.datetime(year=2018, day=26, month=6)
        symbolList = getSF0Tickers(self.user_settings)
        instrumentList = getInstrumentList(symbolList=symbolList, currency=Currency.usd, asset_type=AssetType.us_equity)
        ratioList = Ratio.fundamental_list_Y
        for ratio in ratioList:
            for instrument in instrumentList:
                print('testing ratio %s for %s ' % (ratio, instrument))
                downloadTest = self.fundamental_data_object.download(instrument,
                                                                     fundamental_ratio=ratio,
                                                                     fromDate=fromDate, toDate=toDate)
                self.assertIsNotNone(downloadTest)
                self.assertNotEquals(len(downloadTest), 0)
                self.assertTrue(Ratio.ratio in downloadTest.columns)
                self.assertEqual(len(downloadTest.columns), 1)
