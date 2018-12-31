import datetime
import unittest

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.quant_data.quant_data_impl import QuantDataImpl
from tradeasystems_connector.tests import user_settings_tests


class QuandlQuantCalculatorTestCase(unittest.TestCase):
    instrument = Instrument('AAPL', asset_type=AssetType.us_equity, currency=Currency.usd)
    user_settings = user_settings_tests
    quant_data_object = QuantDataImpl(user_settings)

    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)

    def test_calculateReturnDiff(self):
        ratio = Ratio.quant_returnsDiff_120
        data = self.quant_data_object.download(self.instrument,
                                               quant_ratio=ratio,
                                               fromDate=self.fromDate, toDate=self.toDate)
        self.assertIsNotNone(data)
        self.assertTrue(Ratio.ratio in data.columns)
        self.assertEqual(len(data.columns), 1)

    def test_all_quants(self):
        ratioList = Ratio.quant_list
        for ratio in ratioList:
            print('testing ratio %s ' % ratio)
            downloadTest = self.quant_data_object.download(self.instrument,
                                                           quant_ratio=ratio,
                                                           fromDate=self.fromDate, toDate=self.toDate)
            self.assertIsNotNone(downloadTest)
            self.assertNotEquals(len(downloadTest), 0)
            self.assertTrue(Ratio.ratio in downloadTest.columns)
            self.assertEqual(len(downloadTest.columns), 1)
