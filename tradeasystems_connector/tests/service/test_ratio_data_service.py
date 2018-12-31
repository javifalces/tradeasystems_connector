import datetime
import unittest

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.ratio_data_service import RatioDataService
from tradeasystems_connector.tests import user_settings_tests


class RatioDataServiceTestCase(unittest.TestCase):
    user_settings = user_settings_tests
    ratio_fundamental_service = RatioDataService(user_settings)
    instrument = Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd)
    ratio_fundamental = Ratio.fundamental_ebitda_Y
    ratio_quant = Ratio.quant_returnsDiff_120
    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)

    def test_getRatio_fund(self):
        # clean first

        data = self.ratio_fundamental_service.getRatioData(self.instrument, ratio=self.ratio_fundamental,
                                                           fromDate=self.fromDate, toDate=self.toDate,
                                                           force_download=True)

        self.assertIsNotNone(data)

    def test_getRatio_quant(self):
        # clean first

        data = self.ratio_fundamental_service.getRatioData(self.instrument, ratio=self.ratio_quant,
                                                           fromDate=self.fromDate, toDate=self.toDate,
                                                           force_download=True)

        self.assertIsNotNone(data)

    def test_getRatioList_fund(self):
        # clean first

        data = self.ratio_fundamental_service.getRatioDataBatch(self.instrument, ratio_list=[Ratio.fundamental_ebit_Y,
                                                                                             Ratio.fundamental_net_income_Y],
                                                                fromDate=self.fromDate, toDate=self.toDate,
                                                                force_download=True)

        self.assertIsNotNone(data)

        self.assertEqual(len(data.columns), 2)
