import datetime
import unittest

from tradeasystems_connector.historical_market_data.alpha_vantage_historical_market_data import \
    AlphaVantageHistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.broker import Broker
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.tests import user_settings_tests


class AlphaVantageHistoricalDataTestCase(unittest.TestCase):
    symbol = 'MSFT'
    broker = Broker.manual_email
    currency = Currency.usd
    asset_type = AssetType.future
    instrument = Instrument(symbol, asset_type=asset_type, broker=broker, currency=currency)

    instrument_wrong = Instrument(symbol + 'losif', asset_type=asset_type, broker=broker, currency=currency)

    fromDate = datetime.datetime.today() - datetime.timedelta(days=5)
    user_settings = user_settings_tests
    market_data_object = AlphaVantageHistoricalMarketData(user_settings)

    # def setUp(self):
    #     pass
    #
    # def tearDown(self):
    #     pass

    @unittest.skip("error premium account")
    def test_download_data_minute(self):
        # premium
        fromDateMin = datetime.datetime.today() - datetime.timedelta(days=1)
        dataframe = self.market_data_object.download(self.instrument, period=Period.minute,
                                                     number_of_periods=1, fromDate=fromDateMin)

        self.assertIsNotNone(dataframe)

    def test_download_data_day(self):
        dataframe = self.market_data_object.download(self.instrument, period=Period.day,
                                                     number_of_periods=1, fromDate=self.fromDate)

        self.assertIsNotNone(dataframe)

    def test_download_data_day_wrong(self):
        dataframe = self.market_data_object.download(self.instrument_wrong, period=Period.day,
                                                     number_of_periods=1, fromDate=self.fromDate)

        self.assertIsNone(dataframe)

    def test_download_data_minute_wrong(self):
        dataframe = self.market_data_object.download(self.instrument_wrong, period=Period.minute,
                                                     number_of_periods=1, fromDate=self.fromDate)

        self.assertIsNone(dataframe)
