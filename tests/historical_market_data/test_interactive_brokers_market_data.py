import datetime
import unittest

from tradeasystems_connector.historical_market_data.interactive_brokers_market_data import \
    InteractiveBrokersHistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.broker import Broker
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.tests import user_settings_tests


class InteractiveBrokersHistoricalDataTestCase(unittest.TestCase):
    symbol = 'EUR'
    broker = Broker.manual_email
    currency = Currency.usd
    asset_type = AssetType.forex
    instrument = Instrument(symbol, asset_type=asset_type, broker=broker, currency=currency)

    fromDate = datetime.datetime.today() - datetime.timedelta(days=5)
    user_settings = user_settings_tests
    market_data_object = InteractiveBrokersHistoricalMarketData(user_settings)

    @unittest.skip  # no reason needed
    def test_getPeriodsRequests(self):
        fromDate = datetime.datetime.today() - datetime.timedelta(days=40)
        toDate = datetime.datetime.today()
        periodSplit = Period.hour
        durationStr_list, toDateString_list = self.market_data_object.getPeriodsRequests(periodSplit, fromDate, toDate)
        self.assertGreater(len(durationStr_list), 1)
        self.assertGreater(len(toDateString_list), 1)
        pass

    @unittest.skip  # no reason needed
    def test_download_fx(self):
        symbol = 'EUR'
        broker = Broker.manual_email
        currency = Currency.usd
        asset_type = AssetType.forex
        instrument = Instrument(symbol, asset_type=asset_type, broker=broker, currency=currency)

        fromDate = datetime.datetime.today() - datetime.timedelta(days=40)
        toDate = datetime.datetime.today()
        periodSplit = Period.hour

        dataframe = self.market_data_object.download(instrument, periodSplit, 1, fromDate, toDate)
        self.assertIsNotNone(dataframe)
        self.assertGreater(len(dataframe), 1)

        pass

    @unittest.skip  # no reason needed
    def test_download_equity(self):
        symbol = 'IBM'
        broker = Broker.manual_email
        currency = Currency.usd
        asset_type = AssetType.equity
        instrument = Instrument(symbol, asset_type=asset_type, broker=broker, currency=currency)

        fromDate = datetime.datetime.today() - datetime.timedelta(days=40)
        toDate = datetime.datetime.today()
        periodSplit = Period.hour

        dataframe = self.market_data_object.download(instrument, periodSplit, 1, fromDate, toDate)
        self.assertIsNotNone(dataframe)
        self.assertGreater(len(dataframe), 1)

        pass
