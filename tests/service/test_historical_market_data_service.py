import unittest

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.broker import Broker
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService
from tradeasystems_connector.tests import user_settings_tests


class HistoricalMarketDataServiceTestCase(unittest.TestCase):
    user_settings = user_settings_tests
    historicalMarketService = HistoricalMarketDataService(user_settings)
    instrument = Instrument(symbol='ETH', broker=Broker.gdax, asset_type=AssetType.crypto, currency=Currency.eur)
    period = Period.hour
    number_of_periods = 1

    def setUp(self):
        pass

    def tearDown(self):
        # delete
        # self.historicalMarketService.deleteBar(instrument=self.instrument,period=self.period,number_of_periods=self.number_of_periods)
        pass

    def test_getHistorical_hour(self):
        # clean first
        # self.historicalMarketService.deleteBar(instrument=self.instrument, period=self.period,
        #                                        number_of_periods=self.number_of_periods)

        import datetime
        from_date = datetime.datetime.today() - datetime.timedelta(days=2)
        to_date = datetime.datetime.today() - datetime.timedelta(days=1)

        data = self.historicalMarketService.getHistoricalData(self.instrument, period=self.period,
                                                              number_of_periods=self.number_of_periods,
                                                              fromDate=from_date, toDate=to_date, force_download=True)
        self.assertIsNotNone(data)

    def test_getHistorical__dailyClean(self):
        # clean first
        # self.historicalMarketService.deleteBar(instrument=self.instrument, period=self.period,
        #                                        number_of_periods=self.number_of_periods)

        import datetime
        instrument = Instrument(symbol='AAPL', broker=Broker.manual_email, asset_type=AssetType.us_equity,
                                currency=Currency.usd)

        from_date = datetime.datetime(year=2010, month=1, day=1)
        to_date = datetime.datetime(year=2012, month=1, day=1)
        period = Period.day
        number_of_periods = 1
        data = self.historicalMarketService.getHistoricalData(instrument, period=period,
                                                              number_of_periods=number_of_periods,
                                                              fromDate=from_date, toDate=to_date,
                                                              force_download=True,
                                                              endOfDayData=True
                                                              )
        self.assertIsNotNone(data)

    # def test_getHistorical_hour_append(self):
    #     #clean first
    #     self.historicalMarketService.deleteBar(instrument=self.instrument, period=self.period,
    #                                            number_of_periods=self.number_of_periods)
    #     import datetime
    #     from_date = datetime.datetime.today()-datetime.timedelta(days=2)
    #     to_date = datetime.datetime.today()-datetime.timedelta(days=1)
    #     data = self.historicalMarketService.getHistoricalData(self.instrument,period=self.period,number_of_periods=self.number_of_periods,fromDate=from_date,toDate=to_date)
    #     self.assertIsNotNone(data)
    #     lenOrigin = len(data)
    #
    #     from_date = datetime.datetime.today()-datetime.timedelta(days=1)
    #     data2 = self.historicalMarketService.getHistoricalData(self.instrument,period=self.period,number_of_periods=self.number_of_periods,fromDate=from_date)
    #     self.assertIsNotNone(data2)
    #     lenFinal = len(data2)
    #     self.assertEqual(lenFinal,lenOrigin*2)
