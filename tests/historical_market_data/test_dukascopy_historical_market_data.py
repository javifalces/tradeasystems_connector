import datetime
import glob
import unittest

from tradeasystems_connector.historical_market_data.dukascopy_file_historical_market_data import \
    DukasCopyFileHistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.broker import Broker
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.tests import user_settings_tests


class DukasCopyHistoricalDataTestCase(unittest.TestCase):
    symbol = 'EUR'
    broker = Broker.manual_email
    currency = Currency.usd
    instrument = Instrument(symbol=symbol, asset_type=AssetType.forex, broker=broker, currency=currency)

    fromDate = datetime.datetime(year=2018, day=1, month=6)
    fromDate_tick = datetime.datetime(year=2018, day=26, month=6)
    toDate = datetime.datetime(year=2018, day=26, month=6)
    user_settings = user_settings_tests
    market_data_object = DukasCopyFileHistoricalMarketData(user_settings)

    def setUp(self):
        import os
        os.environ[
            "TRADEA_DUKASCOPY_INPUT_PATH"] = "D:\javif\Coding\Python\AInvesting\tradeasystems_connector\tests\historical_market_data\dukascopy_test"
        pass

    def tearDown(self):
        import os
        filesInDirectory = glob.glob(
            self.user_settings.dukascopy_source_folder + os.sep + "*" + self.market_data_object.processedFilesPattern)
        for processedFiles in filesInDirectory:
            os.rename(processedFiles, processedFiles[:-len(self.market_data_object.processedFilesPattern)])
        pass

    @unittest.skip  # no reason needed
    def test_download_data_tick(self):
        dataframe = self.market_data_object.download(self.instrument, period=Period.tick,
                                                     number_of_periods=1, fromDate=self.fromDate_tick,
                                                     toDate=self.toDate)

        self.assertIsNotNone(dataframe)

    @unittest.skip  # no reason needed
    def test_download_data_day(self):
        dataframe = self.market_data_object.download(self.instrument, period=Period.day,
                                                     number_of_periods=1, fromDate=self.fromDate, toDate=self.toDate)

        self.assertIsNotNone(dataframe)
