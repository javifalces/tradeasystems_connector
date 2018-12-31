import unittest

from tradeasystems_connector.tests import user_settings_tests
from tradeasystems_connector.util.instrument_util import getAllYahooTickers


class InstrumentUtilTest(unittest.TestCase):
    user_settings = user_settings_tests

    def test_getAllYahooTickers(self):
        tickerList = getAllYahooTickers(self.user_settings)
        self.assertIsNotNone(tickerList)
        self.assertTrue(len(tickerList) > 1000)
