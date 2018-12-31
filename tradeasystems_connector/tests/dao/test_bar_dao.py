import unittest

from tradeasystems_connector.dao.bar_dao import BarDao
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.tests import user_settings_tests


class BarDaoTestCase(unittest.TestCase):
    symbol = 'aapl_test'
    instrument = Instrument(symbol, 'quandl')
    period = Period.hour
    barDataframe = None

    dao_test = None
    collection = None

    def setUp(self):
        self.dao_test = BarDao(user_settings_tests)
        self.barDataframe = self.generateRandomData()

    def tearDown(self):
        self.dao_test.delete(instrument=self.instrument, period=self.period, number_of_periods=1)

    def generateRandomData(self):
        import numpy as np
        import pandas as pd
        import datetime

        columns = Bar.columns
        output = pd.DataFrame(np.random.rand(20, len(columns)), columns=columns)

        base = datetime.datetime.today()
        date_list = [base - datetime.timedelta(days=x) for x in range(0, 20)]
        output[Bar.index] = date_list

        output.set_index(Bar.index, inplace=True)
        return output

    def test_save_load(self):
        initial_size = self.dao_test.load(instrument=self.instrument, period=self.period, number_of_periods=1)
        if initial_size is None:
            initial_size = 0

        self.dao_test.save(self.barDataframe, instrument=self.instrument, period=self.period, number_of_periods=1)
        dataframeLoaded = self.dao_test.load(instrument=self.instrument, period=self.period, number_of_periods=1)
        self.assertIsNotNone(dataframeLoaded)

        final_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(self.barDataframe), final_size,
                         'incorrect size after save')
