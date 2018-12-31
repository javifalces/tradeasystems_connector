import unittest

from tradeasystems_connector.dao.tick_dao import TickDao
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.tick import Tick
from tradeasystems_connector.tests import user_settings_tests


class TickDaoTestCase(unittest.TestCase):
    symbol = 'aapl_test'
    instrument = Instrument(symbol, 'quandl')

    tickDataframe = None

    dao_test = None
    collection = None

    def setUp(self):
        self.dao_test = TickDao(user_settings_tests)
        self.tickDataframe = self.generateRandomData()

    def tearDown(self):
        self.dao_test.delete(self.instrument)

    def generateRandomData(self):
        import numpy as np
        import pandas as pd
        import datetime

        columns = Tick.columns
        output = pd.DataFrame(np.random.rand(20, len(columns)), columns=columns)

        base = datetime.datetime.today()
        date_list = [base - datetime.timedelta(minutes=x) for x in range(0, 20)]
        output[Tick.index] = date_list

        output.set_index(Tick.index, inplace=True)
        return output

    def test_save_load(self):
        initial_size = self.dao_test.load(self.instrument)
        if initial_size is None:
            initial_size = 0

        self.dao_test.save(self.tickDataframe, self.instrument)
        dataframeLoaded = self.dao_test.load(self.instrument)
        self.assertIsNotNone(dataframeLoaded)

        final_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(self.tickDataframe), final_size,
                         'incorrect size after save')
