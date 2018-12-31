import unittest

from tradeasystems_connector.dao.vector_dao import VectorDao
from tradeasystems_connector.model.data_dict_keys import DataDictKeys
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.tests import user_settings_tests


class VectorDaoTestCase(unittest.TestCase):
    symbol = 'aapl_test'
    instrument = Instrument(symbol, 'quandl')
    period = Period.hour
    vectorDataframe = None

    dao_test = None
    collection = None

    columnList = None

    def setUp(self):
        self.dao_test = VectorDao(user_settings_tests)
        self.vectorDataframe = self.generateRandomData()

    def tearDown(self):
        self.dao_test.delete(instrument=self.instrument)

    def generateRandomData(self):
        import numpy as np
        import pandas as pd
        import datetime

        columns = DataDictKeys.keys.copy()
        columns += ([Ratio.fundamental_ebit_Y, Ratio.quant_std1Y])
        self.columnList = columns
        output = pd.DataFrame(np.random.rand(20, len(columns)), columns=columns)

        base = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        date_list = [base - datetime.timedelta(days=x) for x in range(0, 20)]
        output[DataDictKeys.index] = date_list

        output.set_index(DataDictKeys.index, inplace=True)
        return output

    def generateRandomData2(self):
        import numpy as np
        import pandas as pd
        import datetime

        columns = DataDictKeys.keys.copy()
        columns += ([Ratio.fundamental_ebit_Y, Ratio.quant_std1Y, Ratio.fundamental_shares_Y])

        output = pd.DataFrame(np.random.rand(30, len(columns)), columns=columns)

        base = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        date_list = [base - datetime.timedelta(days=x) for x in range(0, 30)]
        output[DataDictKeys.index] = date_list

        output.set_index(DataDictKeys.index, inplace=True)
        return output

    def test_save_load(self):
        initial_size = self.dao_test.load(instrument=self.instrument, columnList=self.columnList)
        if initial_size is None:
            initial_size = 0

        isSave = self.dao_test.save(self.vectorDataframe, instrument=self.instrument)
        self.assertTrue(isSave)
        dataframeLoaded = self.dao_test.load(instrument=self.instrument, columnList=self.columnList)
        self.assertIsNotNone(dataframeLoaded)

        final_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(self.vectorDataframe), final_size,
                         'incorrect size after save')

    def test_save_append(self):

        initial_size = self.dao_test.load(instrument=self.instrument, columnList=self.columnList)
        if initial_size is None:
            initial_size = 0
        else:
            initial_size = len(initial_size)

        isSave = self.dao_test.save(self.vectorDataframe, instrument=self.instrument)
        self.assertTrue(isSave)

        dataframeLoaded = self.dao_test.load(instrument=self.instrument, columnList=self.columnList)
        self.assertIsNotNone(dataframeLoaded)

        middle_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(self.vectorDataframe), middle_size,
                         'incorrect size after save')

        newVector = self.generateRandomData2()
        isSave = self.dao_test.save(newVector, instrument=self.instrument)
        self.assertTrue(isSave)

        dataframeLoaded = self.dao_test.load(instrument=self.instrument, columnList=self.columnList)
        self.assertIsNotNone(dataframeLoaded)

        end_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(newVector), end_size,
                         'incorrect size after save')
