import os
import unittest

from tradeasystems_connector.dao.pystore_dao import PyStoreDao
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.tests import user_settings_tests


class PyStoreDaoTestCase(unittest.TestCase):
    symbol = 'aapl_test'
    instrument = Instrument(symbol, 'quandl')
    test_path = os.path.dirname(os.path.realpath(__file__)) + os.sep
    dao_test = None
    collection = None

    def setUp(self):
        self.dao_test = PyStoreDao(user_settings_tests, storeName='testStore')
        store = self.dao_test.store
        collectionName = "testCollection"
        collection = store.collection(collectionName, overwrite=True)

        self.dao_test.collectionName = collectionName
        self.dao_test.collection = collection

    def tearDown(self):
        self.dao_test.delete(self.instrument.symbol)

    def test_save_load(self):
        import pandas as pd

        initial_size = self.dao_test.load(self.instrument.symbol)
        if initial_size is None:
            initial_size = 0
        # aapl = quandl.get("WIKI/AAPL", authtoken=user_settings.quandl_token)
        aapl = pd.DataFrame.from_csv(self.test_path + 'mock_dataframe.csv')

        self.dao_test.save(self.instrument.symbol, aapl, source='quandl_test')
        dataframeLoaded = self.dao_test.load(self.instrument.symbol)
        self.assertIsNotNone(dataframeLoaded)

        final_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(aapl), final_size,
                         'incorrect size after save')

    def test_save_2_load(self):
        import pandas as pd

        initial_size = self.dao_test.load(self.instrument.symbol)
        if initial_size is None:
            initial_size = 0
        # aapl = quandl.get("WIKI/AAPL", authtoken=user_settings.quandl_token)
        aapl = pd.DataFrame.from_csv(self.test_path + 'mock_dataframe.csv')

        isSaved = self.dao_test.save(self.instrument.symbol, aapl, source='quandl_test')
        self.assertIs(isSaved, True)
        dataframeLoaded = self.dao_test.load(self.instrument.symbol)
        self.assertIsNotNone(dataframeLoaded)

        final_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(aapl), final_size,
                         'incorrect size after save 1')

        aapl2 = pd.DataFrame.from_csv(self.test_path + 'mock_dataframe_2.csv')
        isSaved = self.dao_test.save(self.instrument.symbol, aapl2, source='quandl_test')
        self.assertIs(isSaved, True)
        dataframeLoaded2 = self.dao_test.load(self.instrument.symbol)
        final_size_2 = len(dataframeLoaded2)

        self.assertEqual(initial_size + len(aapl) + len(aapl2), final_size_2,
                         'incorrect size after save 2')

    def test_save_overlapping_load(self):
        import pandas as pd

        initial_size = self.dao_test.load(self.instrument.symbol)
        if initial_size is None:
            initial_size = 0
        # aapl = quandl.get("WIKI/AAPL", authtoken=user_settings.quandl_token)
        aapl = pd.DataFrame.from_csv(self.test_path + 'mock_dataframe.csv')

        isSaved = self.dao_test.save(self.instrument.symbol, aapl, source='quandl_test')
        self.assertIs(isSaved, True)
        dataframeLoaded = self.dao_test.load(self.instrument.symbol)
        self.assertIsNotNone(dataframeLoaded)

        final_size = len(dataframeLoaded)
        self.assertEqual(initial_size + len(aapl), final_size,
                         'incorrect size after save 1')

        aapl2 = pd.DataFrame.from_csv(self.test_path + 'mock_dataframe_2.csv')
        isSaved = self.dao_test.save(self.instrument.symbol, aapl2, source='quandl_test')
        self.assertIs(isSaved, True)
        dataframeLoaded2 = self.dao_test.load(self.instrument.symbol)
        final_size_2 = len(dataframeLoaded2)

        self.assertEqual(initial_size + len(aapl) + len(aapl2), final_size_2,
                         'incorrect size after save 2')

        aapl_overlap = pd.DataFrame.from_csv(self.test_path + 'mock_dataframe_overlap.csv')
        isSaved = self.dao_test.save(self.instrument.symbol, aapl_overlap, source='quandl_test')
        self.assertIs(isSaved, True)
        dataframeLoaded3 = self.dao_test.load(self.instrument.symbol)
        final_size_3 = len(dataframeLoaded3)

        self.assertEqual(initial_size + len(aapl) + len(aapl2), final_size_3,
                         'incorrect size after save overlap')
