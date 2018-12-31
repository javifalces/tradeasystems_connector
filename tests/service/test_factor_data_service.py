import unittest

import pandas as pd
from alphalens.utils import get_clean_factor_and_forward_returns

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.factor_service import FactorService
from tradeasystems_connector.tests import user_settings_tests
from tradeasystems_connector.util.instrument_util import getInstrumentList, getSharadarTickers
from tradeasystems_connector.util.persist_util import save_to_file, load_from_file


def readDictionary(file):
    return load_from_file(file)


def saveDictionary(data, filename):
    return save_to_file(data, filename)


class FactorServiceTestCase(unittest.TestCase):
    user_settings = user_settings_tests
    factor_service = FactorService(user_settings)

    instrumentList = getInstrumentList(['GOOG', 'AAPL', 'MSFT', 'A'], asset_type=AssetType.us_equity)
    factor_groups = {'GOOG': 'Group1', 'AAPL': 'Group1', 'MSFT': 'Group2', 'A': 'Group2'}

    ratio_list = [Ratio.fundamental_ebitda_Y, Ratio.fundamental_enterprise_value_Y]
    fromDate = pd.Timestamp('2012-01-01', tz='utc')
    toDate = pd.Timestamp('2017-11-01', tz='utc')

    def getDataDictOfMatrixAlphalens(self, instrumentList, ratioList, fromDate, toDate=None, persistTempFile=False):
        factor = readDictionary('factorDF.pkl')
        prices = readDictionary('prices.pkl')
        return factor, prices

    def saveDataDict(self, factorDf, prices):
        saveDictionary(factorDf, 'factorDF.pkl')
        saveDictionary(prices, 'prices.pkl')

    # @unittest.skip('sloow and not working with this data need cached factorDF + prices')
    def test_getFactorsAlphalens(self):
        factorDf, prices = self.getDataDictOfMatrixAlphalens(instrumentList=self.instrumentList,
                                                             ratioList=self.ratio_list,
                                                             fromDate=self.fromDate,
                                                             toDate=self.toDate
                                                             )
        # factorDf, prices = self.factor_service.getDataDictOfMatrixAlphalens(instrumentList=self.instrumentList,
        #                                                      ratioList=self.ratio_list,
        #                                                      fromDate=self.fromDate,
        #                                                      toDate=self.toDate
        #                                                      )

        self.assertIsNotNone(factorDf)
        self.assertIsNotNone(prices)
        # self.saveDataDict(factorDf, prices)

        factor = factorDf[self.ratio_list[0]]
        self.assertIsNotNone(factor)

        # factor_data = addFactorReturns(prices, factorDf, n_fwd_days=3)
        factor_data = get_clean_factor_and_forward_returns(
            factor,
            prices,
            # groupby=self.factor_groups,
            quantiles=None,
            bins=2,
            periods=[3],
            filter_zscore=None,
            max_loss=1  # .35
        )

        self.assertIsNotNone(factor_data)

        # fullTear = create_full_tear_sheet(factor_data, long_short=False, group_neutral=False, by_group=False)
        # self.assertIsNotNone(fullTear)
        # eventReturns = create_event_returns_tear_sheet(factor_data, prices, avgretplot=(3, 11),
        #                                                long_short=False, group_neutral=False, by_group=False)
        # self.assertIsNotNone(eventReturns)

    def test_getSP500Groups(self):
        keyDict = self.factor_service.getSP500GroupsSector(['GOOG', 'AAPL', 'MSFT'])
        self.assertIsNotNone(keyDict)
        self.assertEqual(3, len(keyDict))

    def test_getIBEXGroups(self):
        keyDict = self.factor_service.getIBEXGroups(['ABE.MC', 'AENA.MC', 'BKT.MC'])
        self.assertIsNotNone(keyDict)
        self.assertEqual(3, len(keyDict))

    @unittest.skip
    def test_getSharadarGroups(self):
        symbols = getSharadarTickers(self.user_settings)
        self.assertIsNotNone(symbols)
        keyDict = self.factor_service.getSharadarGroupsSector(symbols)
        self.assertIsNotNone(keyDict)
