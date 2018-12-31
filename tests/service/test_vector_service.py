import datetime
import unittest

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.data_dict_keys import DataDictKeys
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.vector_service import VectorService
from tradeasystems_connector.tests import user_settings_tests
from pandas.tseries.offsets import BDay
import pandas as pd


class VectorServiceTestCase(unittest.TestCase):
    user_settings = user_settings_tests

    vector_service = VectorService(user_settings)

    instrumentList = [
        Instrument(symbol='ABC', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='ACN', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='GOOGL', asset_type=AssetType.us_equity, currency=Currency.usd),
        Instrument(symbol='MSFT', asset_type=AssetType.us_equity, currency=Currency.usd),

    ]

    ratio_list = [Ratio.fundamental_ebitda_Y, Ratio.fundamental_enterprise_value_Y, Ratio.fundamental_ms_Y]
    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)

    def test_getOneInstrumentCompleteDeleting(self):
        import time
        instrument = self.instrumentList[2]
        self.vector_service.vectorDao.delete(instrument)
        self.vector_service.useFunctionTemp = False
        start = time.time()
        dict_of_matrix = self.vector_service.getDataDictOfMatrix(instrumentList=[instrument],
                                                                 ratioList=Ratio.allRatios,
                                                                 fromDate=datetime.datetime(year=2010, day=1, month=1),
                                                                 toDate=self.toDate,
                                                                 )
        end = time.time()
        elapsed = end - start
        print('1-Took %d seconds to complete an instrument downloading' % (elapsed))
        self.assertIsNotNone(dict_of_matrix)
        self.assertEqual(len(Ratio.allRatios) + 5, len(dict_of_matrix))
        keysOutput = dict_of_matrix.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)
        for key in self.ratio_list:
            self.assertEqual(key in keysOutput, True)
        self.assertLess(elapsed, 40)

        # %% from DDBB
        start2 = time.time()
        self.vector_service.force_download = False
        dict_of_matrix_2 = self.vector_service.getDataDictOfMatrix(instrumentList=[instrument],
                                                                   ratioList=Ratio.allRatios,
                                                                   fromDate=datetime.datetime(year=2010, day=1,
                                                                                              month=1),
                                                                   toDate=self.toDate,
                                                                   )

        end2 = time.time()
        elapsed2 = end2 - start2
        self.vector_service.force_download = True
        print('2-Took %d seconds to complete an instrument from DDBB' % (elapsed2))
        self.assertIsNotNone(dict_of_matrix_2)
        self.assertEqual(len(Ratio.allRatios) + 5, len(dict_of_matrix_2))
        keysOutput = dict_of_matrix_2.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)
        for key in self.ratio_list:
            self.assertEqual(key in keysOutput, True)
        self.assertLess(elapsed2, 10)
        # %%
        for key in dict_of_matrix.keys():
            df = dict_of_matrix[key]
            df2 = dict_of_matrix_2[key]
            self.assertEqual(len(df), len(df2))
            sum1 = df.sum().sum()
            sum2 = df2.sum().sum()
            self.assertEqual(sum1, sum2)

            df_dropna = df.dropna()
            self.assertFalse(df_dropna.empty)

    def test_get4InstrumentCompleteDeleting(self):
        import time
        instrumentList = [
            Instrument(symbol='A', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='AAAL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='ABT', asset_type=AssetType.us_equity, currency=Currency.usd),

        ]
        for instrument in instrumentList:
            self.vector_service.vectorDao.delete(instrument)

        start = time.time()
        dictPricing = {}
        for instrument in instrumentList:
            self.vector_service.vectorizedDataService.historicalMarketDataService.deleteBar(instrument=instrument,
                                                                                            period=Period.day,
                                                                                            number_of_periods=1)

            self.vector_service.vectorizedDataService.__downloadHistoricalPrices__(wrongInstrumentsSymbolLists=[],
                                                                                   outputDict=dictPricing,
                                                                                   fromDate=datetime.datetime(year=2010,
                                                                                                              day=1,
                                                                                                              month=1),
                                                                                   toDate=self.toDate,
                                                                                   instrument=instrument
                                                                                   )

        dict_of_matrix = self.vector_service.getDataDictOfMatrix(instrumentList=instrumentList,
                                                                 ratioList=Ratio.allRatios,
                                                                 fromDate=datetime.datetime(year=2010, day=1, month=1),
                                                                 toDate=self.toDate,
                                                                 )
        end = time.time()
        elapsed = end - start
        print('1-Took %d seconds to complete 4 instrument downloading' % (elapsed))
        self.assertIsNotNone(dict_of_matrix)
        self.assertEqual(len(Ratio.allRatios) + 5, len(dict_of_matrix))
        keysOutput = dict_of_matrix.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)
        for key in self.ratio_list:
            self.assertEqual(key in keysOutput, True)

        # %% from DDBB
        start2 = time.time()
        self.vector_service.force_download = False
        dict_of_matrix_2 = self.vector_service.getDataDictOfMatrix(instrumentList=instrumentList,
                                                                   ratioList=Ratio.allRatios,
                                                                   fromDate=datetime.datetime(year=2010, day=1,
                                                                                              month=1),
                                                                   toDate=self.toDate,
                                                                   )

        end2 = time.time()
        elapsed2 = end2 - start2
        self.vector_service.force_download = True
        print('2-Took %d seconds to complete an instrument from DDBB' % (elapsed2))
        self.assertIsNotNone(dict_of_matrix_2)
        self.assertEqual(len(Ratio.allRatios) + 5, len(dict_of_matrix_2))
        keysOutput = dict_of_matrix_2.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)
        for key in self.ratio_list:
            self.assertEqual(key in keysOutput, True)

        # %%
        for key in dict_of_matrix.keys():
            print('testing equality in %s' % key)
            df = dict_of_matrix[key]
            df2 = dict_of_matrix_2[key]
            self.assertEqual(len(df), len(df2))
            sum1 = df.sum().sum()
            sum2 = df2.sum().sum()
            self.assertEqual(sum1, sum2)

            df_dropna = df.dropna()
            self.assertFalse(df_dropna.empty)
        # %%
        for key in dictPricing.keys():
            if key == 'wrong':
                continue
            for symbol in dict_of_matrix[key].columns:
                print('testing pricing equality in %s:%s' % (key, symbol))
                isBusinessDay = BDay().onOffset
                match_series = pd.to_datetime(dictPricing[key].index).map(isBusinessDay)
                dictPricing[key] = dictPricing[key][match_series]
                dictPricing[key].fillna(method='ffill', inplace=True)

                df = dict_of_matrix[key][symbol]
                df_pricing = dictPricing[key][symbol]
                # self.assertEqual(len(df), len(df_pricing.dropna()))
                sum1 = df.sum()
                sum2 = df_pricing.sum()
                self.assertEqual(sum1, sum2)
        lastSum = None
        for fundamental_key in Ratio.allRatios:
            sum = dict_of_matrix[fundamental_key].sum().sum()
            if lastSum is not None:
                self.assertNotEquals(sum, lastSum)
            lastSum = sum

    def test_checkPricing(self):
        import time
        instrumentList = [
            Instrument(symbol='A', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='AAAL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='ABT', asset_type=AssetType.us_equity, currency=Currency.usd),

        ]
        for instrument in instrumentList:
            self.vector_service.vectorDao.delete(instrument)

            self.vector_service.vectorizedDataService.historicalMarketDataService.deleteBar(instrument=instrument,
                                                                                            period=Period.day,
                                                                                            number_of_periods=1)

        start = time.time()
        dictPricing = {}
        wrongInstrumentsSymbolLists = []
        for instrument in instrumentList:
            self.vector_service.vectorizedDataService.__downloadHistoricalPrices__(
                wrongInstrumentsSymbolLists=wrongInstrumentsSymbolLists,
                outputDict=dictPricing,
                fromDate=datetime.datetime(year=2010,
                                           day=1,
                                           month=1),
                toDate=self.toDate,
                instrument=instrument
            )

        dict_of_matrix = self.vector_service.getDataDictOfMatrix(instrumentList=instrumentList,
                                                                 ratioList=[],
                                                                 fromDate=datetime.datetime(year=2010, day=1, month=1),
                                                                 toDate=self.toDate,
                                                                 )
        end = time.time()
        elapsed = end - start
        print('1-Took %d seconds to complete 4 instrument downloading' % (elapsed))
        self.assertIsNotNone(dict_of_matrix)
        self.assertEqual(5, len(dict_of_matrix))
        keysOutput = dict_of_matrix.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)

        # %%
        for key in dictPricing.keys():
            if key == 'wrong':
                continue
            for symbol in dict_of_matrix[key].columns:
                print('testing pricing equality in %s:%s' % (key, symbol))
                isBusinessDay = BDay().onOffset
                match_series = pd.to_datetime(dictPricing[key].index).map(isBusinessDay)
                dictPricing_df = dictPricing[key][match_series].copy()
                dictPricing_df.fillna(method='ffill', inplace=True)

                df = dict_of_matrix[key][symbol]
                df_pricing = dictPricing_df[symbol]
                # self.assertEqual(len(df), len(df_pricing.dropna()))
                sum1 = df.sum()
                sum2 = df_pricing.sum()
                # self.assertEqual(sum1, sum2)

    def test_getDict(self):
        dict_of_matrix = self.vector_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
                                                                 ratioList=self.ratio_list,
                                                                 fromDate=self.fromDate,
                                                                 toDate=self.toDate,
                                                                 )
        self.assertIsNotNone(dict_of_matrix)
        self.assertEqual(8, len(dict_of_matrix))
        keysOutput = dict_of_matrix.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)
        for key in self.ratio_list:
            self.assertEqual(key in keysOutput, True)

    @unittest.skip(reason='slow')
    def test_getAllDict(self):
        ratio_list = Ratio.fundamental_list_Y
        ratio_list += (Ratio.quant_list)

        dict_of_matrix = self.vector_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
                                                                 ratioList=ratio_list,
                                                                 fromDate=self.fromDate,
                                                                 toDate=self.toDate
                                                                 )
        self.assertIsNotNone(dict_of_matrix)

        keysOutput = dict_of_matrix.keys()
        for key in DataDictKeys.keys:
            self.assertEqual(key in keysOutput, True)
        for key in ratio_list:
            self.assertEqual(key in keysOutput, True)

    @unittest.skip(reason='slow , and not yet')
    def test_getAllDict_timeIt(self):
        import time
        ratio_list = [Ratio.fundamental_shares_Y, Ratio.fundamental_accrual_Y]

        startTemp = time.time()
        dict_of_matrix_normal = self.vector_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
                                                                        ratioList=ratio_list,
                                                                        fromDate=self.fromDate,
                                                                        toDate=self.toDate
                                                                        )

        endTemp = time.time()
        print('TEMP::: Dict matrix of %d ratios of  %d symbols  took %d seconds ' % (
            len(ratio_list), len(self.instrumentList), endTemp - startTemp))

        self.assertIsNotNone(dict_of_matrix_normal)
        start = time.time()
        dict_of_matrix_notTemp = self.vector_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
                                                                         ratioList=ratio_list,
                                                                         fromDate=self.fromDate,
                                                                         toDate=self.toDate,
                                                                         useFunctionTemp=False
                                                                         )
        end = time.time()
        timeInSeconds = end - start
        print('Dict matrix of %d ratios of  %d symbols  took %d seconds ' % (
            len(ratio_list), len(self.instrumentList), timeInSeconds))

        self.assertIsNotNone(dict_of_matrix_notTemp)

        self.assertEqual(dict_of_matrix_normal.keys(), dict_of_matrix_notTemp.keys())

        self.assertEqual(dict_of_matrix_normal[DataDictKeys.close].shape,
                         dict_of_matrix_notTemp[DataDictKeys.close].shape)

        self.assertEqual(dict_of_matrix_normal[Ratio.fundamental_shares_Y].shape,
                         dict_of_matrix_notTemp[Ratio.fundamental_shares_Y].shape)

        ## test time <10 seconds
        # self.assertLess(timeInSeconds,10)
