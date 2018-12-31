import datetime
import unittest

from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.data_dict_keys import DataDictKeys
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.vector_online_service import VectorOnlineService
from tradeasystems_connector.tests import user_settings_tests


class VectorizedDataServiceTestCase(unittest.TestCase):
    user_settings = user_settings_tests
    vectorized_data_service = VectorOnlineService(user_settings)

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

    @unittest.skip('not used anymore')
    def test_getDict(self):
        dict_of_matrix = self.vectorized_data_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
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

        dict_of_matrix = self.vectorized_data_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
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
        dict_of_matrix_normal = self.vectorized_data_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
                                                                                 ratioList=ratio_list,
                                                                                 fromDate=self.fromDate,
                                                                                 toDate=self.toDate
                                                                                 )

        endTemp = time.time()
        print('TEMP::: Dict matrix of %d ratios of  %d symbols  took %d seconds ' % (
            len(ratio_list), len(self.instrumentList), endTemp - startTemp))

        self.assertIsNotNone(dict_of_matrix_normal)
        start = time.time()
        dict_of_matrix_notTemp = self.vectorized_data_service.getDataDictOfMatrix(instrumentList=self.instrumentList,
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

    def test___getInputKeys__(self):
        import time
        pk = self.vectorized_data_service.__getInputKeys__(instrumentList=self.instrumentList, fromDate=self.fromDate,
                                                           toDate=self.toDate)
        time.sleep(1)
        pk1 = self.vectorized_data_service.__getInputKeys__(instrumentList=self.instrumentList, fromDate=self.fromDate,
                                                            toDate=self.toDate)

        time.sleep(1)
        instrumentList2 = [
            Instrument(symbol='ABC', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='ACN', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='GOOGL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='MSFT', asset_type=AssetType.us_equity, currency=Currency.usd),

        ]

        pk2 = self.vectorized_data_service.__getInputKeys__(instrumentList=instrumentList2, fromDate=self.fromDate,
                                                            toDate=self.toDate)

        self.assertEqual(pk, pk2)

    # check if parallel downloading is working
    @unittest.skip('not used anymore')
    def test_getDataDictOfMatrixParallel(self):
        # ratio_list = Ratio.fundamental_list_Y
        # symbolList = getSharadarTickers(self.user_settings)[20:30]
        instrumentList2 = [
            Instrument(symbol='ABC', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='ACN', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='AAPL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='GOOGL', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='MSFT', asset_type=AssetType.us_equity, currency=Currency.usd),

            Instrument(symbol='GNW', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='SCG', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='KANG', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='S', asset_type=AssetType.us_equity, currency=Currency.usd),
            Instrument(symbol='A', asset_type=AssetType.us_equity, currency=Currency.usd),

        ]
        ratio_list = [Ratio.fundamental_ebitda_Y,
                      Ratio.fundamental_enterprise_value_Y,
                      Ratio.fundamental_ms_Y,
                      # Ratio.quant_return1Y,
                      # Ratio.quant_return1YFrom20,
                      # Ratio.quant_returnsDiff_120,
                      # Ratio.quant_std1Y,
                      # Ratio.fundamental_asset_quality_index_Y,
                      # Ratio.fundamental_book_value_liabilities_Y,
                      # Ratio.fundamental_shares_Y
                      ]

        instrumentList = instrumentList2  # getInstrumentList(symbolList, asset_type=AssetType.us_equity, currency=Currency.usd)

        fromDate = datetime.datetime(year=2015, day=1, month=1)
        self.vectorized_data_service.parallelDownloadInstruments = False
        self.vectorized_data_service.parallelDownloadRatios = False
        self.vectorized_data_service.force_download = True
        self.vectorized_data_service.threads = 8
        dict_of_matrix_serial = self.vectorized_data_service.getDataDictOfMatrix(instrumentList=instrumentList,
                                                                                 ratioList=ratio_list,
                                                                                 fromDate=fromDate,
                                                                                 toDate=self.toDate,
                                                                                 )

        self.assertIsNotNone(dict_of_matrix_serial)
        closeDF = dict_of_matrix_serial[DataDictKeys.close]
        self.assertIsNotNone(closeDF)
        prices = closeDF

        columnsRemove = list(prices.sum(axis=0)[prices.sum(axis=0) == 0].index)
        if len(columnsRemove) > 0:
            prices = prices.drop(columnsRemove, axis=1)
        self.assertIsNotNone(prices)
        self.assertTrue(len(prices) > 0)

        self.vectorized_data_service.parallelDownloadInstruments = True
        self.vectorized_data_service.parallelDownloadRatios = True
        dict_of_matrix_parallel = self.vectorized_data_service.getDataDictOfMatrix(instrumentList=instrumentList,
                                                                                   ratioList=ratio_list,
                                                                                   fromDate=fromDate,
                                                                                   toDate=self.toDate
                                                                                   )
        self.assertIsNotNone(dict_of_matrix_parallel)

        self.assertEquals(len(dict_of_matrix_serial), len(dict_of_matrix_parallel))
        for key in DataDictKeys.keys:
            print('Checking %s' % key)
            self.assertEquals(len(dict_of_matrix_serial[key]), len(dict_of_matrix_parallel[key]))
            self.assertEquals(dict_of_matrix_serial[key].sum().sum(), dict_of_matrix_parallel[key].sum().sum())

        for key in ratio_list:
            print('Checking %s' % key)
            self.assertEquals(len(dict_of_matrix_serial[key]), len(dict_of_matrix_parallel[key]))
            self.assertEquals(dict_of_matrix_serial[key].sum().sum(), dict_of_matrix_parallel[key].sum().sum())
