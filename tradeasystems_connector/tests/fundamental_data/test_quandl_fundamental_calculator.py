import unittest

from tradeasystems_connector.fundamental_data.quandl_fundamental_calculator import *
from tradeasystems_connector.fundamental_data.quandl_fundamental_data import QuandlFundamentalData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.tests import user_settings_tests


class QuandlFundamentalCalculatorTestCase(unittest.TestCase):
    instrument = Instrument('AAPL', asset_type=AssetType.us_equity, currency=Currency.usd)
    user_settings = user_settings_tests
    fundamental_data_object = QuandlFundamentalData(user_settings)

    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)
    fundamental_period = FundamentalPeriod.yearly

    def test_getAllBatch(self):
        import time
        start = time.time()
        fromDate = datetime.datetime(year=2010, day=1, month=1)

        df_batch = self.fundamental_data_object.downloadBatch(instrument=self.instrument,
                                                              fundamental_ratio_list=Ratio.fundamental_list_Y,
                                                              fromDate=fromDate, toDate=self.toDate)
        end = time.time()
        secondsElapsed = end - start
        print('Get all fundamentals of %s took %d seconds' % (self.instrument, secondsElapsed))
        self.assertIsNotNone(df_batch)
        for ratio in Ratio.fundamental_list_Y:
            if ratio not in list(df_batch.columns):
                print('Ratio %s is not in output!' % ratio)
            self.assertTrue(ratio in list(df_batch.columns))

        self.assertEqual(len(Ratio.fundamental_list_Y) + 1, len(df_batch.columns))  # +1 because of close_temp
        self.assertLess(secondsElapsed, 30)  # less 30 seconds per instrument

    def test_getROABatch(self):
        import time
        start = time.time()
        fromDate = datetime.datetime(year=2010, day=1, month=1)

        df_batch = self.fundamental_data_object.downloadBatch(instrument=self.instrument, fundamental_ratio_list=[
            Ratio.fundamental_return_over_assets_Y], fromDate=fromDate, toDate=self.toDate)
        end = time.time()
        secondsElapsed = end - start
        print('test_getROABatch of %s took %d seconds' % (self.instrument, secondsElapsed))
        self.assertIsNotNone(df_batch)
        self.assertEqual(1, len(df_batch.columns))

    def test_getBVBatch(self):
        import time
        start = time.time()
        fromDate = datetime.datetime(year=2010, day=1, month=1)

        df_batch = self.fundamental_data_object.downloadBatch(instrument=self.instrument, fundamental_ratio_list=[
            Ratio.fundamental_book_value_per_share_Y], fromDate=fromDate, toDate=self.toDate)
        end = time.time()
        secondsElapsed = end - start
        print('test_getBVBatch of %s took %d seconds' % (self.instrument, secondsElapsed))
        self.assertIsNotNone(df_batch)
        self.assertEqual(1, len(df_batch.columns))

    def test_getBV_BVLiabBatch(self):
        import time
        start = time.time()
        fromDate = datetime.datetime(year=2010, day=1, month=1)

        df_batch = self.fundamental_data_object.downloadBatch(instrument=self.instrument, fundamental_ratio_list=[
            Ratio.fundamental_book_value_liabilities_Y, Ratio.fundamental_book_value_per_share_Y], fromDate=fromDate,
                                                              toDate=self.toDate)
        end = time.time()
        secondsElapsed = end - start
        print('test_getBV_BVLiabBatch of  %s took %d seconds' % (self.instrument, secondsElapsed))
        self.assertIsNotNone(df_batch)
        self.assertEqual(3, len(df_batch.columns))

    def test_getAllBatch_wrong(self):
        fromDate = datetime.datetime(year=2010, day=1, month=1)
        instrument = Instrument('A123APL', asset_type=AssetType.us_equity, currency=Currency.usd)
        df_batch = self.fundamental_data_object.downloadBatch(instrument=instrument,
                                                              fundamental_ratio_list=Ratio.fundamental_list_Y,
                                                              fromDate=fromDate, toDate=self.toDate)
        self.assertIsNone(df_batch)

    def test_getCapitalization(self):
        capitalization = getCapitalization(self.fundamental_data_object, instrument=self.instrument,
                                           fundamental_period=self.fundamental_period, fromDate=self.fromDate,
                                           toDate=self.toDate)

        self.assertIsNotNone(capitalization)

    def test_getNetAssetValue(self):
        nav = getNetAssetValue(self.fundamental_data_object, instrument=self.instrument,
                               fundamental_period=self.fundamental_period)

        self.assertIsNotNone(nav)

    def test_getAdjustedBookValue(self):
        adj_book_value = getAdjustedBookValue(self.fundamental_data_object, instrument=self.instrument,
                                              fundamental_period=self.fundamental_period)

        self.assertIsNotNone(adj_book_value)

    def test_getDaySalesReceivables(self):
        day_sales_receivables = getDaySalesReceivables(self.fundamental_data_object, instrument=self.instrument,
                                                       fundamental_period=self.fundamental_period)

        self.assertIsNotNone(day_sales_receivables)

    def test_getEnterpriseValue(self):
        ev = getEnterpriseValue(self.fundamental_data_object, instrument=self.instrument,
                                fundamental_period=self.fundamental_period)

        self.assertIsNotNone(ev)

    def test_getReturnOverAssets(self):
        roa = getReturnOverAssets(self.fundamental_data_object, instrument=self.instrument,
                                  fundamental_period=self.fundamental_period, fromDate=self.fromDate,
                                  toDate=self.toDate)

        self.assertIsNotNone(roa)

    def test_getOperatingAssets(self):
        op_assets = getOperatingAssets(self.fundamental_data_object, instrument=self.instrument,
                                       fundamental_period=self.fundamental_period)

        self.assertIsNotNone(op_assets)

    def test_getReturnOverCapital(self):
        roc = getReturnOverCapital(self.fundamental_data_object, instrument=self.instrument,
                                   fundamental_period=self.fundamental_period, fromDate=self.fromDate,
                                   toDate=self.toDate)

        self.assertIsNotNone(roc)

    def test_getSalesGrowth(self):
        sales_growth = getSalesGrowth(self.fundamental_data_object, instrument=self.instrument,
                                      fundamental_period=self.fundamental_period)

        self.assertIsNotNone(sales_growth)

    def test_getBookValueLiabilities(self):
        bv_liabilities = getBookValueLiabilities(self.fundamental_data_object, instrument=self.instrument,
                                                 fundamental_period=self.fundamental_period)

        self.assertIsNotNone(bv_liabilities)

    def test_getLongTermDebt(self):
        lt_debt = getLongTermDebt(self.fundamental_data_object, instrument=self.instrument,
                                  fundamental_period=self.fundamental_period)

        self.assertIsNotNone(lt_debt)

    def test_getShortTermDebt(self):
        st_debt = getShortTermDebt(self.fundamental_data_object, instrument=self.instrument,
                                   fundamental_period=self.fundamental_period)

        self.assertIsNotNone(st_debt)

    def test_getWrongInstrument(self):
        instrument_wrong = Instrument('A123APL', asset_type=AssetType.us_equity, currency=Currency.usd)
        st_debt = getShortTermDebt(self.fundamental_data_object, instrument=instrument_wrong,
                                   fundamental_period=self.fundamental_period)

        self.assertIsNone(st_debt)

    def test_getAccrual(self):
        data = getAccrual(self.fundamental_data_object, instrument=self.instrument,
                          fundamental_period=self.fundamental_period, fromDate=self.fromDate, toDate=self.toDate)

        self.assertIsNotNone(data)

    def test_getDaySalesReceivablesIndex(self):
        data = getDaySalesReceivablesIndex(self.fundamental_data_object, instrument=self.instrument,
                                           fundamental_period=self.fundamental_period, fromDate=self.fromDate,
                                           toDate=self.toDate)

        self.assertIsNotNone(data)
