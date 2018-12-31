# import unittest
#
# import pandas as pd
#
#
#
# from tradeasystems_connector.model.asset_type import AssetType
# from tradeasystems_connector.model.currency import Currency
# from tradeasystems_connector.model.instrument import Instrument
# from tradeasystems_connector.model.period import Period
# from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService
# from tradeasystems_connector.service.zipline_bundle_service import ZiplineBundleService
# from tradeasystems_connector.tests import user_settings_tests
#
# symbolName = 'AAPL'
#
#
# def initialize(context):
#     context.asset = symbol(symbolName)
#
#     # Explicitly set the commission/slippage to the "old" value until we can
#     # rebuild example data.
#     # github.com/quantopian/zipline/blob/master/tests/resources/
#     # rebuild_example_data#L105
#     context.set_commission(commission.PerShare(cost=.0075, min_trade_cost=1.0))
#     context.set_slippage(slippage.VolumeShareSlippage())
#
#
# def handle_data(context, data):
#     order(context.asset, 10)
#     record(symbolName=data.current(context.asset, 'price'))
#
#
# # Note: this function can be removed if running
# # this algorithm on quantopian.com
# def analyze(context=None, results=None):
#     import matplotlib.pyplot as plt
#     # Plot the portfolio and asset data.
#     ax1 = plt.subplot(211)
#     results.portfolio_value.plot(ax=ax1)
#     ax1.set_ylabel('Portfolio value (USD)')
#     ax2 = plt.subplot(212, sharex=ax1)
#     results[symbolName].plot(ax=ax2)
#     ax2.set_ylabel('%s price (USD)' % symbolName)
#
#     # Show the plot.
#     plt.gcf().set_size_inches(18, 8)
#     plt.show()
#
#
# class ZiplineBundleServiceTestCase(unittest.TestCase):
#     user_settings = user_settings_tests
#
#     symbol_list = [symbolName]
#
#     instrument = Instrument(symbol=symbolName, asset_type=AssetType.es_equity,
#                             currency=Currency.eur)
#     period = Period.day
#     number_of_periods = 1
#
#     fromDate = pd.Timestamp('2014-01-01', tz='utc')
#     toDate = pd.Timestamp('2014-11-01', tz='utc')
#
#     zipline_service = ZiplineBundleService(user_settings, symbol_list=symbol_list)
#     historical_service = HistoricalMarketDataService(user_settings)
#
#     @unittest.skip(reason='problem versions zipline')
#     def test_executeBackest(self):
#         from zipline import run_algorithm
#         from zipline.api import order, record, symbol
#         from zipline.finance import commission, slippage
#
#         import pandas as pd
#         # load data to Store
#         # #force download
#         self.historical_service.getHistoricalData(instrument=self.instrument, period=self.period,
#                                                   number_of_periods=self.number_of_periods,
#                                                   fromDate=self.fromDate,
#                                                   toDate=self.toDate,
#                                                   force_download=True
#                                                   )
#         start = pd.Timestamp('2014-01-01', tz='utc')
#         end = pd.Timestamp('2014-11-01', tz='utc')
#         returnBacktest = run_algorithm(start, end,
#                                        initialize,
#                                        capital_base=1000,
#                                        handle_data=handle_data,
#                                        bundle=self.zipline_service.getBundle()
#                                        )
#
#         self.assertIsNotNone(returnBacktest)
#         self.assertTrue(returnBacktest.columns.contains('short_value'))
