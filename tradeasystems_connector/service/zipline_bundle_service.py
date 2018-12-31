import numpy as np
import pandas as pd
import requests
from zipline.utils.cli import maybe_show_progress

# based on
# https://financialzipline.wordpress.com/2016/08/24/importing-south-african-equities-data-into-zipline/
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService


class ZiplineBundleService:
    user_settings = None
    historical_market_data_service = None
    bundle_name = 'tradea_bundle'

    symbol_list = {
        # 'JSE:ADR',
        'AAPL'
    }

    def __init__(self, user_settings, symbol_list=None):
        self.user_settings = user_settings
        self.historical_market_data_service = HistoricalMarketDataService(self.user_settings)
        if symbol_list is not None:
            self.symbol_list = symbol_list
        else:
            self.symbol_list = self.user_settings.zipline_symbol_list
        self.__register_bundle__()

    def __register_bundle__(self):
        from zipline.data.bundles import register
        from zipline.data.bundles import ingest
        register(
            self.bundle_name,  # name this whatever you like
            self.tradea_bundle(self.symbol_list),
        )

        ingest(self.bundle_name)

    def getBundle(self):
        return self.bundle_name

    def tradea_bundle(self, symbols, start=None, end=None):
        # http: // www.prokopyshen.com / create - custom - zipline - data - bundle

        """Create a data bundle ingest function from a set of symbols loaded from
        database.

        Parameters
        ----------
        symbols : iterable[str]
            The ticker symbols to load data for.
        start : datetime, optional
            The start date to query for. By default this pulls the full history
            for the calendar.
        end : datetime, optional
            The end date to query for. By default this pulls the full history
            for the calendar.

        Returns
        -------
        ingest : callable
            The bundle ingest function for the given set of symbols.

        Examples
        --------
        This code should be added to ~/.zipline/extension.py

        .. code-block:: python

           from zipline.data.bundles import yahoo_equities, register

           symbols = (
               'AAPL',
               'IBM',
               'MSFT',
           )
           register('my_bundle', yahoo_equities(symbols))

        Notes
        -----
        The sids for each symbol will be the index into the symbols sequence.
        """

        # strict this in memory so that we can reiterate over it
        symbols = tuple(symbols)

        def ingest(environ,
                   asset_db_writer,
                   minute_bar_writer,  # unused
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start,
                   end,
                   cache,
                   show_progress,
                   output_dir,
                   # pass these as defaults to make them 'nonlocal' in py2
                   ):
            if start is None:
                start = calendar[0]
            if end is None:
                end = None

            metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
                ('start_date', 'datetime64[ns]'),
                ('end_date', 'datetime64[ns]'),
                ('auto_close_date', 'datetime64[ns]'),
                ('symbol', 'object'),
            ]))

            def _pricing_iter():
                sid = 0
                with maybe_show_progress(
                        symbols,
                        show_progress,
                        label='Downloading Tradea Database pricing data ') as it, \
                        requests.Session() as session:
                    for symbol in it:
                        logger.debug('zipline bundle downloading %s' % symbol)
                        try:
                            instrument = Instrument(symbol=symbol, asset_type=AssetType.us_equity)

                            df = self.historical_market_data_service.getHistoricalData(instrument,
                                                                                       period=Period.day,
                                                                                       number_of_periods=1,
                                                                                       fromDate=start,
                                                                                       toDate=end,
                                                                                       bar_type=BarType.time_bar,
                                                                                       force_download=False,
                                                                                       cleanOutliers=False
                                                                                       )
                        except Exception as e:
                            logger.error('Error downloading bundle zipline %s : %s' % (symbol, str(e)))
                            print('Error downloading bundle zipline %s : %s' % (symbol, str(e)))
                            df = None
                            continue

                        # the start date is the date of the first trade and
                        # the end date is the date of the last trade
                        indexSet = df.index.copy()
                        indexSet = (indexSet + pd.DateOffset(hours=3)) - pd.DateOffset(days=1)
                        df.index = indexSet

                        start_date = df.index[0]
                        end_date = df.index[-1]
                        # The auto_close date is the day after the last trade.
                        ac_date = end_date + pd.Timedelta(days=1)
                        metadata.iloc[sid] = start_date, end_date, ac_date, symbol

                        df.rename(
                            columns={
                                Bar.open: 'open',
                                Bar.high: 'high',
                                Bar.low: 'low',
                                Bar.close: 'close',
                                Bar.volume: 'volume',
                            },
                            inplace=True,
                        )
                        yield sid, df
                        sid += 1

            daily_bar_writer.write(_pricing_iter(), show_progress=True)
            metadata['exchange'] = "YAHOO"
            symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)

            asset_db_writer.write(equities=metadata)

            adjustment_writer.write()

        return ingest
