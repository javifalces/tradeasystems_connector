from tempfile import mkdtemp

import joblib
import pdblp

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.historical_market_data.historical_market_data import HistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.period import Period


class BloombergHistoricalMarketData(HistoricalMarketData):
    formatDate = '%Y%m%d'
    formatDateBBG = '%Y-%m-%d'  # 2015-01-09
    bbg_object = None
    period_dict = \
        {
            Period.day: ['PX_LAST', 'PX_HIGH', 'PX_LOW', 'PX_OPEN', 'PX_VOLUME'],
            Period.minute: 'minute',
            Period.hour: 'hour'
        }

    columns_historical_dict = \
        {
            Bar.close: 'PX_LAST',
            Bar.open: 'PX_OPEN',
            Bar.high: 'PX_HIGH',
            Bar.low: 'PX_LOW',
            Bar.time: 'date',
            Bar.volume: 'PX_VOLUME'
        }
    user_settings = None
    temp_dir = mkdtemp()
    cacher = joblib.Memory(temp_dir)
    bdh = None
    suffix_assetType = {
        AssetType.equity: ' SM Equity',
        AssetType.forex: ' SM Equity'
    }

    def __init__(self, user_settings):
        HistoricalMarketData.__init__(self, user_settings)
        self.user_settings = user_settings
        self.bbg_object = pdblp.BCon(debug=True, port=self.user_settings.bloomberg_port)
        self.bbg_object.start()
        self.bdh = self.cacher.cache(self.bbg_object.bdh, ignore=['self'])
        pass

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        timeColumn = timeColumn + pd.DateOffset(hours=17, minutes=30)  # Add close time of market
        originTZ = 'Etc/GMT'

        datetimeSeries = pd.to_datetime(timeColumn)
        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def formatHistorical(self, input_df):
        import pandas as pd

        output = pd.DataFrame(0, columns=Bar.columns, index=range(len(input_df)))
        input_df.reset_index(inplace=True)
        for column in output.columns:
            if column == Bar.time:
                timeProcessed = self.formatTime(input_df[self.columns_historical_dict[column]])
                output[column] = timeProcessed
            else:
                output[column] = input_df[self.columns_historical_dict[column]]

        output.set_index(Bar.index, inplace=True)

        return output

    # day unlimited
    # minute limited to 7 days!
    def download(self, instrument, period, number_of_periods, fromDate, toDate=None):
        import datetime
        logger.debug("Downloading %s" % instrument.symbol)

        if period != Period.day:
            logger.error("Bloomberg can only download daily! not %s" % period)
            return None
        if toDate is None:
            toDate = datetime.datetime.today()

        try:
            data_downloaded = self.bdh(instrument.symbol + self.suffix_assetType[instrument.asset_type],
                                       self.period_dict[period],
                                       fromDate.strftime(self.formatDate),
                                       toDate.strftime(self.formatDate))

        except Exception as e:
            logger.error("Cant download from bloomberg %s => return None   %s" % (instrument.symbol, e))
            return None
        data_downloaded = data_downloaded[instrument.symbol]
        outputComplete = self.formatHistorical(data_downloaded)
        if fromDate is not None and toDate is not None:
            outputComplete = outputComplete[fromDate: toDate]
        elif fromDate is not None:
            outputComplete = outputComplete[fromDate:]
        elif toDate is not None:
            outputComplete = outputComplete[:toDate]
        return outputComplete
