import quandl

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.historical_market_data.historical_market_data import HistoricalMarketData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.period import Period


class QuandlHistoricalMarketData(HistoricalMarketData):
    period_dict = \
        {
            Period.day: 'day',
            Period.minute: 'minute',
            Period.hour: 'hour'
        }

    columns_historical_dict = \
        {
            Bar.close: 'Last',
            Bar.open: 'Open',
            Bar.high: 'High',
            Bar.low: 'Low',
            Bar.time: 'Date',
            Bar.volume: 'Volume'
        }
    user_settings = None

    def __init__(self, user_settings):
        HistoricalMarketData.__init__(self, user_settings)
        self.user_settings = user_settings
        quandl.ApiConfig.api_key = self.user_settings.quandl_token
        pass

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
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
        logger.debug("Downloading %s" % instrument)

        if period != Period.day:
            logger.error("Quandl can only download daily! not %s" % period)
            return None
        dateFormat = "%Y-%m-%d"
        if toDate is None:
            toDate = datetime.datetime.today()

        # download dataframe
        prefix = 'CHRIS'

        if instrument.asset_type == AssetType.commodity_future:
            prefix = 'CHRIS'
        if instrument.asset_type == AssetType.future:
            prefix = 'CHRIS'

        quandlDatabase = '%s/%s' % (prefix, instrument.symbol)
        try:
            data_downloaded = quandl.get(quandlDatabase, start_date=fromDate.strftime(dateFormat),
                                         end_date=toDate.strftime(
                                             dateFormat))  # , authtoken=self.user_settings.quandl_token)
            # data_downloaded = pdr.get_data_yahoo(instrument.symbol, start=fromDate.strftime(dateFormat), end=toDate.strftime(dateFormat))
        except Exception as e:
            logger.error("Cant download from quandl %s => return None   %s" % (instrument.symbol, e))
            return None

        outputComplete = self.formatHistorical(data_downloaded)
        outputComplete = self.setTimeCorrect(outputComplete, period=period, instrument=instrument)

        return outputComplete
