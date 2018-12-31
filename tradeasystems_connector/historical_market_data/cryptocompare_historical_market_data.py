from cryptocompy import price

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.historical_market_data.historical_market_data import HistoricalMarketData
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.period import Period


class CryptoCompareHistoricalMarketData(HistoricalMarketData):
    sourceCrypto = 'all'  # 'Kraken'  # 'all'

    period_dict = \
        {
            Period.day: 'day',
            Period.minute: 'minute',
            Period.hour: 'hour'
        }

    columns_historical_dict = \
        {
            Bar.close: 'close',
            Bar.open: 'open',
            Bar.high: 'high',
            Bar.low: 'low',
            Bar.time: 'time',
            Bar.volume: 'volumeto'
        }

    def __init__(self, user_settings):
        HistoricalMarketData.__init__(self, user_settings);
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

    def formatHistorical(self, input):
        import pandas as pd
        input_df = pd.DataFrame.from_dict(input)

        output = pd.DataFrame(0, columns=Bar.columns, index=input_df.index)
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
        logger.debug("Downloading %s" % instrument)

        crypto_period = self.period_dict[period]
        dict_downloaded = price.get_historical_data(
            instrument.symbol, instrument.currency, crypto_period, aggregate=number_of_periods)
        if len(dict_downloaded) == 0:
            logger.error('Cant download cryptocompare %s_%s for %s return None' % (
                instrument.symbol, instrument.currency, period))
            return None
        outputComplete = self.formatHistorical(dict_downloaded)

        # Already added
        # outputComplete = self.setTimeCorrect(outputComplete, period=period, instrument=instrument)
        return outputComplete
