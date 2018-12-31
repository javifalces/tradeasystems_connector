from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.historical_market_data.historical_market_data import getCloseTimeHourMinInUTC


class QuantData:
    def __init__(self, user_settings):
        pass

    def download(self, instrument, quant_ratio, fromDate, toDate=None):
        pass

    def setTimeCorrect(self, outputComplete, instrument):
        import pandas as pd
        # must be in UTC directly
        try:
            if outputComplete is not None and outputComplete.index[0].hour == 0:
                hour, minute = getCloseTimeHourMinInUTC(instrument)
                outputComplete.index = outputComplete.index + pd.DateOffset(hours=hour, minute=minute)
                # outputComplete.index=outputComplete.index.tz_convert( timezone_setting)
        except Exception as e:
            logger.error('Error setting time of fundamental data of %s:%s' % (instrument.symbol, str(e)))
            outputComplete = None
        return outputComplete
