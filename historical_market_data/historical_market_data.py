from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.period import Period


def getCloseTimeHourMinInUTC(instrument):
    # https: // en.wikipedia.org / wiki / List_of_stock_exchange_trading_hours
    hour = 21
    minute = 00
    if instrument.asset_type == AssetType.us_equity:
        hour = 21
        minute = 00
    if instrument.asset_type == AssetType.es_equity:
        hour = 16
        minute = 30
    if instrument.asset_type == AssetType.forex:
        # depends on data source oanda daily alignment is at 17 NY =21
        hour = 21
        minute = 00
    if instrument.asset_type == AssetType.etf:
        # depends on data source from yahoo is at 4 ETC = 20
        hour = 20
        minute = 00

    return hour, minute


class HistoricalMarketData:

    def __init__(self, user_settings):
        pass

    def setTimeCorrect(self, outputComplete, period, instrument):
        import pandas as pd
        # must be in UTC directly

        if period is Period.day:
            hour, minute = getCloseTimeHourMinInUTC(instrument)
            outputComplete.index = outputComplete.index + pd.DateOffset(hours=hour, minutes=minute)

        return outputComplete

    def download(self, instrument, period, number_of_periods, fromDate, toDate=None):
        pass
