import datetime

import pandas as pd

from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.quant_data.quant_data import QuantData
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService


class QuantDataImpl(QuantData):
    user_Settings = None
    marketDataServices = None

    def __init__(self, user_Settings):
        self.user_Settings = user_Settings
        self.marketDataServices = HistoricalMarketDataService(self.user_Settings)

    ######################
    ###All data is downloaded with weekends included
    ######################
    def download(self, instrument, quant_ratio, fromDate, toDate=None):
        if quant_ratio == Ratio.quant_returnsDiff_120:
            output = self.getReturnDiff(instrument, 150, fromDate, toDate)
        if quant_ratio == Ratio.quant_return1YFrom20:
            output = self.getReturn1Y(instrument, 30, fromDate, toDate)
        if quant_ratio == Ratio.quant_return1Y:
            output = self.getReturn1Y(instrument, 0, fromDate, toDate)
        if quant_ratio == Ratio.quant_std1Y:
            output = self.getStd(instrument, 365, fromDate, toDate)

        return output

    def getReturnDiff(self, instrument, period, fromDate, toDate):
        if toDate is None:
            toDate = datetime.datetime.today()

        fromDateRequest = fromDate - datetime.timedelta(days=period * 2)
        barPrices = self.marketDataServices.getHistoricalData(instrument, period=Period.day, number_of_periods=1,
                                                              force_download=True,
                                                              fromDate=fromDateRequest,
                                                              toDate=toDate,
                                                              cleanOutliers=False
                                                              )
        if barPrices is None:
            return None

        outputIndex = pd.date_range(start=fromDate, end=toDate)
        output = pd.DataFrame(0, index=outputIndex, columns=[Ratio.ratio])

        returnDaily = (barPrices[Bar.close] / barPrices[Bar.close].shift(1)) - 1
        returnDailyDiff_ = returnDaily - returnDaily.shift(period)

        closesIndexBar = returnDailyDiff_.index.searchsorted(output.index) - 1
        output[Ratio.ratio] = returnDailyDiff_[closesIndexBar].values

        return output

    def getReturn1Y(self, instrument, period, fromDate, toDate):
        if toDate is None:
            toDate = datetime.datetime.today()

        fromDateRequest = fromDate - datetime.timedelta(days=2 * 365)
        barPrices = self.marketDataServices.getHistoricalData(instrument, period=Period.day, number_of_periods=1,
                                                              force_download=True,
                                                              fromDate=fromDateRequest,
                                                              toDate=toDate
                                                              )
        if barPrices is None:
            return None
        outputIndex = pd.date_range(start=fromDate, end=toDate)
        output = pd.DataFrame(0, index=outputIndex, columns=[Ratio.ratio])

        returnDaily = (barPrices[Bar.close].shift(period) / barPrices[Bar.close].shift(period + 365)) - 1

        closesIndexBar = returnDaily.index.searchsorted(output.index) - 1
        output[Ratio.ratio] = returnDaily[closesIndexBar].values

        return output

    def getStd(self, instrument, period, fromDate, toDate):
        if toDate is None:
            toDate = datetime.datetime.today()

        fromDateRequest = fromDate - datetime.timedelta(days=2 * 365)
        barPrices = self.marketDataServices.getHistoricalData(instrument,
                                                              period=Period.day,
                                                              number_of_periods=1,
                                                              force_download=True,
                                                              fromDate=fromDateRequest,
                                                              toDate=toDate
                                                              )
        if barPrices is None:
            return None
        outputIndex = pd.date_range(start=fromDate, end=toDate)

        output = pd.DataFrame(0, index=outputIndex, columns=[Ratio.ratio])

        volatilityDaily = barPrices[Bar.close].rolling(period).std()

        closesIndexBar = volatilityDaily.index.searchsorted(output.index) - 1
        output[Ratio.ratio] = volatilityDaily[closesIndexBar].values

        return output
