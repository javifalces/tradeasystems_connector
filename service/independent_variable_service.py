import pandas as pd

from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.bar_type import BarType
from tradeasystems_connector.model.broker import Broker
from tradeasystems_connector.model.independent_variable import IndependentVariable
from tradeasystems_connector.model.instrument import *
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService
from tradeasystems_connector.service.ratio_data_service import RatioDataService


class IndependentVariableService:
    user_setttings = None
    ratio_service = None
    historical_market_data_service = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.ratio_service = RatioDataService(self.user_settings)
        self.historical_market_data_service = HistoricalMarketDataService(self.user_settings)

    def getInstrument(self, instrument, fromDate, toDate, force_download):
        dataframePrices = self.historical_market_data_service.getHistoricalData(instrument, number_of_periods=1,
                                                                                fromDate=fromDate, toDate=toDate,
                                                                                bar_type=BarType.time_bar,
                                                                                force_download=force_download,
                                                                                endOfDayData=True,
                                                                                cleanOutliers=False,
                                                                                period=Period.day
                                                                                )

        output = pd.DataFrame(dataframePrices[Bar.close].values, columns=[Ratio.ratio], index=dataframePrices.index)
        return output

    def getEurUsd(self, fromDate, toDate, force_download):
        instrument = Instrument(symbol='EUR', currency='USD', asset_type=AssetType.forex, broker=Broker.oanda)
        dataframePrices = self.historical_market_data_service.getHistoricalData(instrument, number_of_periods=1,
                                                                                fromDate=fromDate, toDate=toDate,
                                                                                bar_type=BarType.time_bar,
                                                                                force_download=force_download,
                                                                                endOfDayData=True,
                                                                                cleanOutliers=False,
                                                                                period=Period.day
                                                                                )

        output = pd.DataFrame(dataframePrices[Bar.close].values, columns=[Ratio.ratio], index=dataframePrices.index)
        return output

    def getIndex(self, symbol, fromDate, toDate, force_download):
        instrument = Instrument(symbol=symbol, currency='USD', asset_type=AssetType.index)
        dataframePrices = self.historical_market_data_service.getHistoricalData(instrument, number_of_periods=1,
                                                                                fromDate=fromDate, toDate=toDate,
                                                                                bar_type=BarType.time_bar,
                                                                                force_download=force_download,
                                                                                endOfDayData=True,
                                                                                cleanOutliers=False,
                                                                                period=Period.day
                                                                                )
        if dataframePrices is None:
            return None
        output = pd.DataFrame(dataframePrices[Bar.close].values, columns=[Ratio.ratio], index=dataframePrices.index)
        return output

    def getUS(self, symbol, fromDate, toDate, force_download):
        instrument = Instrument(symbol=symbol, currency='USD', asset_type=AssetType.us_equity)
        dataframePrices = self.historical_market_data_service.getHistoricalData(instrument, number_of_periods=1,
                                                                                fromDate=fromDate, toDate=toDate,
                                                                                bar_type=BarType.time_bar,
                                                                                force_download=force_download,
                                                                                endOfDayData=True,
                                                                                cleanOutliers=False,
                                                                                period=Period.day
                                                                                )
        if dataframePrices is None:
            return None
        output = pd.DataFrame(dataframePrices[Bar.close].values, columns=[Ratio.ratio], index=dataframePrices.index)
        return output[fromDate:toDate]

    def getVariableData(self, independentVariable, fromDate, toDate=None, force_download=True):

        if independentVariable == IndependentVariable.prices_eur_usd:
            instrument = eur_usd
        if independentVariable == IndependentVariable.prices_vix:
            instrument = vix
        if independentVariable == IndependentVariable.prices_sp500:
            instrument = sp500
        if independentVariable == IndependentVariable.macro_interest_rates_us:
            instrument = t_bond
        if independentVariable == IndependentVariable.prices_dax:
            instrument = dax
        if independentVariable == IndependentVariable.prices_eonia:
            return self.getUS('C100.DE', fromDate, toDate, force_download)
        if independentVariable == IndependentVariable.prices_ibex:
            instrument = ibex

        # if independentVariable == IndependentVariable.macro_interest_rates_eur:
        #     return self.getIndex('TNX',fromDate,toDate,force_download)

        return self.getInstrument(instrument, fromDate, toDate, force_download)
